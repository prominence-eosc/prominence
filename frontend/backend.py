import base64
import json
import os
import re
import shlex
import subprocess
import uuid

from shutil import copyfile
from threading import Timer

import classad
import htcondor

from CreateSwiftURL import create_swift_url

# Job template
JOB_SUBMIT = \
"""
universe = vanilla
executable = promlet.py
arguments = tasks %(processes)s $(ProcId)

output = job.%(name)s.$(ProcId).out
error = job.%(name)s.$(ProcId).err
log = job.%(name)s.$(ProcId).log

should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = promlet.log
transfer_output_remaps = "promlet.log=promlet.$(ProcId).log"
requirements = false
transfer_executable = true
stream_output = true
stream_error = true

RequestCpus = %(cpus)s
RequestMemory = %(reqmemory)s

+remote_cerequirements = RequiredTasks == 1 && RequiredMemoryPerCpu == 1 && RequiredCpusPerTask == 1 && RequiredTime == 10

+ProminenceWantJobRouter = ProminenceMaxIdleTime =?= 0 || (ProminenceMaxIdleTime > 0 && JobStatus == 1 && CurrentTime - EnteredCurrentStatus > ProminenceMaxIdleTime)
+ProminenceJobUniqueIdentifier = "%(uuid)s"
+ProminenceIdentity = "%(username)s"
+ProminenceName = "%(name)s"
+ProminenceMemoryPerNode = %(memory)s
+ProminenceCpusPerNode = %(cpus)s
+ProminenceNumNodes = %(nodes)s
+ProminenceSharedDiskSize = %(disk)s
+ProminenceMaxIdleTime = %(maxidle)s
+ProminenceWantMPI = %(wantmpi)s
+ProminenceStorageType = "%(storagetype)s"
+ProminenceStorageMountPoint = "%(storagemountpoint)s"
+ProminenceStorageCredentials = "%(storagecreds)s"

+WantIOProxy = true

%(extras)s

queue %(instances)s
"""

def condor_str(str_in):
    """
    Returns a double-quoted string
    """
    return str('"%s"' % str_in)

def redact_storage_creds(storage):
    """
    Redact storage credentials
    """
    if 'b2drop' in storage:
        if 'app-username' in storage['b2drop']:
            storage['b2drop']['app-username'] = '***'
        if 'app-password' in storage['b2drop']:
            storage['b2drop']['app-password'] = '***'
    elif 'onedata' in storage:
        if 'provider' in storage['onedata']:
            storage['onedata']['provider'] = '***'
        if 'token' in storage['onedata']:
            storage['onedata']['token'] = '***'
    return storage

def get_job_unique_id(job_id):
    """
    Return the uid and identity for a specified job id
    """
    uid = None
    identity = None
    schedd = htcondor.Schedd()
    jobs_condor = schedd.history('RoutedBy =?= undefined && ClusterId =?= %s' % job_id,
                                 ['ProminenceJobUniqueIdentifier', 'ProminenceIdentity'], 1)
    for job in jobs_condor:
        if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
            uid = job['ProminenceJobUniqueIdentifier']
            identity = job['ProminenceIdentity']

    if uid is None or identity is None:
        jobs_condor = schedd.xquery('RoutedBy =?= undefined && ClusterId =?= %s' % job_id,
                                    ['ProminenceJobUniqueIdentifier', 'ProminenceIdentity'], 1)
        for job in jobs_condor:
            if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
                uid = job['ProminenceJobUniqueIdentifier']
                identity = job['ProminenceIdentity']
    return (uid, identity)

def create_job(username, group, uid, job_sandbox, jjob):
    """
    Create a job
    """
    cjob = {}

    # Copy executable to sandbox, change current working directory to the sandbox
    copyfile('/etc/prominence/promlet.py', os.path.join(job_sandbox, 'promlet.py'))

    os.chdir(job_sandbox)
    os.chmod(os.path.join(job_sandbox, 'promlet.py'), 0775)

    # Write input files to sandbox
    input_files = []
    if 'inputs' in jjob:
        filenames = []
        for file_input in jjob['inputs']:
            filename_new = os.path.join(job_sandbox + '/input', os.path.basename(file_input['filename']))
            with open(filename_new, 'w') as file:
                file.write(base64.b64decode(file_input['content']))
                filenames.append(file_input['filename'])
                input_files.append(filename_new)
        cjob['+ProminenceUserInputFiles'] = condor_str(','.join(filenames))

    # Default number of nodes
    if 'nodes' not in jjob['resources']:
        jjob['resources']['nodes'] = 1

    # Write tasks definition to file
    if 'tasks' in jjob:
        filename = os.path.join(job_sandbox, '.job.json')

        # Replace image identifiers with Swift temporary URLs
        tasks_new = []
        for task in jjob['tasks']:
            if 'http' not in task['image'] and ('.tar' in task['image'] or '.simg' in task['image']):
                image = task['image']
                task['image'] = create_swift_url('GET', '/v1/prominence-jobs/%s/%s' % (username, image), 6000)
                if '.tar' in task['image']:
                    task['runtime'] = 'udocker'
                elif '.simg' in task['image']:
                    task['runtime'] = 'singularity'
            tasks_new.append(task)

        jjob_new = jjob
        jjob_new['tasks'] = tasks_new
        with open(filename, 'w') as file:
            json.dump(jjob_new, file)
        input_files.append(filename)
        mpi_processes = int(jjob['resources']['cpus'])*int(jjob['resources']['nodes'])
        cjob['arguments'] = str('tasks %d 0' % mpi_processes)
    elif 'workflow' in jjob:
        if jjob['workflow']['type'] == 'cwl':
            cjob['arguments'] = str('cwl %s %s' % (jjob['workflow']['cwl'], jjob['workflow']['inputs']))
    else:
        return (1, {"error":"No tasks or workflow specified"})

    if 'name' in jjob:
        cjob['+ProminenceName'] = condor_str(jjob['name'])
        match_obj_name = re.match(r'([\w\-\_]+)', jjob['name'])
        if jjob['name'] != "" and (not match_obj_name or len(jjob['name']) > 64):
            return (1, {"error":"Invalid job name specified"})
    else:
        cjob['+ProminenceName'] = condor_str('')

    cjob['+ProminenceJobUniqueIdentifier'] = condor_str(uid)
    cjob['executable'] = 'promlet.py'
    cjob['transfer_executable'] = 'true'
    cjob['+ProminenceIdentity'] = condor_str(username)
    cjob['+ProminenceType'] = condor_str('job')

    cjob['+ProminenceMemoryPerNode'] = str(jjob['resources']['memory'])
    cjob['RequestMemory'] = str(1000*int(jjob['resources']['memory']))

    cjob['+ProminenceCpusPerNode'] = str(jjob['resources']['cpus'])
    cjob['RequestCpus'] = str(jjob['resources']['cpus'])

    cjob['+ProminenceNumNodes'] = str(jjob['resources']['nodes'])

    cjob['+ProminenceSharedDiskSize'] = str(jjob['resources']['disk'])

    # Set default disk size if one is not specified
    if int(jjob['resources']['disk']) < 1:
        cjob['+ProminenceSharedDiskSize'] = 10

    mpi_processes = int(jjob['resources']['cpus'])*int(jjob['resources']['nodes'])
    cjob['arguments'] = str('%s %d' % (cjob['arguments'], mpi_processes))

    cjob['universe'] = 'vanilla'
    cjob['Log'] = job_sandbox + '/job.$(Cluster).$(Process).log'
    cjob['Output'] = job_sandbox + '/job.$(Cluster).$(Process).out'
    cjob['Error'] = job_sandbox +  '/job.$(Cluster).$(Process).err'
    cjob['should_transfer_files'] = 'YES'
    cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
    cjob['transfer_output_files'] = 'promlet.log'
    cjob['+WantIOProxy'] = 'true'

    if group is not None:
        cjob['+ProminenceGroup'] = condor_str(group)

    # Stream stdout/err
    cjob['stream_error'] = 'true'
    cjob['stream_output'] = 'true'

    # Preemptible
    if 'preemptible' in jjob:
        cjob['+ProminencePreemptible'] = 'true'

    if 'storage' in jjob:
        if 'type' in jjob['storage']:
            cjob['+ProminenceStorageType'] = condor_str(jjob['storage']['type'])
            cjob['+ProminenceStorageMountPoint'] = condor_str(jjob['storage']['mountpoint'])
            if jjob['storage']['type'] == 'b2drop':
                cjob['+ProminenceStorageCredentials'] = condor_str('%s/%s' % (jjob['storage']['b2drop']['app-username'], jjob['storage']['b2drop']['app-password']))
            elif jjob['storage']['type'] == 'onedata':
                cjob['+ProminenceStorageCredentials'] = condor_str('%s/%s' % (jjob['storage']['onedata']['provider'], jjob['storage']['onedata']['token']))

    # Job router
    cjob['+ProminenceWantJobRouter'] = str('(ProminenceMaxIdleTime =?= 0 || (ProminenceMaxIdleTime > 0 && JobStatus == 1 && CurrentTime - EnteredCurrentStatus > ProminenceMaxIdleTime)) && Preemptible =!= True')

    # Output files
    if 'outputFiles' in jjob:
        cjob['+ProminenceOutputFiles'] = condor_str(','.join(jjob['outputFiles']))

        output_locations_put = []
        output_locations_get = []

        for filename in jjob['outputFiles']:
            filename_base = os.path.basename(filename)
            url_put = create_swift_url('PUT', '/v1/prominence-jobs/%s/%s' % (uid, filename_base), 864000)
            url_get = create_swift_url('GET', '/v1/prominence-jobs/%s/%s' % (uid, filename_base), 864000)
            output_locations_put.append(url_put)
            output_locations_get.append(url_get)

        if jjob['outputFiles']:
            cjob['+ProminenceOutputLocations'] = condor_str(",".join(output_locations_put))
            cjob['+ProminenceOutputLocationsUser'] = condor_str(",".join(output_locations_get))

    # Artifacts
    artifacts = []
    if 'artifacts' in jjob:
        for artifact in jjob['artifacts']:
            artifacts.append(artifact)
            if 'http' not in artifact:
                artifact = create_swift_url('GET', '/v1/prominence-jobs/%s/%s' % (username, artifact), 6000)
            input_files.append(artifact)
        cjob['+ProminenceArtifacts'] = condor_str(",".join(artifacts))

    cjob['transfer_input_files'] = str(','.join(input_files))

    if 'outputDirs' in jjob:
        cjob['+ProminenceOutputDirs'] = condor_str(','.join(jjob['outputDirs']))

    # Set max runtime
    max_run_time = 43200
    if 'walltime' in jjob['resources']:
        if jjob['resources']['walltime'] > -1:
            max_run_time = int(jjob['resources']['walltime'])*60
    cjob['periodic_hold'] = str('JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName)' % max_run_time)
    cjob['periodic_hold_subcode'] = str('ifThenElse(JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName), 1001, 1000)' % max_run_time)
    max_run_time /= 60
    cjob['+ProminenceMaxRunTime'] = str("%d" % max_run_time)

    # Is job MPI?
    if 'tasks' in jjob:
        if 'type' in jjob['tasks'][0]:
            if jjob['tasks'][0]['type'] == 'openmpi':
                cjob['+ProminenceWantMPI'] = 'true'
                cjob['+ProminenceMPIType'] = condor_str('openmpi')
            elif jjob['tasks'][0]['type'] == 'mpich':
                cjob['+ProminenceWantMPI'] = 'true'
                cjob['+ProminenceMPIType'] = condor_str('mpich')

    # Prepare for submission to a remote HPC system
    if jjob['resources']['nodes'] > 1 or 1 == 1:
        tasks = jjob['resources']['nodes']
        cpusPerTask = jjob['resources']['cpus']
        memoryPerCpu = jjob['resources']['memory']*1000
        timeRequired = '{:02d}:{:02d}:00'.format(*divmod(max_run_time/60, 60))
        cjob['+remote_cerequirements'] = "RequiredTasks == %d && RequiredMemoryPerCpu == %d && RequiredCpusPerTask == %d && RequiredTime == \"%s\"" % (tasks, memoryPerCpu, cpusPerTask, timeRequired)

    # Set max idle time for local resources
    max_idle_time = 0
    if 'constraints' in jjob:
        if 'maxidletime' in jjob['constraints']:
            max_idle_time = int(jjob['constraints']['maxidletime'])
            cjob['+HookKeyword'] = condor_str('CONTAINER')
        if 'site' in jjob['constraints']:
            cjob['+ProminenceWantCloud'] = condor_str(jjob['constraints']['site'])
    cjob['+ProminenceMaxIdleTime'] = str("%d" % max_idle_time)

    if 'labels' in jjob:
        valid = True
        labels_list = []
        for label in jjob['labels']:
            value = jjob['labels'][label]
            match_obj_label = re.match(r'([\w]+)', label)
            match_obj_value = re.match(r'([\w\-\_\.\/]+)', value)
            if match_obj_label and match_obj_value and len(label) < 64 and len(value) < 64:
                cjob[str('+ProminenceUserMetadata_%s' % label)] = str('"%s"' % value)
                labels_list.append('%s=%s' % (label, value))
            else:
                return (1, {"error":"Invalid label specified"})

        cjob['+ProminenceUserMetadata'] = condor_str(','.join(labels_list))

    data = {}
    retval = 0

    try:
        sub = htcondor.Submit(cjob)
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            cid = sub.queue(txn, 1)
        data['id'] = cid
    except IOError:
        retval = 1
        data = {"error":"Job submission failed"}

    return (retval, data)

def kill_proc(proc, timeout):
    timeout["value"] = True
    proc.kill()

def run(cmd, cwd, timeout_sec):
    proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    timeout = {"value": False}
    timer = Timer(timeout_sec, kill_proc, [proc, timeout])
    timer.start()
    stdout, stderr = proc.communicate()
    timer.cancel()
    return proc.returncode, stdout, stderr, timeout["value"]

def create_workflow(username, group, uid, job_sandbox, jjob):
    """
    Create a workflow
    """
    dag = []
    if 'jobs' in jjob:
        with open(job_sandbox + '/workflow.json', 'w') as fd:
            json.dump(jjob, fd)

        # Firstly, generate unique Swift temporary URLs for output/input files
        file_maps = {}
        for job in jjob['jobs']:
            if 'outputFiles' in job:
                for filename in job['outputFiles']:
                    filename_base = os.path.basename(filename)
                    url_put = create_swift_url('PUT', '/v1/prominence-jobs/%s/%s' % (uid, filename_base), 864000)
                    url_get = create_swift_url('GET', '/v1/prominence-jobs/%s/%s' % (uid, filename_base), 864000)
                    file_maps[filename_base] = (filename, url_put, url_get)

        # Check for storage specified in workflow, to be applied to all jobs
        storage_type = None
        storage_creds = None
        if 'storage' in jjob:
            if 'type' in jjob['storage']:
                storage_type = jjob['storage']['type']
                if jjob['storage']['type'] == 'b2drop':
                    storage_creds = '%s/%s' % (jjob['storage']['b2drop']['app-username'], jjob['storage']['b2drop']['app-password'])
                    storage_mountpoint = jjob['storage']['b2drop']['mountpoint']

        for job in jjob['jobs']:

            info = {}

            # If a job name is not defined, create one as we require all jobs to have a name
            if 'name' not in job:
                job['name'] = str(uuid.uuid4())

            # Check validity of name
            match_obj_name = re.match(r'([\w\-\_]+)', job['name'])
            if not match_obj_name or len(job['name']) > 64:
                return (1, {"error":"Invalid job name specified"})

            info['name'] = job['name']

            # Create job sandbox
            os.makedirs(job_sandbox + '/' + job['name'])
            os.makedirs(job_sandbox + '/' + job['name'] + '/input')
            job_filename = job_sandbox + '/' + job['name'] + '/job.jdl'

            info['uuid'] = uid
            info['username'] = username

            if 'memory' in job['resources']:
                info['memory'] = job['resources']['memory']
                info['reqmemory'] = 1000*int(job['resources']['memory'])

            if 'cpus' in job['resources']:
                info['cpus'] = job['resources']['cpus']

            if 'nodes' in job['resources']:
                info['nodes'] = job['resources']['nodes']
            else:
                info['nodes'] = 1

            if 'disk' in job and int(job['resources']['disk']) > 1:
                info['disk'] = job['resources']['disk']
            elif int(job['resources']['disk']) > 1:
                info['disk'] = job['resources']['disk']
            else:
                info['disk'] = 10

            info['wantmpi'] = 'False'

            if 'storage' in job:
                if 'type' in job['storage']:
                    info['storagetype'] = job['storage']['type']
                    if job['storage']['type'] == 'b2drop':
                        info['storagecreds'] = '%s/%s' % (job['storage']['b2drop']['app-username'], job['storage']['b2drop']['app-password'])
                        info['storagemountpoint'] = job['storage']['b2drop']['mountpoint']
            elif storage_type is not None and storage_creds is not None and storage_mountpoint is not None:
                info['storagetype'] = storage_type
                info['storagecreds'] = storage_creds
                info['storagemountpoint'] = storage_mountpoint
            else:
                info['storagetype'] = 'None'
                info['storagecreds'] = 'None'
                info['storagemountpoint'] = 'None'

            # If more than 1 node has been requested, assume MPI
            if int(info['nodes']) > 1:
                info['wantmpi'] = 'True'

            # Check if MPI was explicitly requested
            if 'type' in job:
                if job['type'] == 'mpi':
                    info['wantmpi'] = 'True'

            if info['wantmpi'] == 'True':
                info['processes'] = int(job['resources']['cpus'])*int(job['resources']['nodes'])
            else:
                info['processes'] = 1

            info['maxidle'] = 0

            cjob = {}
            input_files = ['.job.json']

            instances = 1
            if 'instances' in job:
                instances = int(job['instances'])
            info['instances'] = instances

            # Artifacts
            artifacts = []
            if 'artifacts' in job:
                for artifact in job['artifacts']:
                    artifacts.append(artifact)
                    if artifact in file_maps:
                        artifact = (file_maps[filename_base])[2]
                    elif 'http' not in artifact and artifact not in file_maps:
                        artifact = create_swift_url('GET', '/v1/prominence-jobs/%s/%s' % (username, artifact), 6000)
                    input_files.append(artifact)
                cjob['+ProminenceArtifacts'] = condor_str(",".join(artifacts))
            cjob['transfer_input_files'] = str(','.join(input_files))

            # Output files
            if 'outputFiles' in job:
                cjob['+ProminenceOutputFiles'] = condor_str(','.join(job['outputFiles']))

                output_locations_put = []
                output_locations_get = []

                for filename in job['outputFiles']:
                    filename_base = os.path.basename(filename)
                    url_put = (file_maps[filename_base])[1]
                    url_get = (file_maps[filename_base])[2]
                    output_locations_put.append(url_put)
                    output_locations_get.append(url_get)

                if job['outputFiles']:
                    cjob['+ProminenceOutputLocations'] = condor_str(",".join(output_locations_put))
                    cjob['+ProminenceOutputLocationsUser'] = condor_str(",".join(output_locations_get))

            contents_additional = "\n"
            for key in cjob:
                contents_additional += "%s = %s\n" % (key, cjob[key])
            info['extras'] = contents_additional

            # Write JDL
            with open(job_filename, 'w') as fd:
                fd.write(JOB_SUBMIT % info)
            dag.append('JOB ' + job['name'] + ' job.jdl DIR ' + job['name'])

            # Write .tasks.json
            json_tasks = job['tasks']
            filename = job_sandbox + '/' + job['name'] + '/.tasks.json'
            with open(filename, 'w') as file:
                json.dump(json_tasks, file)

            # Copy executable
            copyfile('/etc/prominence/promlet.py', os.path.join(job_sandbox, job['name'], 'promlet.py'))
            os.chmod(job_sandbox + '/' + job['name'] + '/promlet.py', 0775)

    # Create dag
    if 'dependencies' in jjob:
        for relationship in jjob['dependencies']:
            parents = " ".join(relationship['parents'])
            children = " ".join(relationship['children'])
            dag.append('PARENT ' + parents + ' CHILD ' + children)
    dag.append('NODE_STATUS_FILE workflow.dag.status')
    with open(job_sandbox + '/job.dag', 'w') as fd:
        fd.write('\n'.join(dag))

    wf_name = ''
    if 'name' in jjob:
        wf_name = str(jjob['name'])

    data = {}

    # Submit dag
    cmd = "condor_submit_dag -batch-name %s -append '+ProminenceType=\"workflow\"' -append '+ProminenceIdentity=\"%s\"' -append '+ProminenceJobUniqueIdentifier=\"%s\"' job.dag" % (wf_name, username, uid)
    (return_code, stdout, stderr, timedout) = run(cmd, job_sandbox, 30)
    m = re.search(r'submitted to cluster\s(\d+)', stdout)
    if m:
        retval = 201
        data['id'] = int(m.group(1))
    else:
        retval = 1
        data = {"error":"Job submission failed"}

    return (retval, data)

def delete_job(username, job_id):
    """
    Delete the specified job
    """
    schedd = htcondor.Schedd()
    ret = schedd.act(htcondor.JobAction.Remove,
                     'ProminenceIdentity == "%s" && ClusterId == %d && Cmd != "/bin/condor_dagman"' % (username, job_id))

    if ret["TotalSuccess"] > 0:
        return (0, {})
    return (1, {"error":"No such job"})

def delete_workflow(username, workflow_id):
    """
    Delete the specified workflow
    """
    schedd = htcondor.Schedd()
    ret = schedd.act(htcondor.JobAction.Remove, 'ProminenceIdentity == "%s" && ClusterId == %d && Cmd == "/bin/condor_dagman"' % (username, workflow_id))

    if ret["TotalSuccess"] > 0:
        return (0, {})
    return (1, {"error":"No such workflow"})

def list_jobs(job_id, identity, active, completed, num, detail, constraint):
    """
    List jobs or describe a specified job
    """
    required_attrs = ['JobStatus',
                      'ClusterId',
                      'ProcId',
                      'DAGManJobId',
                      'ProminenceMemoryPerNode',
                      'ProminenceCpusPerNode',
                      'ProminenceNumNodes',
                      'ProminenceSharedDiskSize',
                      'ProminenceMaxRunTime',
                      'ProminenceWantMPIVersion',
                      'ProminenceInfrastructureSite',
                      'ProminenceInfrastructureState',
                      'QDate',
                      'JobStartDate',
                      'JobCurrentStartExecutingDate',
                      'CompletionDate',
                      'EnteredCurrentStatus',
                      'RemoveReason',
                      'LastHoldReasonSubCode',
                      'ProminenceUserInputFiles',
                      'ProminenceOutputFiles',
                      'ProminenceOutputDirs',
                      'ProminenceUserEnvironment',
                      'ProminenceUserMetadata',
                      'ProminenceOutputLocationsUser',
                      'ProminenceOutputDirLocationsUser',
                      'TransferInput',
                      'ProminenceJobUniqueIdentifier',
                      'ProminenceArtifacts',
                      'ProminenceName',
                      'ProminenceExitCode',
                      'ProminencePreemptible',
                      'ProminenceImagePullSuccess',
                      'ProminenceStorageType',
                      'ProminenceStorageCredentials',
                      'ProminenceStorageMountPoint',
                      'Iwd']
    jobs_state_map = {1:'idle',
                      2:'running',
                      3:'deleted',
                      4:'completed',
                      5:'held'}

    schedd = htcondor.Schedd()

    jobs = []
    jobs_condor = []

    if constraint[0] is not None and constraint[1] is not None:
        restrict = str('ProminenceUserMetadata_%s =?= "%s"' % (constraint[0], constraint[1]))
    else:
        restrict = 'True'
    constraintc = 'ProminenceIdentity =?= "%s" && %s' % (identity, restrict)
    if int(job_id) > -1:
        constraintc = 'ClusterId =?= %s && %s' % (job_id, constraintc)

    # Get completed jobs if necessary
    if completed:
        jobs_completed = schedd.history('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs, int(num))
        jobs_condor.extend(jobs_completed)

    # Get active jobs if necessary
    if active:
        jobs_active = schedd.xquery('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs)
        jobs_condor.extend(jobs_active)

    for job in jobs_condor:
        jobj = {}
        jobj['id'] = job['ClusterId']
        jobj['status'] = jobs_state_map[job['JobStatus']]
        jobj['name'] = job['ProminenceName']

        # If job is idle and infrastructure is ready, set status to 'ready'
        if 'ProminenceInfrastructureState' in job:
            if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'configured':
                jobj['status'] = 'ready'
            if job['JobStatus'] == 1 and (job['ProminenceInfrastructureState'] == 'deployment-init' or job['ProminenceInfrastructureState'] == 'creating'):
                jobj['status'] = 'deploying'

        # Get json
        with open(job['Iwd'] + '/.job.json') as json_file:
            tasks = json.load(json_file)['tasks']
        jobj['tasks'] = tasks

        # Return status as failed if container image pull failed
        if 'ProminenceImagePullSuccess' in job:
            if job['ProminenceImagePullSuccess'] == 1:
                jobj['status'] = 'failed'
                jobj['statusReason'] = 'Container image pull failed'

        # Generate useful error messages
        if job['JobStatus'] == 3:
            reason = ''
            if 'ProminenceInfrastructureState' in job:
                if job['ProminenceInfrastructureState'] == "failed":
                    reason = 'Infrastructure deployment failed'
                    jobj['status'] = 'failed'
            if 'RemoveReason' in job:
                if 'Python-initiated action' in job['RemoveReason']:
                    reason = 'Job cancelled by user'
                if 'Infrastructure took too long to be deployed' in job['RemoveReason']:
                    reason = 'Infrastructure took too long to be deployed'
            if 'LastHoldReasonSubCode' in job:
                if job['LastHoldReasonSubCode'] == 1001:
                    reason = 'Runtime limit exceeded'
                    jobj['status'] = 'killed'
            jobj['statusReason'] = reason

        jobj['type'] = 'batch'
        if 'ProminenceWantMPI' in job:
            jobj['type'] = 'mpi'

        if 'ProminencePreemptible' in job:
            jobj['preemptible'] = True

        events = {}
        events['createTime'] = int(job['QDate'])

        if 'JobStartDate' in job and int(job['JobStartDate']) > 0:
            events['startTime'] = int(job['JobStartDate'])

        # Get the job end date if needed. Note that if a job was removed CompletionDate is 0,
        # so we use EnteredCurrentStatus instead
        if 'CompletionDate' in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
            if int(job['CompletionDate']) > 0:
                events['endTime'] = int(job['CompletionDate'])
            elif int(job['CompletionDate']) == 0 and int(job['EnteredCurrentStatus']) > 0:
                events['endTime'] = int(job['EnteredCurrentStatus'])

        if detail > 0:
            resources = {}
            resources['memory'] = int(job['ProminenceMemoryPerNode'])
            resources['cpus'] = int(job['ProminenceCpusPerNode'])
            resources['nodes'] = int(job['ProminenceNumNodes'])
            resources['disk'] = int(job['ProminenceSharedDiskSize'])
            resources['walltime'] = int(job['ProminenceMaxRunTime'])
            jobj['resources'] = resources

            if 'ProminenceStorageType' in job and 'ProminenceStorageCredentials' in job and 'ProminenceStorageMountPoint' in job:
                storage = {}
                storage['type'] = job['ProminenceStorageType']
                storage['mountpoint'] = job['ProminenceStorageMountPoint']
                storage[storage['type']] = {}
                if storage['type'] == 'onedata':
                    storage[storage['type']]['provider'] = '***'
                    storage[storage['type']]['token'] = '***'
                elif storage['type'] == 'b2drop':
                    storage[storage['type']]['app-username'] = '***'
                    storage[storage['type']]['app-password'] = '***'
                jobj['storage'] = storage

            execution = {}
            if 'ProminenceInfrastructureSite' in job:
                if job['ProminenceInfrastructureSite'] != 'none':
                    execution['site'] = str(job['ProminenceInfrastructureSite'])
                if 'ProminenceExitCode' in job:
                    execution['exitCode'] = int(job['ProminenceExitCode'])
                jobj['execution'] = execution

            if 'ProminenceUserInputFiles' in job:
                input_files = str(job['ProminenceUserInputFiles']).split(',')
                jobj['inputFiles'] = input_files

            if 'ProminenceOutputFiles' in job:
                output_files = str(job['ProminenceOutputFiles']).split(',')
                if 'ProminenceOutputLocationsUser' in job:
                    output_locations = str(job['ProminenceOutputLocationsUser']).split(',')
                outputs = []
                for counter, output_file in enumerate(output_files):
                    if 'ProminenceOutputLocationsUser' in job:
                        url = output_locations[counter]
                    else:
                        url = ''
                    file_map = {'name':output_file, 'url':url}
                    outputs.append(file_map)
                jobj['outputFiles'] = outputs
            if 'ProminenceOutputDirs' in job:
                output_dirs = str(job['ProminenceOutputDirs']).split(',')
                if 'ProminenceOutputDirLocationsUser' in job:
                    output_locations = str(job['ProminenceOutputDirLocationsUser']).split(',')
                outputs = []
                for counter, output_dir in enumerate(output_dirs):
                    if 'ProminenceOutputDirLocationsUser' in job:
                        url = output_locations[counter]
                    else:
                        url = ''
                    file_map = {'name':output_dir, 'url':url}
                    outputs.append(file_map)
                jobj['outputDirs'] = outputs
            if 'TransferInput' in job:
                input_files = str(job['TransferInput']).split(',')
            if 'ProminenceArtifacts' in job:
                artifacts = str(job['ProminenceArtifacts']).split(',')
                if artifacts:
                    jobj['artifacts'] = artifacts
            if 'ProminenceUserMetadata' in job:
                metadata = []
                for var in str(job['ProminenceUserMetadata']).split(','):
                    key = var.split("=")[0]
                    value = var.split("=")[1]
                    metadata.append({key:value})
                jobj['labels'] = metadata

        jobj['events'] = events

        jobs.append(jobj)

    return jobs

def list_workflows(workflow_id, identity, active, completed, num, detail, constraint):
    """
    List workflows or describe a specified workflow
    """
    required_attrs = ['JobStatus',
                      'ClusterId',
                      'ProcId',
                      'DAGManJobId',
                      'JobBatchName',
                      'QDate',
                      'Cmd',
                      'Iwd'
                      ]
    jobs_state_map = {1:'idle',
                      2:'running',
                      3:'deleted',
                      4:'completed',
                      5:'held'}

    schedd = htcondor.Schedd()

    wfs = []
    wfs_condor = []

    if constraint[0] is not None and constraint[1] is not None:
        restrict = str('ProminenceUserMetadata_%s =?= "%s"' % (constraint[0], constraint[1]))
    else:
        restrict = 'True'
    constraintc = 'ProminenceIdentity =?= "%s" && %s' % (identity, restrict)
    if int(workflow_id) > -1:
        constraintc = 'ClusterId =?= %s && %s' % (workflow_id, constraintc)

    # Get completed workflows if necessary
    if completed:
        wfs_completed = schedd.history('RoutedBy =?= undefined && ProminenceType == "workflow" && %s' % constraintc, required_attrs, int(num))
        wfs_condor.extend(wfs_completed)

    # Get active workflows if necessary
    if active:
        wfs_active = schedd.xquery('RoutedBy =?= undefined && ProminenceType == "workflow" && %s' % constraintc, required_attrs)
        wfs_condor.extend(wfs_active)

    for wf in wfs_condor:
        wfj = {}

        if detail > 0:
            with open('%s/workflow.json' % wf['Iwd'], 'r') as json_file:
                wfj = json.load(json_file)
        else:
            wfj['name'] = wf['JobBatchName']

        # Redact credentials if necessary
        if 'storage' in wfj:
            wfj['storage'] = redact_storage_creds(wfj['storage'])
        if 'jobs' in wfj:
            for job in wfj['jobs']:
                if 'storage' in job:
                    job['storage'] = redact_storage_creds(job['storage'])

        wfj['id'] = wf['ClusterId']
        wfj['status'] = jobs_state_map[wf['JobStatus']]

        events = {}
        events['createTime'] = int(wf['QDate'])
        wfj['events'] = events

        nodes_total = 0
        nodes_done = 0
        nodes_failed = 0
        nodes_queued = 0
        nodes_unready = 0
        dag_status = 0

        file = '%s/workflow.dag.status' % wf['Iwd']

        node_state_map = {0:'waiting',
                          1:'idle',
                          3:'running',
                          5:'completed',
                          6:'failed'}

        try:
            class_ads = classad.parseAds(open(file, 'r'))
            for class_ad in class_ads:
                if ['Type'] == 'DagStatus':
                    nodes_total = class_ad['NodesTotal']
                    nodes_done = class_ad['NodesDone']
                    nodes_failed = class_ad['NodesFailed']
                    nodes_queued = class_ad['NodesQueued']
                    nodes_unready = class_ad['NodesUnready']
                    dag_status = class_ad['DagStatus']
                if class_ad['Type'] == 'NodeStatus':
                    node = class_ad['Node']
                    status = node_state_map[class_ad['NodeStatus']]
        except Exception:
            pass

        nodes = {}
        jobs = {}

        nodes['total'] = nodes_total
        nodes['done'] = nodes_done
        nodes['failed'] = nodes_failed
        nodes['queued'] = nodes_queued
        nodes['waiting'] = nodes_unready

        wfj['progress'] = nodes

        wfs.append(wfj)

    return wfs

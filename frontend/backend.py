""" HTCondor backend"""
import base64
import json
import os
import re
import shlex
import shutil
import subprocess
import threading
import uuid

import boto3
import classad
import htcondor

import validate

# Job template
JOB_SUBMIT = \
"""
universe = vanilla
executable = promlet.py
arguments = --job .job.mapped.json --id $(prominencecount) %(extra_args)s
output = job.$(prominencecount).out
error = job.$(prominencecount).err
log = job.$(prominencecount).log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_output_files = promlet.$(prominencecount).log,promlet.$(prominencecount).json
skip_filechecks = true
requirements = false
transfer_executable = true
stream_output = true
stream_error = true
RequestCpus = %(cpus)s
RequestMemory = %(reqmemory)s
+ProminenceJobUniqueIdentifier = %(uuid)s
+ProminenceIdentity = %(username)s
+ProminenceGroup = %(group)s
+ProminenceName = %(name)s
+ProminenceMemoryPerNode = %(memory)s
+ProminenceCpusPerNode = %(cpus)s
+ProminenceNumNodes = %(nodes)s
+ProminenceSharedDiskSize = %(disk)s
+ProminenceMaxIdleTime = %(maxidle)s
+ProminenceMaxTimeInQueue = %(maxtimeinqueue)s
+ProminenceWantMPI = %(wantmpi)s
+ProminenceType = "job"
+WantIOProxy = true
%(extras)s
%(extras_metadata)s
queue 1
"""

def write_htcondor_job(cjob, filename):
    """
    Write a HTCondor JDL
    """
    keys = ['transfer_input_files',
            '+ProminenceWantJobRouter',
            '+remote_cerequirements_default',
            '+ProminenceFactoryId',
            '+ProminenceWorkflowName']
    extras = "\n"
    for key in keys:
        if key in cjob:
            extras += "%s = %s\n" % (key, cjob[key])

    info = {}
    info['name'] = cjob['+ProminenceName']
    info['uuid'] = cjob['+ProminenceJobUniqueIdentifier']
    info['username'] = cjob['+ProminenceIdentity']
    info['group'] = cjob['+ProminenceGroup']
    info['memory'] = cjob['+ProminenceMemoryPerNode']
    info['reqmemory'] = cjob['RequestMemory']
    info['cpus'] = cjob['+ProminenceCpusPerNode']
    info['nodes'] = cjob['+ProminenceNumNodes']
    info['disk'] = cjob['+ProminenceSharedDiskSize']
    info['wantmpi'] = cjob['+ProminenceWantMPI']
    info['maxidle'] = 0
    info['maxtimeinqueue'] = cjob['+ProminenceMaxTimeInQueue']
    info['extras'] = extras
    if 'extra_args' in cjob:
        info['extra_args'] = cjob['extra_args']
    else:
        info['extra_args'] = ''

    # Add any labels
    extras_metadata = ''
    for item in cjob:
        if 'ProminenceUserMetadata' in item:
            extras_metadata += '%s = %s\n' % (item, cjob[item])
    info['extras_metadata'] = extras_metadata

    # Write to a file
    try:
        with open(filename, 'w') as fd:
            fd.write(JOB_SUBMIT % info)
    except IOError:
        return False

    return True

def condor_str(str_in):
    """
    Returns a double-quoted string
    """
    return str('"%s"' % str_in)

def kill_proc(proc, timeout):
    """
    Helper function used by "run"
    """
    timeout["value"] = True
    proc.kill()

def run(cmd, cwd, timeout_sec):
    """
    Run a subprocess, capturing stdout & stderr, with a timeout
    """
    proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    timeout = {"value": False}
    timer = threading.Timer(timeout_sec, kill_proc, [proc, timeout])
    timer.start()
    stdout, stderr = proc.communicate()
    timer.cancel()
    return proc.returncode, stdout, stderr, timeout["value"]

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

def delete_object(url, access_key_id, secret_access_key, bucket, key):
    """
    Delete object from object storage
    """
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=url,
                                 aws_access_key_id=access_key_id,
                                 aws_secret_access_key=secret_access_key)
        response = s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return False

    return True

def get_matching_s3_objects(url, access_key_id, secret_access_key, bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket filtered by a prefix and/or suffix
    """
    s3 = boto3.client('s3',
                      endpoint_url=url,
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key)
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj

class ProminenceBackend(object):
    """
    PROMINENCE backend class
    """

    def __init__(self, config):
        self._config = config
        self._promlet_file = '/usr/local/libexec/promlet.py'

    def output_params(self, workflow):
        """
        Generate params
        """
        params = ''
        count = 0

        for job in workflow['jobs']:
            if 'outputFiles' in job:
                for filename in job['outputFiles']:
                    params += ' --outfile %s=$(prominenceout%d) ' % (filename, count)
                    count += 1

            if 'outputDirs' in job:
                for filename in job['outputDirs']:
                    params += ' --outdir %s=$(prominenceout%d) ' % (filename, count)
                    count += 1

        return params

    def write_parameter_value(self, value):
        """
        Write a parameter value, taking into account its type
        """
        if isinstance(value, int):
            return '%d' % value
        elif isinstance(value, float):
            return '%f' % value
        elif isinstance(value, basestring):
            return '%s' % value

    def output_urls(self, workflow, uid, label):
        """
        Generate output files/dirs
        """
        lists = ''
        count = 0

        for job in workflow['jobs']:

            if 'outputFiles' in job:
                for filename in job['outputFiles']:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%d/%s' % (uid, label, os.path.basename(filename)),
                                                        604800)
                    lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                    count += 1

            if 'outputDirs' in job:
                for dirname in job['outputDirs']:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%d/%s.tgz' % (uid, label, os.path.basename(dirname)),
                                                        604800)
                    lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                    count += 1

        return lists

    def get_snapshot_url(self, uid):
        """
        Return a pre-signed URL to retrieve a snapshot
        """
        return str(self.create_presigned_url('get', self._config['S3_BUCKET'], 'snapshots/%s/snapshot.tgz' % uid, 3600))

    def create_sandbox(self, uid):
        """
        Create job sandbox
        """
        job_sandbox = self._config['SANDBOX_PATH'] + '/' + uid
        try:
            os.makedirs(job_sandbox)
            os.makedirs(job_sandbox + '/input')
        except:
            return None
        return job_sandbox

    def create_presigned_url(self, method, bucket_name, object_name, duration_in_seconds=600):
        """
        Create presigned S3 URL
        """
        s3_client = boto3.client('s3',
                                 endpoint_url=self._config['S3_URL'],
                                 aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                                 aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])
        if method == 'get':
            try:
                response = s3_client.generate_presigned_url('get_object',
                                                            Params={'Bucket': bucket_name, 'Key': object_name},
                                                            ExpiresIn=duration_in_seconds)
            except Exception:
                return None
        else:
            try:
                response = s3_client.generate_presigned_url('put_object',
                                                            Params={'Bucket':bucket_name, 'Key':object_name},
                                                            ExpiresIn=duration_in_seconds,
                                                            HttpMethod='PUT')
            except Exception:
                return None

        return response

    def list_objects(self, user, groups, path=None):
        """
        List objects in S3 storage
        """
        if path is None:
            prefix = 'uploads/%s' % user
            prefix_to_remove = ['uploads', user]
        else:
            prefix = 'uploads/%s' % path
            prefix_to_remove = ['uploads']

        objects = []

        try:
            keys = get_matching_s3_objects(self._config['S3_URL'],
                                           self._config['S3_ACCESS_KEY_ID'],
                                           self._config['S3_SECRET_ACCESS_KEY'],
                                           self._config['S3_BUCKET'],
                                           prefix=prefix)
        except Exception:
            return objects

        for key in keys:
            name = key['Key']
            pieces = name.split('/')
            for item in prefix_to_remove:
                pieces.remove(item)
            obj = {}
            obj['name'] = '/'.join(pieces)
            obj['size'] = key['Size']
            obj['lastModified'] = key['LastModified']
            objects.append(obj)

        return objects

    def delete_object(self, username, group, obj):
        """
        Delete object from object storage
        """
        if '/' in obj:
            key = 'uploads/%s' % obj
        else:
            key = 'uploads/%s/%s' % (username, obj)

        success = delete_object(self._config['S3_URL'],
                                self._config['S3_ACCESS_KEY_ID'],
                                self._config['S3_SECRET_ACCESS_KEY'],
                                self._config['S3_BUCKET'],
                                key)

        return success

    def get_job_unique_id(self, job_id):
        """
        Return the uid and identity for a specified job id
        """
        uid = None
        identity = None
        name = None
        iwd = None
        out = None
        err = None
        status = -1
        schedd = htcondor.Schedd()
        jobs_condor = schedd.history('RoutedBy =?= undefined && ClusterId =?= %s' % job_id,
                                     ['ProminenceJobUniqueIdentifier', 'ProminenceIdentity', 'Iwd', 'Out', 'Err', 'DAGNodeName', 'JobStatus'], 1)
        for job in jobs_condor:
            if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
                uid = job['ProminenceJobUniqueIdentifier']
                identity = job['ProminenceIdentity']
                iwd = job['Iwd']
                out = job['Out']
                err = job['Err']
                status = job['JobStatus']
                # If a job has a DAGNodeName it must be part of a workflow, and to get the stdout/err of a such
                # a job we need to know the job name
                if 'DAGNodeName' in job:
                    name = job['DAGNodeName']

        if uid is None or identity is None:
            jobs_condor = schedd.xquery('RoutedBy =?= undefined && ClusterId =?= %s' % job_id,
                                        ['ProminenceJobUniqueIdentifier', 'ProminenceIdentity', 'Iwd', 'Out', 'Err', 'DAGNodeName', 'JobStatus'], 1)
            for job in jobs_condor:
                if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
                    uid = job['ProminenceJobUniqueIdentifier']
                    identity = job['ProminenceIdentity']
                    iwd = job['Iwd']
                    out = job['Out']
                    err = job['Err']
                    status = job['JobStatus']
                    # If a job has a DAGNodeName it must be part of a workflow, and to get the stdout/err of a such
                    # a job we need to know the job name
                    if 'DAGNodeName' in job:
                        name = job['DAGNodeName']
        return (uid, identity, iwd, out, err, name, status)

    def _get_routed_job_id(self, job_id):
        """
        Return the routed job id
        """
        schedd = htcondor.Schedd()
        jobs_condor = schedd.xquery('RoutedBy =?= undefined && ClusterId =?= %s' % job_id, ['RoutedToJobId'], 1)
        for job in jobs_condor:
            if 'RoutedToJobId' in job:
                return int(float(job['RoutedToJobId']))
        return None

    def _create_htcondor_job(self, username, groups, email, uid, jjob, job_path, workflow=False):
        """
        Create a dict representing a HTCondor job & write the JSON job description
        (original & mapped) files to disk
        """
        cjob = {}

        # Copy of job (mapped version)
        jjob_mapped = jjob.copy()

        # Write any input files to sandbox directory
        input_files = []
        if 'inputs' in jjob:
            filenames = []
            for file_input in jjob['inputs']:
                filename_new = os.path.join(job_path + '/input', os.path.basename(file_input['filename']))
                try:
                    with open(filename_new, 'w') as file:
                        file.write(base64.b64decode(file_input['content']))
                except IOError:
                    return (1, {"error":"Unable to write input file to disk"}, cjob)
                filenames.append(file_input['filename'])
                input_files.append(filename_new)

        # Set default number of nodes if not already specified
        if 'nodes' not in jjob['resources']:
            jjob['resources']['nodes'] = 1

        # Write original job.json
        try:
            with open(os.path.join(job_path, '.job.json'), 'w') as file:
                json.dump(jjob, file)
        except IOError:
            return (1, {"error":"Unable to write .job.json"}, cjob)

        # Replace image identifiers with S3 presigned URLs if necessary
        tasks_mapped = []
        count_task = 0
        for task in jjob['tasks']:
            if 'http' not in task['image'] and ('.tar' in task['image'] or
                                                '.tgz' in task['image'] or
                                                '.simg' in task['image'] or
                                                '.sif' in task['image']):
                image = task['image']

                # Check if image is the same as a previous task
                count_task_check = 0
                found = False
                for task_check in jjob['tasks']:
                    if image == task_check['image'] and count_task_check < count_task:
                        found = True
                        break
                    count_task_check += 1

                # Replace image name as necessary
                if found and count_task_check < count_task:
                    task['image'] = tasks_mapped[count_task_check]
                else:
                    if '/' in task['image']:
                        path = task['image']
                    else:
                        path = '%s/%s' % (username, task['image'])
                    task['image'] = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 604800)
                    url_exists = validate.validate_presigned_url(task['image'])
                    if not url_exists:
                        return (1, {"error":"Image %s does not exist" % image}, cjob)

            tasks_mapped.append(task)
            count_task += 1

        jjob_mapped['tasks'] = tasks_mapped

        # Include the mapped JSON job description as an input file
        input_files.append(os.path.join(job_path, '.job.json'))
        input_files.append(os.path.join(job_path, '.job.mapped.json'))

        # Standard defaults
        cjob['universe'] = 'vanilla'
        cjob['transfer_executable'] = 'true'
        cjob['executable'] = 'promlet.py'
        cjob['arguments'] = '--job .job.mapped.json --id 0'

        cjob['Log'] = job_path + '/job.0.log'
        cjob['Output'] = job_path + '/job.0.out'
        cjob['Error'] = job_path +  '/job.0.err'
        cjob['should_transfer_files'] = 'YES'
        cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
        cjob['skip_filechecks'] = 'true'
        cjob['transfer_output_files'] = 'promlet.0.log,promlet.0.json'
        cjob['+WantIOProxy'] = 'true'
        cjob['+ProminenceType'] = condor_str('job')

        cjob['stream_error'] = 'true'
        cjob['stream_output'] = 'true'

        # Job name
        if 'name' in jjob:
            cjob['+ProminenceName'] = condor_str(jjob['name'])
        else:
            cjob['+ProminenceName'] = condor_str('')

        # Job uid
        cjob['+ProminenceJobUniqueIdentifier'] = condor_str(uid)

        # Username
        cjob['+ProminenceIdentity'] = condor_str(username)

        # Group
        if groups:
            cjob['+ProminenceGroup'] = condor_str(groups)

        # Email
        if email:
            cjob['+ProminenceEmail'] = condor_str(email)

        # Memory required
        cjob['+ProminenceMemoryPerNode'] = str(jjob['resources']['memory'])
        cjob['RequestMemory'] = str(1000*int(jjob['resources']['memory']))

        # CPUs required
        cjob['+ProminenceCpusPerNode'] = str(jjob['resources']['cpus'])
        cjob['RequestCpus'] = str(jjob['resources']['cpus'])

        # Nodes
        cjob['+ProminenceNumNodes'] = str(jjob['resources']['nodes'])

        # Disk
        if 'disk' not in jjob['resources']:
            cjob['+ProminenceSharedDiskSize'] = str(10)
        else:
            cjob['+ProminenceSharedDiskSize'] = str(jjob['resources']['disk'])

        # Preemptible
        if 'preemptible' in jjob:
            cjob['+ProminencePreemptible'] = 'true'

        # Job router
        cjob['+ProminenceWantJobRouter'] = str('(ProminenceMaxIdleTime =?= 0 || (ProminenceMaxIdleTime > 0 && JobStatus == 1 && CurrentTime - EnteredCurrentStatus > ProminenceMaxIdleTime)) && Preemptible =!= True')

        # Artifacts
        artifacts = []
        if 'artifacts' in jjob:
            for artifact in jjob['artifacts']:
                artifact_url = artifact['url']
                artifacts.append(artifact_url)
                if 'http' not in artifact_url:
                    artifact_original = artifact_url
                    if '/' in artifact_url:
                        path = artifact_url
                    else:
                        path = '%s/%s' % (username, artifact_url)
                    artifact_url = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 604800)
                    url_exists = validate.validate_presigned_url(artifact_url)
                    if not url_exists:
                        return (1, {"error":"Artifact %s does not exist" % artifact_original}, cjob)
                input_files.append(artifact_url)
        cjob['transfer_input_files'] = str(','.join(input_files))

        # Output files
        if 'outputFiles' in jjob:
            output_files_new = []
            output_locations_put = []

            for filename in jjob['outputFiles']:
                if not workflow:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s' % (uid, os.path.basename(filename)),
                                                        604800)
                else:
                    url_put = filename
                output_locations_put.append(url_put)
                output_files_new.append({'name':filename, 'url':url_put})

            jjob_mapped['outputFiles'] = output_files_new

        # Output directories    
        if 'outputDirs' in jjob:
            output_dirs_new = []
            output_locations_put = []

            for dirname in jjob['outputDirs']:
                if not workflow:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s.tgz' % (uid, os.path.basename(dirname)),
                                                        604800)
                else:
                    url_put = dirname
                output_locations_put.append(url_put)
                output_dirs_new.append({'name':dirname, 'url':url_put})

            jjob_mapped['outputDirs'] = output_dirs_new

        # Set max walltime, noting that the promlet will kill jobs anyway when the max
        # walltime has been exceeded
        max_run_time = 43200.0*3
        if 'walltime' in jjob['resources']:
            if jjob['resources']['walltime'] > -1:
                max_run_time = int(jjob['resources']['walltime'])*60*3
        cjob['periodic_hold'] = str('JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName)' % max_run_time)
        cjob['periodic_hold_subcode'] = str('ifThenElse(JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName), 1001, 1000)' % max_run_time)
        cjob['+ProminenceMaxRunTime'] = str("%d" % (max_run_time/60))

        # Is job MPI?
        cjob['+ProminenceWantMPI'] = 'false'
        if jjob['resources']['nodes'] > 1:
            cjob['+ProminenceWantMPI'] = 'true'
        if 'tasks' in jjob:
            for task in jjob['tasks']:
                if 'type' in task:
                    if task['type'] in ('openmpi', 'mpich', 'intelmpi'):
                        cjob['+ProminenceWantMPI'] = 'true'

        # Prepare for submission to a remote HPC system
        tasks = jjob['resources']['nodes']
        cpusPerTask = jjob['resources']['cpus']
        memoryPerCpu = jjob['resources']['memory']*1000
        cjob['+remote_cerequirements_default'] = condor_str("RequiredTasks == %d && RequiredMemoryPerCpu == %d && RequiredCpusPerTask == %d && RequiredTime == %d" % (tasks, memoryPerCpu, cpusPerTask, max_run_time))

        # Set max idle time per resource
        max_idle_time = 0
        if 'policies' in jjob:
            if 'maximumIdleTimePerResource' in jjob['policies']:
                max_idle_time = int(jjob['policies']['maximumIdleTimePerResource'])
        cjob['+ProminenceMaxIdleTime'] = str("%d" % max_idle_time)

        # Maximum time in queue
        max_time_in_queue = -1
        if 'policies' in jjob:
            if 'maximumTimeInQueue' in jjob['policies']:
                # Convert mins to secs
                max_time_in_queue = 60*int(jjob['policies']['maximumTimeInQueue'])
        cjob['+ProminenceMaxTimeInQueue'] = str("%d" % max_time_in_queue)

        # Handle labels
        if 'labels' in jjob:
            valid = True
            labels_list = []
            for label in jjob['labels']:
                value = jjob['labels'][label]
                cjob[str('+ProminenceUserMetadata_%s' % label)] = str('"%s"' % value)
                labels_list.append('%s=%s' % (label, value))

            cjob['+ProminenceUserMetadata'] = condor_str(','.join(labels_list))

        # Write mapped job.json
        try:
            with open(os.path.join(job_path, '.job.mapped.json'), 'w') as file:
                json.dump(jjob_mapped, file)
        except IOError as err:
            return (1, {"error":"Unable to write .job.mapped.json due to %s" % err}, cjob)

        # Write empty files for stdout & stderr - for jobs submitted to a batch system, these will
        # not be initially created by HTCondor by default
        try:
            open('%s/job.0.out' % job_path, 'a').close()
            open('%s/job.0.err' % job_path, 'a').close()
        except IOError:
            pass

        return (0, {}, cjob)

    def create_job(self, username, groups, email, uid, jjob):
        """
        Create a job
        """
        # Create the job sandbox
        job_sandbox = self.create_sandbox(uid)
        if job_sandbox is None:
            return (1, {"error":"Unable to create job sandbox"})

        # Copy executable to sandbox, change current working directory to the sandbox
        shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, 'promlet.py'))
     
        os.chdir(job_sandbox)
        os.chmod(os.path.join(job_sandbox, 'promlet.py'), 0o775)

        # Create dict containing HTCondor job
        (status, msg, cjob) = self._create_htcondor_job(username, groups, email, uid, jjob, job_sandbox)

        # Check if we have an error
        if status != 0:
            return (1, msg)

        # Submit the job to HTCondor
        data = {}
        retval = 0

        try:
            sub = htcondor.Submit(cjob)
            schedd = htcondor.Schedd()
            with schedd.transaction() as txn:
                cid = sub.queue(txn, 1)
            data['id'] = cid
        except Exception as err:
            print('Exception submitting job to HTCondor:', err)
            retval = 1
            data = {"error":"Job submission failed with an exception"}

        return (retval, data)

    def create_workflow(self, username, groups, email, uid, jjob):
        """
        Create a workflow
        """
        # Firstly, create the workflow sandbox
        job_sandbox = self.create_sandbox(uid)
        if job_sandbox is None:
            return (1, {"error":"Unable to create workflow sandbox"})

        # Workflow name
        wf_name = ''
        if 'name' in jjob:
            wf_name = str(jjob['name'])

        # Write the workflow JSON description to disk
        try:
            with open(job_sandbox + '/workflow.json', 'w') as fd:
                json.dump(jjob, fd)
        except IOError:
            return (1, {"error":"Unable to write workflow.json"})

        dag = []

        # Retries
        if 'policies' in jjob:
            if 'maximumRetries' in jjob['policies']:
                dag.append('RETRY ALL_NODES %d' % jjob['policies']['maximumRetries'])

        if 'dependencies' in jjob or 'factory' not in jjob:
            # Handle DAG workflows & bags of jobs
            for job in jjob['jobs']:
                # All jobs must have names
                if 'name' not in job:
                    return (1, {"error":"All jobs in a workflow must have names"})

                # Create job sandbox
                try:
                    os.makedirs(job_sandbox + '/' + job['name'])
                    os.makedirs(job_sandbox + '/' + job['name'] + '/input')
                except IOError:
                    return (1, {"error":"Unable to create job sandbox directories"})

                job_filename = job_sandbox + '/' + job['name'] + '/job.jdl'

                # Create dict containing HTCondor job
                (status, msg, cjob) = self._create_htcondor_job(username, groups, email, str(uuid.uuid4()), job, job_sandbox + '/' + job['name'])
                cjob['+ProminenceWorkflowName'] = condor_str(wf_name)

                # Write JDL
                if not write_htcondor_job(cjob, job_filename):
                    return (1, {"error":"Unable to write JDL for job"})

                # Append job to DAG description
                dag.append('JOB ' + job['name'] + ' job.jdl DIR ' + job['name'])
                dag.append('VARS ' + job['name'] + ' prominencecount="0"')

                # Copy executable to job sandbox
                shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, job['name'], 'promlet.py'))
                os.chmod(job_sandbox + '/' + job['name'] + '/promlet.py', 0o775)

            # Define dependencies if necessary
            if 'dependencies' in jjob:
                for parent in jjob['dependencies']:
                    children = " ".join(jjob['dependencies'][parent])
                    dag.append('PARENT ' + parent + ' CHILD ' + children)

        elif 'factory' in jjob:
            # Copy executable to job sandbox
            shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, 'promlet.py'))
            os.chmod(job_sandbox + '/promlet.py', 0o775)

            # Create dict containing HTCondor job
            (status, msg, cjob) = self._create_htcondor_job(username, groups, email, str(uuid.uuid4()), jjob['jobs'][0], job_sandbox, True)
            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)
            cjob['+ProminenceFactoryId'] = '$(prominencecount)'

            if jjob['factory']['type'] == 'parametricSweep':
                num_dimensions = len(jjob['factory']['parameters'])

                if num_dimensions == 1:
                    ps_name = jjob['factory']['parameters'][0]['name']
                    ps_start = float(jjob['factory']['parameters'][0]['start'])
                    ps_end = float(jjob['factory']['parameters'][0]['end'])
                    ps_step = float(jjob['factory']['parameters'][0]['step'])

                    cjob['extra_args'] = '--param %s=$(prominencevalue0) %s' % (ps_name, self.output_params(jjob))
                
                    value = ps_start
                    job_count = 0
                    while value <= ps_end:
                        dag.append('JOB job%d job.jdl' % job_count)
                        dag.append('VARS job%d prominencevalue0="%s" prominencecount="%d" %s' % (job_count,
                                                                                                 self.write_parameter_value(value),
                                                                                                 job_count,
                                                                                                 self.output_urls(jjob, uid, job_count)))
                        value += ps_step
                        job_count += 1           

                else:
                    ps_num = []
                    ps_name = []
                    ps_start = []
                    ps_end = []
                    ps_step = []

                    for i in range(num_dimensions):
                        ps_name.append(jjob['factory']['parameters'][i]['name'])
                        ps_start.append(float(jjob['factory']['parameters'][i]['start']))
                        ps_end.append(float(jjob['factory']['parameters'][i]['end']))
                        ps_step.append(float(jjob['factory']['parameters'][i]['step']))

                        # Determine the number of values for each parameter
                        value = ps_start[i]
                        count = 0
                        while value <= ps_end[i]:
                            value += ps_step[i]
                            count += 1
                        ps_num.append(count)

                    # Generate extra_args
                    cjob['extra_args'] = self.output_params(jjob) + ' '
                    for i in range(num_dimensions):
                        cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (ps_name[i], i)

                    # TODO: need to work out how to have n nested for loops, for arbitrary n

                    if num_dimensions == 2:
                        job_count = 0
                        for x1 in range(ps_num[0]):
                            for y1 in range(ps_num[1]):
                                x1_val = ps_start[0] + x1*ps_step[0]
                                y1_val = ps_start[1] + y1*ps_step[1]
                                dag.append('JOB job%d job.jdl' % job_count)
                                dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencecount="%d" %s' % (job_count, self.write_parameter_value(x1_val), self.write_parameter_value(y1_val), job_count, self.output_urls(jjob, uid, job_count)))
                                job_count += 1

                    elif num_dimensions == 3:
                        job_count = 0
                        for x1 in range(ps_num[0]):
                            for y1 in range(ps_num[1]):
                                for z1 in range(ps_num[2]):
                                    x1_val = ps_start[0] + x1*ps_step[0]
                                    y1_val = ps_start[1] + y1*ps_step[1]
                                    z1_val = ps_start[2] + z1*ps_step[2]
                                    dag.append('JOB job%d job.jdl' % job_count)
                                    dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencecount="%d" %s' % (job_count, self.write_parameter_value(x1_val), self.write_parameter_value(y1_val), self.write_parameter_value(z1_val), job_count, self.output_urls(jjob, uid, job_count)))
                                    job_count += 1

                    elif num_dimensions == 4:
                        job_count = 0
                        for x1 in range(ps_num[0]):
                            for y1 in range(ps_num[1]):
                                for z1 in range(ps_num[2]):
                                    for t1 in range(ps_num[3]):
                                        x1_val = ps_start[0] + x1*ps_step[0]
                                        y1_val = ps_start[1] + y1*ps_step[1]
                                        z1_val = ps_start[2] + z1*ps_step[2]
                                        t1_val = ps_start[3] + t1*ps_step[3]
                                        dag.append('JOB job%d job.jdl' % job_count)
                                        dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencevalue3="%s" prominencecount="%d" %s' % (job_count, self.write_parameter_value(x1_val), self.write_parameter_value(y1_val), self.write_parameter_value(z1_val), self.write_parameter_value(t1_val), job_count, self.output_urls(jjob, uid, job_count)))
                                        job_count += 1

                    elif num_dimensions > 4:
                        return (1, {"error": "Currently only parameter sweeps up to 4D are supported"})                       

            elif jjob['factory']['type'] == 'zip':

                cjob['extra_args'] = self.output_params(jjob) + ' '
                for index in range(len(jjob['factory']['parameters'])):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (jjob['factory']['parameters'][index]['name'], index)
                for index in range(len(jjob['factory']['parameters'][0]['values'])):
                    parameters = []
                    count = 0
                    for parameter in jjob['factory']['parameters']:
                        parameters.append('prominencevalue%d="%s"' % (count, self.write_parameter_value(parameter['values'][index])))
                        count += 1
                    dag.append('JOB job%d job.jdl' % index)
                    dag.append('VARS job%d %s prominencecount="%d" %s' % (index,
                                                                          ' '.join(parameters),
                                                                          index,
                                                                          self.output_urls(jjob, uid, index)))

            # Write JDL
            if not write_htcondor_job(cjob, '%s/job.jdl' % job_sandbox):
                return (1, {"error":"Unable to write JDL for job"})

        # DAGMan status file
        dag.append('NODE_STATUS_FILE workflow.dag.status')

        # Write DAGMan definition file
        try:
            with open(job_sandbox + '/job.dag', 'w') as fd:
                fd.write('\n'.join(dag))
        except IOError:
            return (1, {"error":"Unable to write DAG file for job"})

        # Handle labels
        dag_appends = []
        if 'labels' in jjob:
            for label in jjob['labels']:
                value = jjob['labels'][label]
                dag_appends.append("'+ProminenceUserMetadata_%s=\"%s\"'" % (label, value))

        # Create command to submit to DAGMan
        dag_appends.append("'+ProminenceType=\"workflow\"'")
        dag_appends.append("'+ProminenceIdentity=\"%s\"'" % username)
        dag_appends.append("'+ProminenceJobUniqueIdentifier=\"%s\"'" % uid)

        if email:
            dag_appends.append("'+ProminenceEmail=\"%s\"'" % email)

        cmd = "condor_submit_dag -maxidle %d -batch-name %s " % (int(self._config['WORKFLOW_MAX_IDLE']), wf_name)
        for dag_append in dag_appends:
            cmd += " -append %s " % dag_append
        cmd += " job.dag "

        # Submit to DAGMan
        (return_code, stdout, stderr, timedout) = run(cmd, job_sandbox, 30)
        m = re.search(r'submitted to cluster\s(\d+)', stdout)
        data = {}
        if m:
            retval = 201
            data['id'] = int(m.group(1))
        else:
            retval = 1
            data = {"error":"Workflow submission failed"}

        return (retval, data)

    def delete_job(self, username, job_ids):
        """
        Delete the specified job(s)
        """
        constraints = []
        for job_id in job_ids:
            constraints.append('ClusterId == %d' % int(job_id))
        constraint = '(%s) && ProminenceIdentity == "%s" && ProminenceType == "job"' % (' || '.join(constraints), username)

        schedd = htcondor.Schedd()
        ret = schedd.act(htcondor.JobAction.Remove, constraint)

        if ret["TotalSuccess"] > 0:
            return (0, {})
        return (1, {"error":"No such job(s)"})

    def delete_workflows(self, username, workflow_ids):
        """
        Delete the specified workflow(s)
        """
        constraints = []
        for workflow_id in workflow_ids:
            constraints.append('ClusterId == %d' % int(workflow_id))
        constraint = '(%s) && ProminenceIdentity == "%s" && ProminenceType == "workflow"' % (' || '.join(constraints), username)

        schedd = htcondor.Schedd()
        ret = schedd.act(htcondor.JobAction.Remove, constraint)

        if ret["TotalSuccess"] > 0:
            return (0, {})
        return (1, {"error":"No such workflow(s)"})

    def list_jobs(self, job_ids, identity, active, completed, workflow, num, detail, constraint):
        """
        List jobs or describe a specified job
        """
        required_attrs = ['JobStatus',
                          'LastJobStatus',
                          'ClusterId',
                          'ProcId',
                          'DAGManJobId',
                          'ProminenceInfrastructureSite',
                          'ProminenceInfrastructureState',
                          'ProminenceInfrastructureType',
                          'QDate',
                          'GridJobStatus',
                          'JobCurrentStartDate',
                          'JobRunCount',
                          'JobCurrentStartExecutingDate',
                          'CompletionDate',
                          'EnteredCurrentStatus',
                          'RemoveReason',
                          'RemoteWallClockTime',
                          'LastHoldReasonSubCode',
                          'ProminenceUserEnvironment',
                          'ProminenceUserMetadata',
                          'TransferInput',
                          'ProminenceJobUniqueIdentifier',
                          'ProminenceName',
                          'ProminenceFactoryId',
                          'ProminenceWorkflowName',
                          'ProminenceExitCode',
                          'ProminencePreemptible',
                          'ProminenceImagePullSuccess',
                          'Iwd',
                          'Args']
        jobs_state_map = {1:'idle',
                          2:'running',
                          3:'deleted',
                          4:'completed',
                          5:'failed'}

        schedd = htcondor.Schedd()

        jobs = []
        jobs_condor = []

        if constraint[0] is not None and constraint[1] is not None:
            restrict = str('ProminenceUserMetadata_%s =?= "%s"' % (constraint[0], constraint[1]))
        else:
            restrict = 'True'
        constraintc = 'ProminenceIdentity =?= "%s" && %s' % (identity, restrict)
        if len(job_ids) > 0 and not workflow:
            constraints = []
            for job_id in job_ids:
                constraints.append('ClusterId == %d' % int(job_id))
            constraintc = '(%s) && %s' % (' || '.join(constraints), constraintc)
            num = len(job_ids)

        if workflow and len(job_ids) > 0:
            constraintc = '(DAGManJobId == %d) && %s' % (int(job_ids[0]), constraintc)

        # Get completed jobs if necessary
        if completed:
            jobs_completed = schedd.history('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs, int(num))
            jobs_condor.extend(jobs_completed)

        # Get active jobs if necessary
        if active:
            jobs_active = schedd.xquery('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs)
            jobs_condor.extend(jobs_active)

        for job in jobs_condor:
            # Get json from file
            try:
                with open(job['Iwd'] + '/.job.json') as json_file:
                    job_json_file = json.load(json_file)
            except:
                continue

            jobj = {}
            jobj['id'] = job['ClusterId']
            jobj['status'] = jobs_state_map[job['JobStatus']]
            jobj['tasks'] = job_json_file['tasks']
 
            # Job name - for jobs from workflows, use the name "<workflow name>/<job name>/(<number>)"
            jobj['name'] = ''
            if 'name' in job_json_file:
                jobj['name'] = job_json_file['name']
                if 'ProminenceWorkflowName' in job:
                    jobj['name'] = '%s/%s' % (job['ProminenceWorkflowName'], jobj['name'])
                    if 'ProminenceFactoryId' in job:
                        jobj['name'] = '%s/%s' % (jobj['name'], job['ProminenceFactoryId'])

            # Set job status as appropriate
            if 'ProminenceInfrastructureState' in job:
                if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'configured':
                    jobj['status'] = 'deploying'
                if job['JobStatus'] == 1 and (job['ProminenceInfrastructureState'] == 'deployment-init' or job['ProminenceInfrastructureState'] == 'creating'):
                    jobj['status'] = 'deploying'
                if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'unable':
                    jobj['status'] = 'idle'
                    jobj['statusReason'] = 'No matching resources'
                if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'failed':
                    jobj['status'] = 'idle'
                    jobj['statusReason'] = 'Deployment failed'

            # Handle idle jobs on remote batch systems
            if 'ProminenceInfrastructureType' in job:
                if job['ProminenceInfrastructureType'] == 'batch':
                    if 'GridJobStatus' in job:
                        if job['GridJobStatus'] == "IDLE" and job['JobStatus'] == 1:
                            jobj['status'] = 'idle'

            # Get promlet output if exists (only for completed jobs)
            promlet_json_filename = '%s/promlet.0.json' % job['Iwd']
            if 'ProminenceFactoryId' in job:
                promlet_json_filename = '%s/promlet.%d.json' % (job['Iwd'], int(job['ProminenceFactoryId']))

            # Handle old jobs temporarily
            if not os.path.isfile(promlet_json_filename) and os.path.isfile('%s/promlet.json' % job['Iwd']):
                promlet_json_filename = '%s/promlet.json' % job['Iwd']
  
            # Read in promlet.json
            job_u = {}
            try:
                with open(promlet_json_filename) as promlet_json_file:
                    job_u = json.load(promlet_json_file)
            except:
                pass

            tasks_u = []
            if 'tasks' in job_u:
                tasks_u = job_u['tasks']
            elif job_u:
                # Handle original promlet.json format
                tasks_u = job_u

            stageout_u = {}
            if 'stageout' in job_u:
                stageout_u = job_u['stageout']

            # Job parameters
            parameters = {}
            if 'ProminenceFactoryId' in job:
                matches = re.findall('--param ([\w]+)=([\w\.]+)', job['Args'])
                if matches:
                    for match in matches:
                        parameters[match[0]] = match[1]
                jobj['parameters'] = parameters

            # Return status as failed if container image pull failed
            if 'ProminenceImagePullSuccess' in job:
                if job['ProminenceImagePullSuccess'] == 1:
                    jobj['status'] = 'failed'
                    jobj['statusReason'] = 'Container image pull failed'

            for task in tasks_u:
                if 'imagePullStatus' in task:
                    if task['imagePullStatus'] == 'failed':
                        jobj['status'] = 'failed'
                        jobj['statusReason'] = 'Container image pull failed'

            # Generate useful error messages
            if 'JobRunCount' in job:
                if job['JobStatus'] == 1 and job['JobRunCount'] > 0:
                    jobj['status'] = 'failed'
                    jobj['statusReason'] = ''

            if job['JobStatus'] == 3:
                reason = ''
                if 'ProminenceInfrastructureState' in job:
                    if job['ProminenceInfrastructureState'] == "failed":
                        reason = 'Infrastructure deployment failed'
                        jobj['status'] = 'failed'
                    if job['ProminenceInfrastructureState'] == "unable":
                        reason = 'No resources meet the specified requirements'
                        jobj['status'] = 'failed'
                if 'RemoveReason' in job:
                    if 'Python-initiated action' in job['RemoveReason']:
                        reason = 'Job deleted by user'
                        jobj['status'] = 'deleted'
                    if 'Infrastructure took too long to be deployed' in job['RemoveReason']:
                        reason = 'Infrastructure took too long to be deployed'
                    if 'OtherJobRemoveRequirements = DAGManJobId' in job['RemoveReason'] and 'was removed' in job['RemoveReason']:
                        reason = 'Job part of a workflow which was deleted by user'

                if 'LastHoldReasonSubCode' in job:
                    if job['LastHoldReasonSubCode'] == 1001:
                        reason = 'Runtime limit exceeded'
                        jobj['status'] = 'killed'
                jobj['statusReason'] = reason

            # Return status as failed if walltime limit execeed
            if tasks_u:
                for task_u in tasks_u:
                    if 'error' in task_u:
                        jobj['status'] = 'failed'
                        jobj['statusReason'] = 'Walltime limit exceeded'

            if 'ProminencePreemptible' in job:
                jobj['preemptible'] = True

            events = {}
            events['createTime'] = int(job['QDate'])

            if 'JobCurrentStartDate' in job and int(job['JobCurrentStartDate']) > 0:
                events['startTime'] = int(job['JobCurrentStartDate'])

            # For remote jobs on remote HTC/HPC, JobCurrentStartDate doesn't exist
            if 'JobCurrentStartDate' not in job and job['JobStatus'] == 2:
                events['startTime'] = int(job['EnteredCurrentStatus'])

            if 'JobCurrentStartDate' not in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
                if int(job['RemoteWallClockTime']) > 0 and int(job['CompletionDate']) > 0:
                    events['startTime'] = int(job['CompletionDate']) - int(job['RemoteWallClockTime'])

            # Get the job end date if needed. Note that if a job was removed CompletionDate is 0,
            # so we use EnteredCurrentStatus instead
            if 'CompletionDate' in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
                if int(job['CompletionDate']) > 0:
                    events['endTime'] = int(job['CompletionDate'])
                elif int(job['CompletionDate']) == 0 and int(job['EnteredCurrentStatus']) > 0 and 'JobCurrentStartDate' in job:
                    events['endTime'] = int(job['EnteredCurrentStatus'])

            # Set end time for a job which was evicted
            if 'LastJobStatus' in job:
                if job['LastJobStatus'] == 2 and job['JobStatus'] == 1:
                    events['endTime'] = int(job['EnteredCurrentStatus'])

            if detail > 0:
                jobj['resources'] = job_json_file['resources']

                if 'policies' in job_json_file:
                    jobj['policies'] = job_json_file['policies']

                if 'artifacts' in job_json_file:
                    jobj['artifacts'] = job_json_file['artifacts']

                if 'inputs' in job_json_file:
                    jobj['inputs'] = job_json_file['inputs']

                if 'labels' in job_json_file:
                    jobj['labels'] = job_json_file['labels']

                if 'constraints' in job_json_file:
                    jobj['constraints'] = job_json_file['constraints']

                if 'storage' in job_json_file:
                    storage = {}
                    storage['type'] = job_json_file['storage']['type']
                    storage['mountpoint'] = job_json_file['storage']['mountpoint']
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
                    new_tasks_u = []
                    if tasks_u:
                        for task_u in tasks_u:
                            if 'maxMemoryUsageKB' in task_u:
                                execution['maxMemoryUsageKB'] = task_u['maxMemoryUsageKB']
                            elif 'error' in task_u:
                                job_wall_time_limit_exceeded = True
                            else:
                                new_tasks_u.append(task_u)
                        execution['tasks'] = new_tasks_u
                    jobj['execution'] = execution

                if 'ProminenceJobUniqueIdentifier' in job:
                    uid = str(job['ProminenceJobUniqueIdentifier'])

                fid = uid
                if 'ProminenceFactoryId' in job:
                    uid = job['Iwd'].split('/')[len(job['Iwd'].split('/'))-1]
                    fid = '%s/%s' % (uid, job['ProminenceFactoryId'])

                if 'outputFiles' in job_json_file:
                    outputs = []
                    for output_file in job_json_file['outputFiles']:
                        filename = os.path.basename(output_file)
                        stageout_success = False
                        url = ''
                        if 'files' in stageout_u:
                            for file in stageout_u['files']:
                                if file['name'] == output_file and file['status'] == 'success':
                                    url = self.create_presigned_url('get',
                                                                    self._config['S3_BUCKET'], 
                                                                    'scratch/%s/%s' % (fid, filename),
                                                                    600)
                        file_map = {'name':output_file, 'url':url}
                        outputs.append(file_map)
                    jobj['outputFiles'] = outputs

                if 'outputDirs' in job_json_file:
                    outputs = []
                    for output_dir in job_json_file['outputDirs']:
                        dirs = output_dir.split('/')
                        dirname_base = dirs[len(dirs) - 1]
                        stageout_success = False
                        url = ''
                        if 'directories' in stageout_u:
                            for directory in stageout_u['directories']:
                                if directory['name'] == output_dir and directory['status'] == 'success':
                                    url = self.create_presigned_url('get',
                                                                    self._config['S3_BUCKET'],
                                                                    'scratch/%s/%s.tgz' % (fid, dirname_base),
                                                                    600)
                        file_map = {'name':output_dir, 'url':url}
                        outputs.append(file_map)
                    jobj['outputDirs'] = outputs

            jobj['events'] = events

            jobs.append(jobj)

        return jobs

    def list_workflows(self, workflow_ids, identity, active, completed, num, detail, constraint):
        """
        List workflows or describe a specified workflow
        """
        required_attrs = ['JobStatus',
                          'ClusterId',
                          'ProcId',
                          'DAGManJobId',
                          'JobBatchName',
                          'RemoveReason',
                          'QDate',
                          'JobStartDate',
                          'Cmd',
                          'Iwd'
                          ]
        jobs_state_map = {1:'idle',
                          2:'running',
                          3:'failed',
                          4:'completed',
                          5:'failed'}

        schedd = htcondor.Schedd()

        wfs = []
        wfs_condor = []

        if constraint[0] is not None and constraint[1] is not None:
            restrict = str('ProminenceUserMetadata_%s =?= "%s"' % (constraint[0], constraint[1]))
        else:
            restrict = 'True'
        constraintc = 'ProminenceIdentity =?= "%s" && %s' % (identity, restrict)
        if len(workflow_ids) > 0:
            constraints = []
            for workflow_id in workflow_ids:
                constraints.append('ClusterId == %d' % int(workflow_id))
            constraintc = '(%s) && %s' % (' || '.join(constraints), constraintc)
            num = len(workflow_ids)

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
                try:
                    with open('%s/workflow.json' % wf['Iwd'], 'r') as json_file:
                        wfj = json.load(json_file)
                except IOError:
                    continue
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
            if 'JobStartDate' in wf:
                events['startTime'] = int(wf['JobStartDate'])

            # The end time of a DAG job does not appear to be in the job's ClassAd, so instead we
            # get the end time from the job.dag.metrics file
            dag_metrics = {}
            try:
                with open('%s/job.dag.metrics' % wf['Iwd'], 'r') as json_file:
                    dag_metrics = json.load(json_file)
            except IOError:
                pass

            if 'end_time' in dag_metrics:
                events['endTime'] = int(dag_metrics['end_time'])

            wfj['events'] = events

            nodes_total = 0
            nodes_done = 0
            nodes_failed = 0
            nodes_queued = 0
            nodes_unready = 0
            dag_status = 0

            node_stats = {}

            file = '%s/workflow.dag.status' % wf['Iwd']

            node_state_map = {0:'waiting',
                              1:'idle',
                              3:'running',
                              5:'completed',
                              6:'failed'}

            try:
                class_ads = classad.parseAds(open(file, 'r'))
                for class_ad in class_ads:
                    if class_ad['Type'] == 'DagStatus':
                        nodes_total = class_ad['NodesTotal']
                        nodes_done = class_ad['NodesDone']
                        nodes_failed = class_ad['NodesFailed']
                        nodes_queued = class_ad['NodesQueued']
                        nodes_unready = class_ad['NodesUnready']
                        dag_status = class_ad['DagStatus']
                    if class_ad['Type'] == 'NodeStatus':
                        node = class_ad['Node']
                        stats = {}
                        stats['status'] = node_state_map[class_ad['NodeStatus']]
                        stats['retries'] = class_ad['RetryCount']
                        node_stats[node] = stats
            except Exception:
                pass

            nodes = {}
            jobs = {}

            # If no jobs have been created, report status as idle
            if nodes_queued == 0 and wfj['status'] != 'completed' and wfj['status'] != 'failed':
                wfj['status'] = 'idle'

            nodes['total'] = nodes_total
            nodes['done'] = nodes_done
            nodes['failed'] = nodes_failed
            nodes['queued'] = nodes_queued
            nodes['waiting'] = nodes_unready

            # Completed workflows with failed jobs should be reported as failed, not completed
            if wfj['status'] == 'completed' and nodes_failed > 0:
                wfj['status'] = 'failed'

            # Handle workflow deleted by user
            if 'RemoveReason' in wf:
                if 'Python-initiated action' in wf['RemoveReason']:
                    wfj['statusReason'] = 'Workflow deleted by user'
                    wfj['status'] = 'deleted'

            wfj['progress'] = nodes

            wfs.append(wfj)

        return wfs

    def get_stdout(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1):
        """
        Return the stdout from the specified job
        """
        if instance_id > -1:
            if os.path.isfile('%s/job.%d.out' % (iwd, instance_id)):
                with open('%s/job.%d.out' % (iwd, instance_id)) as fd:
                    return fd.read()
        elif os.path.isfile('%s/%s' % (iwd, out)):
            with open('%s/%s' % (iwd, out)) as fd:
                return fd.read()
        elif os.path.isfile('%s/%s/job.0.out' % (iwd, job_name)):
            with open('%s/%s/job.0.out' % (iwd, job_name)) as fd:
                return fd.read()
        elif os.path.isfile(out):
            with open(out) as fd:
                return fd.read()
        return None

    def get_stderr(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1):
        """
        Return the stdout from the specified job
        """
        if instance_id > -1:
            if os.path.isfile('%s/job.%d.err' % (iwd, instance_id)):
                with open('%s/job.%d.err' % (iwd, instance_id)) as fd:
                    return fd.read()
        elif os.path.isfile('%s/%s' % (iwd, err)):
            with open('%s/%s' % (iwd, err)) as fd:
                return fd.read()
        elif os.path.isfile('%s/%s/job.0.err' % (iwd, job_name)):
            with open('%s/%s/job.0.err' % (iwd, job_name)) as fd:
                return fd.read()
        elif os.path.isfile(err):
            with open(err) as fd:
                return fd.read()
        return None

    def execute_command(self, job_id, iwd, command):
        """
        Execute a command inside a job
        """
        job_id_routed = self._get_routed_job_id(job_id)
        if not job_id_routed:
            return None

        args = ['condor_ssh_to_job', '%d' % job_id_routed]
        args.extend(self.modify_exec_command(iwd, command))
        
        process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        timeout = {"value": False}
        timer = threading.Timer(int(self._config['EXEC_TIMEOUT']), kill_proc, [process, timeout])
        timer.start()
        output = process.communicate()[0]
        timer.cancel()

        if process.returncode == 0:
            return output
        return None

    def create_snapshot(self, uid, job_id, path):
        """
        Create a snapshot of the specified path
        """
        # Firstly create the PUT URL
        snapshot_url = self.create_presigned_url('put', self._config['S3_BUCKET'], 'snapshots/%s/snapshot.tgz' % uid, 1000)

        job_id_routed = self._get_routed_job_id(job_id)
        if not job_id_routed:
            return None
    
        # Create a tarball & upload to S3
        cmd = 'condor_ssh_to_job %d "tar czf snapshot.tgz %s && curl --upload-file snapshot.tgz \\\"%s\\\""' % (job_id_routed, path, snapshot_url.encode('utf-8'))
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        timeout = {"value": False}
        timer = threading.Timer(int(self._config['EXEC_TIMEOUT']), kill_proc, [process, timeout])
        timer.start()
        output = process.communicate()[0]
        timer.cancel()

        return 0

    def validate_snapshot_path(self, iwd, path):
        """
        Validate the path used for a snapshot
        """
        try:
            with open(iwd + '/.job.json') as json_file:
                job = json.load(json_file)
        except:
            return None

        found = None
        if 'artifacts' in job:
            for artifact in job['artifacts']:
                if 'mountpoint' in artifact:
                    mountpoint = artifact['mountpoint'].split(':')[1]
                    directory = artifact['mountpoint'].split(':')[0]
                    if path == mountpoint:
                        found = directory

        if not found and path.startswith('/'):
            return None
        elif path.startswith('/'):
            return found

        return path

    def modify_exec_command(self, iwd, command):
        """
        Replace any artifact mounts with actual path
        """
        try:
            with open(iwd + '/.job.json') as json_file:
                job = json.load(json_file)
        except:
            return None

        if 'artifacts' in job:
            for artifact in job['artifacts']:
                if 'mountpoint' in artifact:
                    mountpoint = artifact['mountpoint'].split(':')[1]
                    directory = artifact['mountpoint'].split(':')[0]
                    command = [item.replace(mountpoint, directory) for item in command]

        return command

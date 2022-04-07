import base64
import json
import os
import math
import requests

from .utilities import condor_str, retry
from .create_job_token import create_job_token

@retry(tries=2, delay=1, backoff=1)
def validate_presigned_url(url):
    """
    Validate a presigned URL
    """
    try:
        response = requests.get(url, stream=True, timeout=30)
    except requests.exceptions.RequestException:
        return False

    if response.status_code != 200:
        return False
    return True

def _create_htcondor_job(self, username, groups, email, uid, jjob, job_path, workflow=False, jobfactory=False, workflowuid=None, joblabel=None):
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
                with open(filename_new, 'wb') as file:
                    file.write(base64.b64decode(file_input['content']))
            except IOError:
                return (1, {"error":"Unable to write input file to disk"}, cjob)

            if 'executable' in file_input:
                if file_input['executable']:
                    try:
                        os.chmod(filename_new, 0o775)
                    except IOError:
                        return (1, {"error":"Unable to set input file permissions to executable"}, cjob)

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

    # Use provided storage if necessary
    use_default_object_storage = True
    if 'storage' in jjob:
        if 'default' in jjob['storage']:
            if jjob['storage']['default']:
                use_default_object_storage = False

    # Replace image identifiers with S3 presigned URLs if necessary
    tasks_mapped = []
    count_task = 0
    for task in jjob['tasks']:
        if ('http' not in task['image'] and not task['image'].startswith('/')) and \
           ('.tar' in task['image'] or '.tgz' in task['image'] or
            '.simg' in task['image'] or '.sif' in task['image']):
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
                # Assume an image name beginning with "/" is an absolute path to an image on posix storage
                if not task['image'].startswith('/'):
                    if '/' in task['image']:
                        path = task['image']
                    else:
                        path = '%s/%s' % (username, task['image'])
                    task['image'] = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 864000)
                    url_exists = validate_presigned_url(task['image'])
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

    if jjob['resources']['nodes'] == 1:
        cjob['Output'] = job_path + '/job.0.out'
        cjob['Error'] = job_path +  '/job.0.err'
    else:
        cjob['Output'] = job_path + '/job.0.out.$(Node)'
        cjob['Error'] = job_path +  '/job.0.err.$(Node)'

    cjob['should_transfer_files'] = 'YES'
    cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
    cjob['skip_filechecks'] = 'true'
    cjob['transfer_output_files'] = 'logs,json'
    #cjob['+WantIOProxy'] = 'true'
    cjob['+ProminenceType'] = condor_str('job')

    cjob['stream_error'] = 'true'
    cjob['stream_output'] = 'true'
    #cjob['+HookKeyword'] = condor_str('CONTAINER')

    cjob['transfer_input_files'] = str(','.join(input_files))

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
    else:
        cjob['+ProminenceGroup'] = condor_str('')

    # Email
    if email:
        cjob['+ProminenceEmail'] = condor_str(email)

    # Memory required
    if 'memory' in jjob['resources']:
        cjob['RequestMemory'] = str(1024*int(jjob['resources']['memory']))
    elif 'memoryPerCpu' in jjob['resources'] and 'cpus' in jjob['resources']:
        cjob['RequestMemory'] = str(1024*int(jjob['resources']['memoryPerCpu']*jjob['resources']['cpus']))

    # CPUs required
    if 'cpus' in jjob['resources']:
        cjob['RequestCpus'] = str(jjob['resources']['cpus'])
    elif 'cpusRange' in jjob['resources']:
        cjob['Requirements'] = "Cpus >= %d && (PartitionableSlot || Cpus <= %d)" % (jjob['resources']['cpusRange'][0], jjob['resources']['cpusRange'][1])
        cjob['RequestCpus'] = "ifThenElse(Cpus > %d, %d, Cpus)" % (jjob['resources']['cpusRange'][1], jjob['resources']['cpusRange'][1])
        cjob['Rank'] = "Cpus"

        if 'memoryPerCpu' in jjob['resources']:
            cjob['RequestMemory'] = "%d*ifThenElse(Cpus > %d, %d, Cpus)" % (int(1024*jjob['resources']['memoryPerCpu']),
                                                                            jjob['resources']['cpusRange'][1],
                                                                            jjob['resources']['cpusRange'][1])
    elif 'cpusOptions' in jjob['resources']:
        cjob['Rank'] = "Cpus"
        cjob['RequestCpus'] = "ifThenElse(%d > Cpus, %d, %d)" % (jjob['resources']['cpusOptions'][1],
                                                                 jjob['resources']['cpusOptions'][0],
                                                                 jjob['resources']['cpusOptions'][1])

        if 'memoryPerCpu' in jjob['resources']:
            cjob['RequestMemory'] = "%d*RequestCpus" % int(1024*jjob['resources']['memoryPerCpu'])

    # Disk required (GB converted to KiB)
    cjob['RequestDisk'] = str(jjob['resources']['disk']*10.0**9/2**10)

    # Priority
    if 'policies' in jjob:
        if 'priority' in jjob['policies']:
            cjob['+JobPrio'] = jjob['policies']['priority']

    # Preemptible
    if 'preemptible' in jjob:
        cjob['+ProminencePreemptible'] = 'true'

    # Job router - route idle jobs if they have never been routed before and have been idle for over 40 secs
    # or if they were last routed more than 20 mins ago
    disable_router = False
    if 'policies' in jjob:
        if 'autoScalingType' in jjob['policies']:
            if jjob['policies']['autoScalingType'] == 'none' or not jjob['policies']['autoScalingType']:
                disable_router = True

    if disable_router:
        cjob['+ProminenceWantJobRouter'] = 'false'
    else:
        cjob['+ProminenceWantJobRouter'] = str('JobStatus == 1 && ((CurrentTime - ProminenceLastRouted > 1200) || (CurrentTime - EnteredCurrentStatus > 40 && isUndefined(ProminenceLastRouted)))')

    # Should the job be removed from the queue once finished?
    cjob['+ProminenceRemoveFromQueue'] = 'True'

    if 'policies' in jjob:
        if 'leaveInQueue' in jjob['policies']:
            if jjob['policies']['leaveInQueue']:
                cjob['+ProminenceRemoveFromQueue'] = 'False'
 
    cjob['leave_in_queue'] = '(JobStatus == 4 || JobStatus == 3) && ProminenceRemoveFromQueue =?= False'

    # Site & region requirements
    if 'Requirements' not in cjob:
        cjob['Requirements'] = 'True'
    if 'policies' in jjob:
        if 'placement' in jjob['policies']:
            if 'requirements' in jjob['policies']['placement']:
                if 'sites' in jjob['policies']['placement']['requirements']:
                    sites = ",".join(jjob['policies']['placement']['requirements']['sites'])
                    cjob['Requirements'] = '%s && stringListMember(TARGET.ProminenceCloud, "%s")' % (cjob['Requirements'], sites)
                if 'regions' in jjob['policies']['placement']['requirements']:
                    regions = ",".join(jjob['policies']['placement']['requirements']['regions'])
                    cjob['Requirements'] = '%s && stringListMember(TARGET.ProminenceRegion, "%s")' % (cjob['Requirements'], regions)

    # Artifacts
    artifacts = []
    if 'artifacts' in jjob:
        for artifact in jjob['artifacts']:
            artifact_url = artifact['url']
            if 'http' not in artifact_url and use_default_object_storage:
                artifact_original = artifact_url
                if '/' in artifact_url:
                    path = artifact_url
                else:
                    path = '%s/%s' % (username, artifact_url)
                artifact_url = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 864000)
                url_exists = validate_presigned_url(artifact_url)
                if not url_exists:
                    return (1, {"error":"Artifact %s does not exist" % artifact_original}, cjob)
                artifact['url'] = artifact_url
            artifacts.append(artifact)

        jjob_mapped['artifacts'] = artifacts

    # Output files
    if 'outputFiles' in jjob and not use_default_object_storage:
        output_files_new = []
        for filename in jjob['outputFiles']:
            output_files_new.append({'name':filename})
        jjob_mapped['outputFiles'] = output_files_new
    elif 'outputFiles' in jjob:
        output_files_new = []
        output_locations_put = []

        for filename in jjob['outputFiles']:
            if not jobfactory:
                if not joblabel:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s' % (uid, os.path.basename(filename)),
                                                        864000)
                else:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s/%s' % (workflowuid, joblabel, os.path.basename(filename)),
                                                        864000)
            elif joblabel:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%s/%s' % (workflowuid, joblabel, os.path.basename(filename)),
                                                    864000)
            else:
                url_put = filename
            output_locations_put.append(url_put)
            output_files_new.append({'name':filename, 'url':url_put})

        jjob_mapped['outputFiles'] = output_files_new

    # Output directories
    if 'outputDirs' in jjob and not use_default_object_storage:
        output_dirs_new = []
        for dirname in jjob['outputDirs']:
            output_dirs_new.append({'name':dirname})
        jjob_mapped['outputDirs'] = output_dirs_new
    elif 'outputDirs' in jjob:
        output_dirs_new = []
        output_locations_put = []

        for dirname in jjob['outputDirs']:
            if not jobfactory:
                if not joblabel:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s.tgz' % (uid, os.path.basename(dirname)),
                                                        864000)
                else:
                    url_put = self.create_presigned_url('put',
                                                        self._config['S3_BUCKET'],
                                                        'scratch/%s/%s/%s.tgz' % (workflowuid, joblabel, os.path.basename(dirname)),
                                                        864000)
            elif joblabel:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%s/%s.tgz' % (workflowuid, joblabel, os.path.basename(dirname)),
                                                    864000)
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
    cjob['+ProminenceMaxRunTime'] = str("%d" % (max_run_time/60))

    # Job token
    job_token_uid = uid
    if workflowuid:
        job_token_uid = workflowuid

    token = create_job_token(username,
                             groups,
                             job_token_uid,
                             email,
                             10*24*60*60 + int(max_run_time*2))
    cjob['+ProminenceJobToken'] = condor_str(token)
    cjob['+ProminenceURL'] = condor_str(self._config['URL'])

    # Is job MPI?
    cjob['+ProminenceWantMPI'] = 'false'
    if jjob['resources']['nodes'] > 1:
        cjob['+ProminenceWantMPI'] = 'true'
        cjob['+WantParallelSchedulingGroups'] = 'True'
        cjob['machine_count'] = jjob['resources']['nodes']
        cjob['universe'] = 'parallel'

    if 'tasks' in jjob:
        for task in jjob['tasks']:
            if 'type' in task:
                if task['type'] in ('openmpi', 'mpich', 'intelmpi'):
                    cjob['+ProminenceWantMPI'] = 'true'

    # Dynamic MPI
    if 'totalCpusRange' in jjob['resources'] and 'cpusRange' in jjob['resources']:
        num_nodes_max = math.ceiling(jjob['resources']['totalCpusRange'][1]/jjob['resources']['cpusRange'][1])
        num_nodes_min = math.ceiling(jjob['resources']['totalCpusRange'][0]/jjob['resources']['cpusRange'][0])
        cpus_max = jjob['resources']['cpusRange'][1]
        cpus_min = jjob['resources']['cpusRange'][0]

        cjob['machine_count'] = "ifThenElse(CurrentTime - QDate < 120, %d, %d" % (num_nodes_max, num_nodes_min)
        cjob['RequestCpus'] = "ifThenElse(CurrentTime - QDate < 120, %d, %d" % (cpus_max, cpus_min)

        if 'memoryPerCpu' in jjob['resources']:
            cjob['RequestMemory'] = "%d*RequestCpus" % int(1024*jjob['resources']['memoryPerCpu'])

        cjob['+ProminenceWantMPI'] = 'true'
        cjob['+WantParallelSchedulingGroups'] = 'True'
        cjob['universe'] = 'parallel'

    # Prepare for submission to a remote HPC system
    #tasks = jjob['resources']['nodes']
    #cpus_per_task = jjob['resources']['cpus']
    #if 'memory' in jjob['resources']:
    #    memory_per_cpu = jjob['resources']['memory']*1000
    #else:
    #    memory_per_cpu = jjob['resources']['memoryPerCpu']*jjob['resources']['cpus']*1000
    #cjob['+remote_cerequirements_default'] = condor_str("RequiredTasks == %d && RequiredMemoryPerCpu == %d && RequiredCpusPerTask == %d && RequiredTime == %d" % (tasks, memory_per_cpu, cpus_per_task, max_run_time))

    # Set max idle time per resource
    max_idle_time = 0
    if 'policies' in jjob:
        if 'maximumIdleTimePerResource' in jjob['policies']:
            max_idle_time = int(jjob['policies']['maximumIdleTimePerResource'])
    cjob['+ProminenceMaxIdleTime'] = str("%d" % max_idle_time)

    # Maximum time in queue
    max_time_in_queue = 0
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

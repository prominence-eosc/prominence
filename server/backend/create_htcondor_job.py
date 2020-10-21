import base64
import json
import os
import requests

from .utilities import condor_str, retry

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
                    file.write(str(base64.b64decode(file_input['content'])))
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
        with open(os.path.join(job_path, 'job.json'), 'w') as file:
            json.dump(jjob, file)
    except IOError:
        return (1, {"error":"Unable to write job.json"}, cjob)

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
                    task['image'] = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 604800)
                    url_exists = validate_presigned_url(task['image'])
                    if not url_exists:
                        return (1, {"error":"Image %s does not exist" % image}, cjob)

        tasks_mapped.append(task)
        count_task += 1

    jjob_mapped['tasks'] = tasks_mapped

    # Include the mapped JSON job description as an input file
    input_files.append(os.path.join(job_path, 'job.json'))
    input_files.append(os.path.join(job_path, 'job.mapped.json'))

    # Standard defaults
    cjob['universe'] = 'vanilla'
    cjob['transfer_executable'] = 'true'
    cjob['executable'] = 'promlet.py'
    cjob['arguments'] = '--job job.mapped.json --id 0'
    cjob['Log'] = job_path + '/job.0.log'
    cjob['Output'] = job_path + '/job.0.out'
    cjob['Error'] = job_path +  '/job.0.err'
    cjob['should_transfer_files'] = 'YES'
    cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
    cjob['+SpoolOnEvict']  = 'false'
    cjob['skip_filechecks'] = 'true'
    cjob['transfer_output_files'] = 'promlet.0.log,promlet.0.json'
    cjob['+WantIOProxy'] = 'true'
    cjob['+ProminenceType'] = condor_str('job')
    cjob['stream_error'] = 'true'
    cjob['stream_output'] = 'true'
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

    # Email
    if email:
        cjob['+ProminenceEmail'] = condor_str(email)

    # Memory required
    cjob['RequestMemory'] = str(1000*int(jjob['resources']['memory']))

    # CPUs required
    cjob['RequestCpus'] = str(jjob['resources']['cpus'])

    # Preemptible
    if 'preemptible' in jjob:
        cjob['+ProminencePreemptible'] = 'true'

    # Job router - route idle jobs if they have never been routed before or if they were last routed more than 20 mins ago
    cjob['+ProminenceWantJobRouter'] = str('JobStatus == 1 && ((CurrentTime - ProminenceLastRouted > 1200) || isUndefined(ProminenceLastRouted))')

    # Should the job be removed from the queue once finished?
    cjob['+ProminenceRemoveFromQueue'] = 'True'

    if 'policies' in jjob:
        if 'leaveInQueue' in jjob['policies']:
            if jjob['policies']['leaveInQueue']:
                cjob['+ProminenceRemoveFromQueue'] = 'False'

    cjob['leave_in_queue'] = '(JobStatus == 4 || JobStatus == 3) && ProminenceRemoveFromQueue =?= False'

    # Artifacts
    artifacts = []
    if 'artifacts' in jjob:
        for artifact in jjob['artifacts']:
            artifact_url = artifact['url']
            if 'http' not in artifact_url:
                artifact_original = artifact_url
                if '/' in artifact_url:
                    path = artifact_url
                else:
                    path = '%s/%s' % (username, artifact_url)
                artifact_url = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 604800)
                url_exists = validate_presigned_url(artifact_url)
                if not url_exists:
                    return (1, {"error":"Artifact %s does not exist" % artifact_original}, cjob)
                artifact['url'] = artifact_url
            artifacts.append(artifact)

        jjob_mapped['artifacts'] = artifacts

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
    cpus_per_task = jjob['resources']['cpus']
    memory_per_cpu = jjob['resources']['memory']*1000
    cjob['+remote_cerequirements_default'] = condor_str("RequiredTasks == %d && RequiredMemoryPerCpu == %d && RequiredCpusPerTask == %d && RequiredTime == %d" % (tasks, memory_per_cpu, cpus_per_task, max_run_time))

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
        with open(os.path.join(job_path, 'job.mapped.json'), 'w') as file:
            json.dump(jjob_mapped, file)
    except IOError as err:
        return (1, {"error":"Unable to write job.mapped.json due to %s" % err}, cjob)

    # Write empty files for stdout & stderr - for jobs submitted to a batch system, these will
    # not be initially created by HTCondor by default
    try:
        open('%s/job.0.out' % job_path, 'a').close()
        open('%s/job.0.err' % job_path, 'a').close()
    except IOError:
        pass

    return (0, {}, cjob)

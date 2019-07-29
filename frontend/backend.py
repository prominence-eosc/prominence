import base64
import boto3
import json
import os
import re
import shlex
import subprocess
import uuid
import hmac
from hashlib import sha1
import time

from shutil import copyfile
from threading import Timer

import classad
import htcondor

import validate

# Job template
JOB_SUBMIT = \
"""
universe = vanilla
executable = promlet.py
arguments = --job .job.mapped.json

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

+remote_cerequirements_default = RequiredTasks == 1 && RequiredMemoryPerCpu == 1 && RequiredCpusPerTask == 1 && RequiredTime == 10

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

    def create_job(self, username, groups, uid, jjob):
        """
        Create a job
        """
        # Create the job sandbox
        job_sandbox = self.create_sandbox(uid)
        if job_sandbox is None:
            return (1, {"error":"Unable to create job sandbox"})

        cjob = {}

        # Copy executable to sandbox, change current working directory to the sandbox
        copyfile(self._config['PROMLET_FILE'], os.path.join(job_sandbox, 'promlet.py'))

        os.chdir(job_sandbox)
        os.chmod(os.path.join(job_sandbox, 'promlet.py'), 0775)

        # Copy of job (mapped version)
        jjob_mapped = jjob.copy()

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

        # Default number of nodes
        if 'nodes' not in jjob['resources']:
            jjob['resources']['nodes'] = 1

        # Write original job.json
        with open(os.path.join(job_sandbox, '.job.json'), 'w') as file:
            json.dump(jjob, file)

        # Write tasks definition to file
        if 'tasks' in jjob:

            # Replace image identifiers with S3 presigned URLs
            tasks_new = []
            count_task = 0
            for task in jjob['tasks']:

                if 'http' not in task['image'] and ('.tar' in task['image'] or '.tgz' in task['image'] or '.simg' in task['image'] or '.sif' in task['image']):
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
                        task['image'] = tasks_new[count_task_check]
                    else:
                        if '/' in task['image']:
                            path = task['image']
                        else:
                            path = '%s/%s' % (username, task['image'])
                        task['image'] = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s' % path, 6000)
                        url_exists = validate.validate_presigned_url(task['image'])
                        if not url_exists:
                            return (1, {"error":"Image %s does not exist" % image})

                tasks_new.append(task)
                count_task += 1

            jjob_mapped['tasks'] = tasks_new

            input_files.append(os.path.join(job_sandbox, '.job.mapped.json'))
        else:
            return (1, {"error":"No tasks or workflow specified"})

        if 'name' in jjob:
            cjob['+ProminenceName'] = condor_str(jjob['name'])
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

        if 'nodes' not in jjob['resources']:
            cjob['+ProminenceNumNodes'] = str(1)
        else:
            cjob['+ProminenceNumNodes'] = str(jjob['resources']['nodes'])

        if 'disk' not in jjob['resources']:
            cjob['+ProminenceSharedDiskSize'] = str(10)
        else:
            cjob['+ProminenceSharedDiskSize'] = str(jjob['resources']['disk'])

        cjob['arguments'] = '--job .job.mapped.json'

        cjob['universe'] = 'vanilla'
        cjob['Log'] = job_sandbox + '/job.$(Cluster).$(Process).log'
        cjob['Output'] = job_sandbox + '/job.$(Cluster).$(Process).out'
        cjob['Error'] = job_sandbox +  '/job.$(Cluster).$(Process).err'
        cjob['should_transfer_files'] = 'YES'
        cjob['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
        cjob['transfer_output_files'] = 'promlet.log,promlet.json'
        cjob['+WantIOProxy'] = 'true'

        if groups is not None:
            cjob['+ProminenceGroup'] = condor_str(groups)

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
            output_files_new = []
            cjob['+ProminenceOutputFiles'] = condor_str(','.join(jjob['outputFiles']))

            output_locations_put = []

            for filename in jjob['outputFiles']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%s' % (uid, os.path.basename(filename)),
                                                    604800)
                output_locations_put.append(url_put)
                output_files_new.append({'name':filename, 'url':url_put})

            jjob_mapped['outputFiles'] = output_files_new

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
                        return (1, {"error":"Artifact %s does not exist" % artifact_original})
                input_files.append(artifact_url)
            cjob['+ProminenceArtifacts'] = condor_str(",".join(artifacts))

        cjob['transfer_input_files'] = str(','.join(input_files))

        if 'outputDirs' in jjob:
            output_dirs_new = []
            cjob['+ProminenceOutputDirs'] = condor_str(','.join(jjob['outputDirs']))

            output_locations_put = []

            for dirname in jjob['outputDirs']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%s.tgz' % (uid, os.path.basename(dirname)),
                                                    604800)
                output_locations_put.append(url_put)
                output_dirs_new.append({'name':dirname, 'url':url_put})

            jjob_mapped['outputDirs'] = output_dirs_new

        # Set max runtime
        max_run_time = 43200
        if 'walltime' in jjob['resources']:
            if jjob['resources']['walltime'] > -1:
                max_run_time = int(jjob['resources']['walltime'])*60
        cjob['periodic_hold'] = str('JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName)' % max_run_time)
        cjob['periodic_hold_subcode'] = str('ifThenElse(JobStatus == 2 && CurrentTime - EnteredCurrentStatus > %d && isUndefined(RouteName), 1001, 1000)' % max_run_time)
        cjob['+ProminenceMaxRunTime'] = str("%d" % (max_run_time/60))

        # Is job MPI?
        if 'tasks' in jjob:
            if 'type' in jjob['tasks'][0]:
                if jjob['tasks'][0]['type'] == 'openmpi':
                    cjob['+ProminenceWantMPI'] = 'true'
                elif jjob['tasks'][0]['type'] == 'mpich':
                    cjob['+ProminenceWantMPI'] = 'true'

        # Prepare for submission to a remote HPC system
        tasks = jjob['resources']['nodes']
        cpusPerTask = jjob['resources']['cpus']
        memoryPerCpu = jjob['resources']['memory']*1000
        cjob['+remote_cerequirements_default'] = condor_str("RequiredTasks == %d && RequiredMemoryPerCpu == %d && RequiredCpusPerTask == %d && RequiredTime == %d" % (tasks, memoryPerCpu, cpusPerTask, max_run_time))

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

        # Write mapped job.json
        with open(os.path.join(job_sandbox, '.job.mapped.json'), 'w') as file:
            json.dump(jjob_mapped, file)

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

    def create_workflow(self, username, groups, uid, jjob):
        """
        Create a workflow
        """
        # Firstly, create the job sandbox
        job_sandbox = self.create_sandbox(uid)
        if job_sandbox is None:
            return (1, {"error":"Unable to create job sandbox"})

        dag = []
        if 'jobs' in jjob:
            with open(job_sandbox + '/workflow.json', 'w') as fd:
                json.dump(jjob, fd)

            # Generate unique Swift temporary URLs for output/input files
            file_maps = {}
            for job in jjob['jobs']:
                if 'outputFiles' in job:
                    for filename in job['outputFiles']:
                        filename_base = os.path.basename(filename)
                        url_put = self.create_presigned_url('put', self._config['S3_BUCKET'], 'scratch/%s/%s' % (uid, filename_base), 604800)
                        url_get = self.create_presigned_url('get', self._config['S3_BUCKET'], 'scratch/%s/%s' % (uid, filename_base), 604800)
                        file_maps[filename_base] = (filename, url_put, url_get)

            # Check for storage specified in workflow, to be applied to all jobs
            storage_type = None
            storage_creds = None
            if 'storage' in jjob:
                if 'type' in jjob['storage']:
                    storage_type = jjob['storage']['type']
                    if jjob['storage']['type'] == 'b2drop':
                        storage_creds = '%s/%s' % (jjob['storage']['b2drop']['app-username'], jjob['storage']['b2drop']['app-password'])
                        storage_mountpoint = jjob['storage']['mountpoint']

            for job in jjob['jobs']:
                info = {}

                # If a job name is not defined, create one as we require all jobs to have a name
                if 'name' not in job:
                    job['name'] = str(uuid.uuid4())

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
                input_files = ['.job.mapped.json']

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
                            artifact = self.create_presigned_url('get', self._config['S3_BUCKET'], 'uploads/%s/%s' % (username, artifact), 604800)
                        input_files.append(artifact)
                    cjob['+ProminenceArtifacts'] = condor_str(",".join(artifacts))
                cjob['transfer_input_files'] = str(','.join(input_files))

                # Output files
                if 'outputFiles' in job:
                    cjob['+ProminenceOutputFiles'] = condor_str(','.join(job['outputFiles']))

                    output_locations_put = []

                    for filename in job['outputFiles']:
                        filename_base = os.path.basename(filename)
                        url_put = (file_maps[filename_base])[1]
                        output_locations_put.append(url_put)

                    if job['outputFiles']:
                        cjob['+ProminenceOutputLocations'] = condor_str(",".join(output_locations_put))

                contents_additional = "\n"
                for key in cjob:
                    contents_additional += "%s = %s\n" % (key, cjob[key])
                info['extras'] = contents_additional

                # Write JDL
                with open(job_filename, 'w') as fd:
                    fd.write(JOB_SUBMIT % info)
                dag.append('JOB ' + job['name'] + ' job.jdl DIR ' + job['name'])

                # Retries
                if 'numberOfRetries' in jjob:
                    dag.append('RETRY ALL_NODES %d' % jjob['numberOfRetries'])

                # Write .job.json
                filename = job_sandbox + '/' + job['name'] + '/.job.json'
                with open(filename, 'w') as file:
                    json.dump(job, file)

                filename = job_sandbox + '/' + job['name'] + '/.job.mapped.json'
                with open(filename, 'w') as file:
                    json.dump(job, file)

                # Copy executable
                copyfile(self._config['PROMLET_FILE'], os.path.join(job_sandbox, job['name'], 'promlet.py'))
                os.chmod(job_sandbox + '/' + job['name'] + '/promlet.py', 0775)

        # Create dag
        if 'dependencies' in jjob:
            for parent in jjob['dependencies']:
                children = " ".join(jjob['dependencies'][parent])
                dag.append('PARENT ' + parent + ' CHILD ' + children)
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
        print 'condor_submit stdout=', stdout 
        print 'condor_submit stderr=', stderr
        m = re.search(r'submitted to cluster\s(\d+)', stdout)
        if m:
            retval = 201
            data['id'] = int(m.group(1))
        else:
            retval = 1
            data = {"error":"Job submission failed"}

        return (retval, data)

    def delete_job(self, username, job_ids):
        """
        Delete the specified job(s)
        """
        constraints = []
        for job_id in job_ids:
            constraints.append('ClusterId == %d' % int(job_id))
        constraint = '(%s) && ProminenceIdentity == "%s" && Cmd != "/bin/condor_dagman"' % (' || '.join(constraints), username)

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
        constraint = '(%s) && ProminenceIdentity == "%s" && Cmd == "/usr/bin/condor_dagman"' % (' || '.join(constraints), username)

        schedd = htcondor.Schedd()
        ret = schedd.act(htcondor.JobAction.Remove, constraint)

        if ret["TotalSuccess"] > 0:
            return (0, {})
        return (1, {"error":"No such workflow(s)"})

    def list_jobs(self, job_ids, identity, active, completed, num, detail, constraint):
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
                          'QDate',
                          'JobStartDate',
                          'JobRunCount',
                          'JobCurrentStartExecutingDate',
                          'CompletionDate',
                          'EnteredCurrentStatus',
                          'RemoveReason',
                          'RemoteWallClockTime',
                          'LastHoldReasonSubCode',
                          'ProminenceOutputFiles',
                          'ProminenceOutputDirs',
                          'ProminenceUserEnvironment',
                          'ProminenceUserMetadata',
                          'TransferInput',
                          'ProminenceJobUniqueIdentifier',
                          'ProminenceName',
                          'ProminenceExitCode',
                          'ProminencePreemptible',
                          'ProminenceImagePullSuccess',
                          'ProminenceStorageType',
                          'ProminenceStorageCredentials',
                          'ProminenceStorageMountPoint',
                          'Iwd']
        jobs_state_map = {1:'created',
                          2:'running',
                          3:'failed',
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
        if len(job_ids) > 0:
            constraints = []
            for job_id in job_ids:
                constraints.append('ClusterId == %d' % int(job_id))
            constraintc = '(%s) && %s' % (' || '.join(constraints), constraintc)
            num = len(job_ids)

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
            jobj['name'] = ''
            if 'name' in job_json_file:
                jobj['name'] = job_json_file['name']

            # If job is idle and infrastructure is ready, set status to 'ready'
            if 'ProminenceInfrastructureState' in job:
                if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'configured':
                    jobj['status'] = 'idle'
                if job['JobStatus'] == 1 and (job['ProminenceInfrastructureState'] == 'deployment-init' or job['ProminenceInfrastructureState'] == 'creating'):
                    jobj['status'] = 'deploying'

            # Get promlet output if exists
            tasks_u = []
            try:
                with open(job['Iwd'] + '/promlet.json') as json_file:
                    tasks_u = json.load(json_file)
            except:
                pass
            
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
                if 'LastHoldReasonSubCode' in job:
                    if job['LastHoldReasonSubCode'] == 1001:
                        reason = 'Runtime limit exceeded'
                        jobj['status'] = 'killed'
                jobj['statusReason'] = reason

            if 'ProminencePreemptible' in job:
                jobj['preemptible'] = True

            events = {}
            events['createTime'] = int(job['QDate'])

            if 'JobStartDate' in job and int(job['JobStartDate']) > 0:
                events['startTime'] = int(job['JobStartDate'])

            # For remote jobs on remote HTC/HPC, JobStartDate doesn't exist
            if 'JobStartDate' not in job and job['JobStatus'] == 2:
                events['startTime'] = int(job['EnteredCurrentStatus'])

            if 'JobStartDate' not in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
                if int(job['RemoteWallClockTime']) > 0 and int(job['CompletionDate']) > 0:
                    events['startTime'] = int(job['CompletionDate']) - int(job['RemoteWallClockTime'])

            # Get the job end date if needed. Note that if a job was removed CompletionDate is 0,
            # so we use EnteredCurrentStatus instead
            if 'CompletionDate' in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
                if int(job['CompletionDate']) > 0:
                    events['endTime'] = int(job['CompletionDate'])
                elif int(job['CompletionDate']) == 0 and int(job['EnteredCurrentStatus']) > 0 and 'JobStartDate' in job:
                    events['endTime'] = int(job['EnteredCurrentStatus'])

            # Set end time for a job which was evicted
            if 'LastJobStatus' in job:
                if job['LastJobStatus'] == 2 and job['JobStatus'] == 1:
                    events['endTime'] = int(job['EnteredCurrentStatus'])

            if detail > 0:
                jobj['resources'] = job_json_file['resources']

                if 'numberOfRetries' in job_json_file:
                    jobj['numberOfRetries'] = job_json_file['numberOfRetries']

                if 'artifacts' in job_json_file:
                    jobj['artifacts'] = job_json_file['artifacts']

                if 'inputs' in job_json_file:
                    jobj['inputs'] = job_json_file['inputs']

                if 'labels' in job_json_file:
                    jobj['labels'] = job_json_file['labels']

                if 'constraints' in job_json_file:
                    jobj['constraints'] = job_json_file['constraints']

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
                    new_tasks_u = []
                    if tasks_u:
                        for task_u in tasks_u:
                            if 'maxMemoryUsageKB' in task_u:
                                execution['maxMemoryUsageKB'] = task_u['maxMemoryUsageKB']
                            else:
                                new_tasks_u.append(task_u)
                        execution['tasks'] = new_tasks_u
                    jobj['execution'] = execution

                if 'ProminenceJobUniqueIdentifier' in job:
                    uid = str(job['ProminenceJobUniqueIdentifier'])

                if 'ProminenceOutputFiles' in job:
                    output_files = str(job['ProminenceOutputFiles']).split(',')
                    outputs = []
                    for output_file in output_files:
                        filename = os.path.basename(output_file)
                        if job['JobStatus'] == 4:
                            url = self.create_presigned_url('get', self._config['S3_BUCKET'], 'scratch/%s/%s' % (uid, filename), 600)
                        else:
                            url = ''
                        file_map = {'name':output_file, 'url':url}
                        outputs.append(file_map)
                    jobj['outputFiles'] = outputs
                if 'ProminenceOutputDirs' in job:
                    output_dirs = str(job['ProminenceOutputDirs']).split(',')
                    outputs = []
                    for output_dir in output_dirs:
                        dirs = output_dir.split('/')
                        dirname_base = dirs[len(dirs) - 1]
                        if job['JobStatus'] == 4:
                            url = self.create_presigned_url('get', self._config['S3_BUCKET'], 'scratch/%s/%s.tgz' % (uid, dirname_base), 600)
                        else:
                            url = ''
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
                          'QDate',
                          'Cmd',
                          'Iwd'
                          ]
        jobs_state_map = {1:'created',
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

            # If no jobs have been created, report status as created
            if nodes_queued == 0 and wfj['status'] != 'completed' and wfj['status'] != 'failed':
                wfj['status'] = 'created'

            nodes['total'] = nodes_total
            nodes['done'] = nodes_done
            nodes['failed'] = nodes_failed
            nodes['queued'] = nodes_queued
            nodes['waiting'] = nodes_unready

            # Completed workflows with failed jobs should be reported as failed, not completed
            if wfj['status'] == 'completed' and nodes_failed > 0:
                wfj['status'] = 'failed'

            wfj['progress'] = nodes

            wfs.append(wfj)

        return wfs

    def get_stdout(self, uid, job_id, job_name=None):
        """
        Return the stdout from the specified job
        """
        if job_name is None:
            filename = self._config['SANDBOX_PATH'] + '/%s/job.%d.0.out' % (uid, job_id)
        else:
            filename = self._config['SANDBOX_PATH'] + '/%s/%s/job.%s.0.out' % (uid, job_name, job_name)
        if os.path.isfile(filename):
            with open(filename) as fd:
                return fd.read()
        return None

    def get_stderr(self, uid, job_id, job_name=None):
        """
        Return the stdout from the specified job
        """
        if job_name is None:
            filename = self._config['SANDBOX_PATH'] + '/%s/job.%d.0.err' % (uid, job_id)
        else:
            filename = self._config['SANDBOX_PATH'] + '/%s/%s/job.%s.0.err' % (uid, job_name, job_name)
        if os.path.isfile(filename):
            with open(filename) as fd:
                return fd.read()
        return None



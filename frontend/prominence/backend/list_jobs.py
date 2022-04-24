import glob
import json
import math
import os
import re

import classad
import htcondor

from .utilities import redact_storage_creds

def convert_to_number(value):
    """
    Convert a string to a float or int if possible
    """
    try:
        output = float(value)
    except:
        return value

    if int(output) == output:
        return int(output)

    return output

def list_jobs(self, job_ids, identity, active, completed, status, workflow, num, detail, constraint, name_constraint):
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
                      'ProminenceInfrastructureStateReason',
                      'ProminenceInfrastructureType',
                      'QDate',
                      'GridJobStatus',
                      'JobCurrentStartDate',
                      'JobRunCount',
                      'JobCurrentStartExecutingDate',
                      'CompletionDate',
                      'EnteredCurrentStatus',
                      'LastVacateTime',
                      'JobFinishedHookDone',
                      'RemoveReason',
                      'HoldReason',
                      'LastHoldReason',
                      'RemoteWallClockTime',
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
                      'Args',
                      'CpusProvisioned',
                      'MemoryProvisioned',
                      'DiskProvisioned',
                      'AllRemoteHosts',
                      'MachineAttrProminenceCloud0']
    jobs_state_map = {1:'idle',
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
    if len(job_ids) > 0 and not workflow:
        constraints = []
        for job_id in job_ids:
            constraints.append('ClusterId == %d' % int(job_id))
        constraintc = '(%s) && %s' % (' || '.join(constraints), constraintc)
        num = len(job_ids)

    if workflow and len(job_ids) > 0:
        constraintc = '(DAGManJobId == %d) && %s' % (int(job_ids[0]), constraintc)

    if name_constraint is not None:
        constraintc = 'ProminenceName =?= "%s" && %s' % (str(name_constraint), constraintc)

    # Get completed jobs if necessary
    if completed:
        jobs_completed = schedd.history('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs, int(num))
        jobs_condor.extend(jobs_completed)

    # Get active jobs if necessary
    if active:
        jobs_active = schedd.xquery('RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % constraintc, required_attrs)
        jobs_condor.extend(jobs_active)

    # Get only jobs in specific state
    if status == 'idle' or status == 'running':
        if status == 'idle':
            job_status = 1
        elif status == 'running':
            job_status = 2
        jobs_condor = schedd.xquery('JobStatus == %d && RoutedBy =?= undefined && ProminenceType == "job" && ProminenceName =!= undefined && %s' % (job_status, constraintc), required_attrs)

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

        # Use provided storage if necessary
        use_default_object_storage = True
        if 'storage' in job_json_file:
            if 'default' in job_json_file['storage']:
                if job_json_file['storage']['default']:
                    use_default_object_storage = False

        # Job name - for jobs from workflows, use the name "<workflow name>/<job name>/(<number>)"
        jobj['name'] = ''
        if 'name' in job_json_file:
            jobj['name'] = job_json_file['name']
            if 'ProminenceWorkflowName' in job:
                jobj['name'] = '%s/%s' % (job['ProminenceWorkflowName'], jobj['name'])
                if 'ProminenceFactoryId' in job:
                    jobj['name'] = '%s/%s' % (jobj['name'], job['ProminenceFactoryId'])

        # Get the status reason if possible
        status_reason = None
        if 'ProminenceInfrastructureStateReason' in job:
            status_reason = job['ProminenceInfrastructureStateReason']

        # Set job status as appropriate
        if 'ProminenceInfrastructureState' in job:
            if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'configured':
                jobj['status'] = 'deploying'
            if job['JobStatus'] == 1 and (job['ProminenceInfrastructureState'] == 'deployment-init' or job['ProminenceInfrastructureState'] == 'creating'):
                jobj['status'] = 'deploying'
            if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'unable':
                jobj['status'] = 'waiting'
                if status_reason == 'NoMatchingResources':
                    jobj['status'] = 'failed'
                    jobj['statusReason'] = 'No matching resources'
                elif status_reason == 'NoMatchingResourcesAvailable':
                    jobj['statusReason'] = 'No matching resources currently available'
                else:
                    jobj['statusReason'] = ''
            if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'failed':
                jobj['statusReason'] = 'Deployment failed'

            if job['JobStatus'] == 1 and job['ProminenceInfrastructureState'] == 'waiting':
                jobj['status'] = 'waiting'
                if status_reason == 'NoMatchingResourcesAvailable':
                    jobj['statusReason'] = 'No matching resources currently available'
                else:
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

        # Handle new jobs where promlet JSON is in a directory named json
        multiple_nodes = False
        if not os.path.isfile(promlet_json_filename) and os.path.exists('%s/json' % job['Iwd']):
            promlet_json_filename = '%s/json/promlet.0.json' % job['Iwd']
            if 'ProminenceFactoryId' in job:
                promlet_json_filename = '%s/json/promlet.%d.json' % (job['Iwd'], int(job['ProminenceFactoryId']))

            if not os.path.isfile(promlet_json_filename):
                # For multi-node jobs
                promlet_json_filename = '%s/json/promlet.0-0.json' % job['Iwd']
                if 'ProminenceFactoryId' in job:
                    promlet_json_filename = '%s/json/promlet.%d-0.json' % (job['Iwd'], int(job['ProminenceFactoryId']))
                multiple_nodes = True

        # Read in promlet.json
        job_u = {}
        try:
            with open(promlet_json_filename) as promlet_json_file:
                job_u = json.load(promlet_json_file)
        except:
            pass

        # For multi-node jobs we read in all promlet.json files
        job_u_m = []
        if multiple_nodes:
            files = glob.glob('%s/json/promlet.0-*.json' % job['Iwd'])
            if 'ProminenceFactoryId' in job:
                files = glob.glob('%s/json/promlet.%d-*.json' % (job['Iwd'], int(job['ProminenceFactoryId'])))

            for jfile in files:
                try:
                    with open(jfile) as fh:
                        job_u_m.append(json.load(fh))
                except:
                    pass

        tasks_u = []
        if 'tasks' in job_u:
            tasks_u = job_u['tasks']

        stageout_u = {}
        if 'stageout' in job_u:
            stageout_u = job_u['stageout']
  
        stagein_u = {}
        if 'stagein' in job_u:
            stagein_u = job_u['stagein']

        # Job parameters
        parameters = {}
        if 'ProminenceFactoryId' in job:
            matches = re.findall('--param ([\w]+)=([\w\.\/]+)', job['Args'])
            if matches:
                for match in matches:
                    parameters[match[0]] = convert_to_number(match[1])
            jobj['parameters'] = parameters

        # For jobs which are not idle or running, check for various failed states in the
        # promlet json file
        if job['JobStatus'] != 1 and job['JobStatus'] != 2:

            # Return status as failed if any fuse mounts failed
            if 'mounts' in job_u:
                for mount in job_u['mounts']:
                    if 'status' in mount:
                        if mount['status'] == 'failed':
                            jobj['status'] = 'failed'
                            jobj['statusReason'] = 'Unable to mount storage volume'

            # Return status as failed if artifact download failed
            for item in stagein_u:
                if 'status' in item:
                    if item['status'] == 'failedDownload':
                        jobj['status'] = 'failed'
                        jobj['statusReason'] = 'Artifact download failed'
                    if item['status'] == 'failedUncompress':
                        jobj['status'] = 'failed'
                        jobj['statusReason'] = 'Artifact uncompress failed'

            # Return status as failed if stageout failed
            if 'files' in stageout_u and 'directories' in stageout_u:
                for item in stageout_u['files'] + stageout_u['directories']:
                    if 'status' in item:
                        if item['status'] == 'failedNoSuchFile':
                            jobj['status'] = 'failed'
                            jobj['statusReason'] = 'Stageout failed due to no such file or directory'
                        if item['status'] == 'failedUpload':
                            jobj['status'] = 'failed'
                            jobj['statusReason'] = 'Unable to stageout output to object storage'
                        if item['status'] == 'failedTarCreation':
                            jobj['status'] = 'failed'
                            jobj['statusReason'] = 'Stageout failed due to tarball creation failed'

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

            if 'JobRunCount' in job:
                if job['JobStatus'] == 1 and job['JobRunCount'] > 0:
                    jobj['status'] = 'failed'
                    jobj['statusReason'] = ''

            # Return status as killed if walltime limit execeed
            if tasks_u:
                for task_u in tasks_u:
                    if 'error' in task_u:
                        jobj['status'] = 'killed'
                        jobj['statusReason'] = 'Walltime limit exceeded'

        if job['JobStatus'] == 3:
            reason = ''
            if 'ProminenceInfrastructureState' in job:
                if job['ProminenceInfrastructureState'] == "failed":
                    reason = 'Infrastructure deployment failed'
                    jobj['status'] = 'failed'
                if job['ProminenceInfrastructureState'] == "unable":
                    if status_reason == 'NoMatchingResources':
                        reason = 'No matching resources'
                    elif status_reason == 'NoMatchingResourcesAvailable':
                        reason = 'No matching resources currently available'
                    else:
                        reason = 'Unable to provision resources'
                    jobj['status'] = 'failed'

            if 'RemoveReason' in job:
                if 'Python-initiated action' in job['RemoveReason']:
                    reason = 'Job deleted by user'
                    jobj['status'] = 'deleted'
                if 'Infrastructure took too long to be deployed' in job['RemoveReason']:
                    reason = 'Infrastructure took too long to be deployed'
                if 'OtherJobRemoveRequirements = DAGManJobId' in job['RemoveReason'] and 'was removed' in job['RemoveReason']:
                    reason = 'Job part of a workflow which was deleted by user'
                    jobj['status'] = 'deleted'
                if job['RemoveReason'] == 'NoMatchingResourcesAvailable':
                    reason = 'No matching resources currently available'
                    jobj['status'] = 'failed'
                if job['RemoveReason'] == 'NoMatchingResources':
                    reason = 'No matching resources'
                    jobj['status'] = 'failed'

            if 'HoldReason' in job:
                if 'Infrastructure took too long to be deployed' in job['HoldReason']:
                    reason = 'Infrastructure took too long to be deployed'
                    jobj['status'] = 'failed'
                if 'Job took too long to start running' in job['HoldReason']:
                    reason = 'Job took too long to start running after deployment'
                    jobj['status'] = 'failed'
                if 'Job was evicted' in job['HoldReason']:
                    reason = 'Job was evicted'
                    jobj['status'] = 'failed'
                if job['HoldReason'] == 'NoMatchingResourcesAvailable':
                    reason = 'No matching resources currently available'
                    jobj['status'] = 'failed'
                if job['HoldReason'] == 'NoMatchingResources':
                    reason = 'No matching resources'
                    jobj['status'] = 'failed'
                if job['HoldReason'] == 'Job was queued for too long':
                    reason = 'Maximum time queued was exceeded'
                    jobj['status'] = 'failed'
                if 'Job has gone over memory limit' in job['HoldReason'] or 'Job has encountered an out-of-memory event' in job['HoldReason']:
                    reason = 'Job used too much memory'
                    jobj['status'] = 'killed'

            # Handle job using too much memory
            if 'LastHoldReason' in job:
                if 'Job has gone over memory limit' in job['LastHoldReason'] or 'Job has encountered an out-of-memory event' in job['LastHoldReason']:
                    jobj['status'] = 'killed'
                    reason = 'Job used too much memory'

            jobj['statusReason'] = reason

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

        # Under some situations a completed job will have a CompletionDate of 0 and EnteredCurrentStatus will be the
        # time the job started running, so check if we can use LastVacateTime
        if 'LastVacateTime' in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
            if int(job['LastVacateTime']) > 0:
                if 'endTime' in events:
                    if int(job['LastVacateTime']) > events['endTime']:
                        events['endTime'] = int(job['LastVacateTime'])
                else:
                    events['endTime'] = int(job['LastVacateTime'])

        # Also try JobFinishedHookDone
        if 'JobFinishedHookDone' in job and (job['JobStatus'] == 3 or job['JobStatus'] == 4):
            if int(job['JobFinishedHookDone']) > 0:
                if 'endTime' in events:
                    if int(job['JobFinishedHookDone']) > events['endTime'] and events['endTime'] == 0:
                        events['endTime'] = int(job['JobFinishedHookDone'])
                else:
                    events['endTime'] = int(job['JobFinishedHookDone'])

        # Return a single pending state instead of idle, deploying & waiting
        if 'USE_PENDING_STATE' in self._config:
            if jobj['status'] in ('idle', 'waiting'):
                jobj['status'] = 'pending'
            elif jobj['status'] == 'deploying':
                jobj['status'] = 'pending' 
                jobj['statusReason'] = 'Creating infrastructure to run job'

        if detail > 0:
            jobj['resources'] = job_json_file['resources']

            if 'policies' in job_json_file:
                jobj['policies'] = job_json_file['policies']

            if 'notifications' in job_json_file:
                jobj['notifications'] = job_json_file['notifications']

            if 'artifacts' in job_json_file:
                jobj['artifacts'] = job_json_file['artifacts']

            if 'inputs' in job_json_file:
                jobj['inputs'] = job_json_file['inputs']

            if 'labels' in job_json_file:
                jobj['labels'] = job_json_file['labels']

            if 'constraints' in job_json_file:
                jobj['constraints'] = job_json_file['constraints']

            if 'storage' in job_json_file:
                jobj['storage'] = redact_storage_creds(job_json_file['storage'])

            execution = {}

            stagein_total = 0
            for item in stagein_u:
                if 'time' in item:
                    stagein_total = stagein_total + item['time']
            if stagein_total:
                execution['stageInTime'] = stagein_total

            stageout_total = 0
            if 'files' in stageout_u:
                for item in stageout_u['files']:
                    if 'time' in item:
                        stageout_total = stageout_total + item['time']

            if 'directories' in stageout_u:
                for item in stageout_u['directories']:
                    if 'time' in item:
                        stageout_total = stageout_total + item['time']

            if stageout_total > 0:
                execution['stageOutTime'] = stageout_total

            if 'ProminenceInfrastructureSite' in job:
                if job['ProminenceInfrastructureSite'] != 'none':
                    execution['site'] = str(job['ProminenceInfrastructureSite'])
            elif 'site' in job_u:
                execution['site'] = job_u['site']
            elif 'MachineAttrProminenceCloud0' in job:
                if job['MachineAttrProminenceCloud0']:
                    execution['site'] = job['MachineAttrProminenceCloud0']

            if 'cpu_vendor' in job_u and 'cpu_model' in job_u and 'cpu_clock' in job_u:
                execution['cpu'] = {'vendor': job_u['cpu_vendor'],
                                    'model': job_u['cpu_model'],
                                    'clock': job_u['cpu_clock']}

            nodes_provisioned = 1
            if 'AllRemoteHosts' in job:
                if job['AllRemoteHosts']:
                    nodes_provisioned = job['AllRemoteHosts'].count(',') + 1

            if 'CpusProvisioned' in job and 'MemoryProvisioned' in job and 'DiskProvisioned' in job:
                execution['provisionedResources'] = {'cpus': int(job['CpusProvisioned']),
                                                     'memory': int(job['MemoryProvisioned']/1024.0),
                                                     'disk': math.ceil(job['DiskProvisioned']/1000.0/1000.0),
                                                     'nodes': nodes_provisioned}
            elif 'memory' in job_u and 'cpus' in job_u and 'disk' in job_u:
                if job_u['cpus'] and job_u['memory'] and job_u['disk']:
                    execution['provisionedResources'] = {'cpus': job_u['cpus'],
                                                         'memory': job_u['memory'],
                                                         'disk': job_u['disk'],
                                                         'nodes': nodes_provisioned}

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

            if execution:
                jobj['execution'] = execution

            if 'ProminenceJobUniqueIdentifier' in job:
                uid = str(job['ProminenceJobUniqueIdentifier'])

            fid = uid
            if 'ProminenceFactoryId' in job:
                uid = job['Iwd'].split('/')[len(job['Iwd'].split('/'))-2]
                job_name = job['ProminenceName']
                fid = '%s/%s/%s' % (uid, job_name, job['ProminenceFactoryId'])
            elif 'ProminenceName' in job and 'DAGManJobId' in job:
                uid = job['Iwd'].split('/')[len(job['Iwd'].split('/'))-2]
                job_name = job['ProminenceName']
                fid = '%s/%s' % (uid, job_name)

            if 'outputFiles' in job_json_file:
                outputs = []
                for output_file in job_json_file['outputFiles']:
                    filename = os.path.basename(output_file)
                    stageout_success = False
                    url = ''
                    size = None
                    if 'files' in stageout_u:
                        for file in stageout_u['files']:
                            if 'status' not in file:
                                continue
                            if file['name'] == output_file and file['status'] == 'success' and use_default_object_storage:
                                url = self.create_presigned_url('get',
                                                                self._config['S3_BUCKET'],
                                                                'scratch/%s/%s' % (fid, filename),
                                                                7*24*60*60)
                                size = self.get_object_size(self._config['S3_BUCKET'],
                                                            'scratch/%s/%s' % (fid, filename))

                    file_map = {'name':output_file, 'url':url}
                    if size:
                        file_map['size'] = size
                    outputs.append(file_map)
                jobj['outputFiles'] = outputs

            if 'outputDirs' in job_json_file:
                outputs = []
                for output_dir in job_json_file['outputDirs']:
                    dirs = output_dir.split('/')
                    dirname_base = dirs[len(dirs) - 1]
                    stageout_success = False
                    url = ''
                    size = None
                    if 'directories' in stageout_u:
                        for directory in stageout_u['directories']:
                            if 'status' not in directory:
                                continue
                            if directory['name'] == output_dir and directory['status'] == 'success' and use_default_object_storage:
                                url = self.create_presigned_url('get',
                                                                self._config['S3_BUCKET'],
                                                                'scratch/%s/%s.tgz' % (fid, dirname_base),
                                                                7*24*60*60)
                                size = self.get_object_size(self._config['S3_BUCKET'],
                                                            'scratch/%s/%s.tgz' % (fid, dirname_base))
                    file_map = {'name':output_dir, 'url':url}
                    if size:
                        file_map['size'] = size
                    outputs.append(file_map)
                jobj['outputDirs'] = outputs

        jobj['events'] = events

        jobs.append(jobj)

    return jobs

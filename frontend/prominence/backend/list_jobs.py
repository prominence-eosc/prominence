import json
import os
import re

import classad
import htcondor

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

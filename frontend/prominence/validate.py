""" Functions for validating jobs and workflows"""
import re

def validate_notification(notification, valid_events):
    """
    Validate a notification
    """
    for item in notification:
        if item not in ('event', 'type'):
            return (False, 'invalid item "%s" in notifications' % item)
        if item == 'event':
            if notification[item] not in valid_events:
                return (False, 'invalid notification event "%s"' % notification[item])
        if item == 'type':
            if notification[item] != 'email':
                return (False, 'invalid notification type "%s"' % notification[item])

    return (True, '')

def validate_placement(placement):
    """
    Validate placement policy
    """
    for item in placement:
        if item not in ['requirements', 'preferences']:
            return (False, 'invalid item "%s" in placement policy' % item)

    if 'requirements' in placement:
        for item in placement['requirements']:
            if item not in ['sites', 'regions']:
                return (False, 'invalid item "%s" in requirements' % item)
            if not isinstance(placement['requirements'][item], list):
                return (False, '%s in requirements must be a list' % item)

    if 'preferences' in placement:
        for item in placement['preferences']:
            if item not in ['sites', 'regions']:
                return (False, 'invalid item "%s" in preferences' % item)
            if not isinstance(placement['preferences'][item], list):
                return (False, '%s in preferences must be a list' % item)

    return (True, '')

def validate_workflow(workflow):
    """
    Validate JSON workflow description
    """
    workflow_valids = ['name',
                       'labels',
                       'jobs',
                       'dependencies',
                       'factory',
                       'policies',
                       'notifications']

    policies_workflow_valids = ['maximumRetries',
                                'placement']

    valid_events = ['workflowFinished']

    # Check for valid items in workflow
    for item in workflow:
        if item not in workflow_valids:
            return (False, 'invalid item "%s" in workflow description' % item)

    # Name
    if 'name' in workflow:
        if len(workflow['name']) > 512:
            return (False, 'workflow name must be less than 512 characters in length')

        if workflow['name'] != '' and not re.match(r'^[a-zA-Z0-9\-\_\s\.]+$', workflow['name']):
            return (False, 'invalid workflow name')

    # Jobs
    if 'jobs' in workflow:
        for job in workflow['jobs']:
            (status, msg) = validate_job(job)
            if not status:
                return (status, msg)
    else:
        return (False, 'a workflow must contain jobs')

    # Allow only depdencies or a factory
    if 'dependencies' in workflow and 'factory' in workflow:
        return (False, 'a workflow cannot include both dependencies and a factory')

    # Dependencies
    if 'dependencies' in workflow:
        if not isinstance(workflow['dependencies'], dict):
            return (False, 'dependencies must be a dict')

        jobs = []
        for job in workflow['jobs']:
            if 'name' in job:
                if job['name'] in jobs:
                    return (False, 'all jobs must have unique names: "%s" is used more than once' % job['name'])
                jobs.append(job['name'])
                if job['name'] == '':
                    return (False, 'names of jobs in workflows cannot be empty')
            else:
                return (False, 'all jobs must have names')

        for dependency in workflow['dependencies']:
            children = workflow['dependencies'][dependency]
            if not isinstance(children, list):
                return (False, 'children of parent job must be in the form of a list')
            for child in children:
                if child not in jobs:
                    return (False, 'child job "%s" is not actually defined' % child)

    # Factories
    if 'factory' in workflow:
        valid_factories = ['parametricSweep', 'zip']

        if 'type' not in workflow['factory']:
            return (False, 'a factory type must be specified')

        if workflow['factory']['type'] not in valid_factories:
            return (False, 'invalid factory type')

        if workflow['factory']['type'] == 'parametricSweep':
            if 'parameters' not in workflow['factory']:
                return (False, 'a factory of type parametricSweep must have parameters specified')

            if not isinstance(workflow['factory']['parameters'], list):
                return (False, 'parameters must be a list')

            names = []
            for parameter in workflow['factory']['parameters']:
                if 'name' not in parameter:
                    return (False, 'a parameter must contain a name')
                if 'start' not in parameter:
                    return (False, 'a parameter must contain a start value')
                if 'end' not in parameter:
                    return (False, 'a parameter must contain an end value')
                if 'step' not in parameter and 'number' not in parameter:
                    return (False, 'a parameter must contain a step size or number of steps')
                if 'step' in parameter and 'number' in parameter:
                    return (False, 'a parameter cannot have both a step size and number of steps')

                if parameter['name'] not in names:
                    names.append(parameter['name'])
                else:
                    return (False, 'each parameter must have a unique name')

        elif workflow['factory']['type'] == 'zip':
            if 'parameters' not in workflow['factory']:
                return (False, 'a factory of type zip must have parameters specified')

            if not isinstance(workflow['factory']['parameters'], list):
                return (False, 'parameters must be a list')

            names = []
            previous_length = -1
            for parameter in workflow['factory']['parameters']:
                current_length = len(parameter['values'])
                if current_length != previous_length and previous_length != -1:
                    return (False, 'all parameters must lists of values the same length')
                previous_length = current_length

                if 'name' not in parameter:
                    return (False, 'a parameter must contain a name')
                if 'values' not in parameter:
                    return (False, 'a parameter must contain a list of values')

                if not isinstance(parameter['values'], list):
                    return (False, 'values must be a list')

                if parameter['name'] not in names:
                    names.append(parameter['name'])
                else:
                    return (False, 'each parameter must have a unique name')

    # Polices
    if 'policies' in workflow:
        for item in workflow['policies']:
            if item not in policies_workflow_valids:
                return (False, 'invalid item "%s" in policies' % item)

        if 'maximumRetries' in workflow['policies']:
            if not str(workflow['policies']['maximumRetries']).isdigit():
                return (False, 'the number of retries must be an integer')

            if workflow['policies']['maximumRetries'] < 1:
                return (False, 'the number of retries must be greater than 0')

            if workflow['policies']['maximumRetries'] > 6:
                return (False, 'the number of retries must be less than 6')

    # Retries
    if 'maximumRetries' in workflow:
        if not str(workflow['maximumRetries']).isdigit():
            return (False, 'the number of retries must be an integer')

        if workflow['maximumRetries'] < 1:
            return (False, 'the number of retries must be greater than 0')

        if workflow['maximumRetries'] > 6:
            return (False, 'the number of retries must be less than 6')

    # Labels
    if 'labels' in workflow:
        if not isinstance(workflow['labels'], dict):
            return (False, 'labels must be defined as a dict')
        for label in workflow['labels']:
            if len(label) > 512:
                return (False, 'label names must be less than 512 characters in length')
            if len(workflow['labels'][label]) > 512:
                return (False, 'label values must be less than 512 characters in length')

            if not re.match(r'^[a-zA-Z0-9]+$', label):
                return (False, 'label name "%s" is invalid' % label)
            if not re.match(r'^[\w\-\_\.\/]+$', workflow['labels'][label]):
                return (False, 'label value "%s" is invalid' % workflow['labels'][label])

    # Notifications
    if 'notifications' in workflow:
        if not isinstance(workflow['notifications'], list):
            return (False, 'notifications must be in the form of a list')
        for notification in workflow['notifications']:
            (status, msg) = validate_notification(notification, valid_events)
            if not status:
                return (status, msg)

    return (True, '')

def validate_job(job):
    """
    Validate JSON job description
    """
    job_valids = ['name',
                  'labels',
                  'tasks',
                  'resources',
                  'policies',
                  'notifications',
                  'inputs',
                  'artifacts',
                  'outputFiles',
                  'outputDirs',
                  'storage']

    task_valids = ['image',
                   'cmd',
                   'env',
                   'workdir',
                   'procsPerNode',
                   'type',
                   'runtime',
                   'imagePullCredential']

    resources_valids = ['nodes',
                        'cpus',
                        'memory',
                        'disk',
                        'walltime']

    policies_valids = ['maximumRetries',
                       'preemptible',
                       'maximumTimeInQueue',
                       'maximumIdleTimePerResource',
                       'leaveInQueue',
                       'placement']

    valid_events = ['jobFinished']

    # Check for valid items in job
    for item in job:
        if item not in job_valids:
            return (False, 'invalid item "%s" in job description' % item)

    # Name
    if 'name' in job:
        if len(job['name']) > 512:
            return (False, 'job name must be less than 512 characters in length')

        if job['name'] != '' and not re.match(r'^[a-zA-Z0-9\-\_\s\.]+$', job['name']):
            return (False, 'invalid job name')

    # Labels
    if 'labels' in job:
        if not isinstance(job['labels'], dict):
            return (False, 'labels must be defined as a dict')
        for label in job['labels']:
            if len(label) > 512:
                return (False, 'label names must be less than 512 characters in length')
            if len(job['labels'][label]) > 512:
                return (False, 'label values must be less than 512 characters in length')

            if not re.match(r'^[a-zA-Z0-9]+$', label):
                return (False, 'label name "%s" is invalid' % label)
            if not re.match(r'^[\w\-\_\.\/]+$', job['labels'][label]):
                return (False, 'label value "%s" is invalid' % job['labels'][label])

    # Resources
    if 'resources' in job:
        for item in job['resources']:
            if item not in resources_valids:
                return (False, 'invalid item "%s" in resources' % item)

        if 'nodes' in job['resources']:
            if not str(job['resources']['nodes']).isdigit():
                return (False, 'number of nodes must be an integer')
            if job['resources']['nodes'] < 1:
                return (False, 'number of nodes must be at least 1')

        if 'cpus' in job['resources']:
            if not str(job['resources']['cpus']).isdigit():
                return (False, 'number of cpus must be an integer')
            if job['resources']['cpus'] < 1:
                return (False, 'number of cpus must be at least 1')
        else:
            return (False, 'number of cpus must be defined')

        if 'memory' in job['resources']:
            if not str(job['resources']['memory']).isdigit():
                return (False, 'memory must be an integer')
            if job['resources']['memory'] < 1:
                return (False, 'memory must be at least 1')
        else:
            return (False, 'memory (in GB) must be defined')

        if 'disk' in job['resources']:
            if not str(job['resources']['disk']).isdigit():
                return (False, 'required disk must be an integer')
            if job['resources']['disk'] < 1:
                return (False, 'disk must be at least 1')

        if 'walltime' in job['resources']:
            if not str(job['resources']['walltime']).isdigit():
                return (False, 'required walltime must be an integer')
            if job['resources']['walltime'] < 1:
                return (False, 'walltime must be at least 1 minute')
    else:
        return (False, 'a job must contain resources')

    # Tasks
    if 'tasks' in job:
        if not isinstance(job['tasks'], list):
            return (False, 'an array of tasks must be provided')

        for task in job['tasks']:
            for item in task:
                if item not in task_valids:
                    return (False, 'invalid item "%s" in task' % item)

            if 'image' not in task:
                return (False, 'each task must specify a container image')

            if 'imagePullCredential' in task:
                if 'username' not in task['imagePullCredential'] or 'token' not in task['imagePullCredential']:
                    return (False, 'a username and token must be specified in the image pull credential')

            if 'runtime' in task:
                if task['runtime'] != 'udocker' and task['runtime'] != 'singularity':
                    return (False, 'the container runtime must be either udocker or singularity')
            else:
                return (False, 'a container runtime must be defined')

            if 'env' in task:
                if not isinstance(task['env'], dict):
                    return (False, 'environment variables must be defined as a dict')

            if 'procsPerNode' in task:
                if not str(task['procsPerNode']).isdigit():
                    return (False, 'number of processes per node must be an integer')
                if task['procsPerNode'] < 1:
                    return (False, 'number of processes per node must be at least 1')
                if 'cpus' in job['resources']:
                    if task['procsPerNode'] > job['resources']['cpus']:
                        return (False, 'number of processes per node must be less than number of CPU cores per node')

            if 'type' in task:
                if task['type'] != 'openmpi' and task['type'] != 'mpich' and task['type'] != 'intelmpi':
                    return (False, 'invalid task type')

                if task['type'] == 'openmpi' or task['type'] == 'mpich' or task['type'] == 'intelmpi':
                    if 'cmd' in task:
                        if task['cmd'].startswith('mpirun -n'):
                            return (False, 'it is not necessary to include mpirun in the cmd if an MPI flavour has been specified')
                
    else:
        return (False, 'a job must contain tasks')

    # Artifacts
    if 'artifacts' in job:
        if not isinstance(job['artifacts'], list):
            return (False, 'an array of artifacts must be provided')

        for artifact in job['artifacts']:
            for item in artifact:
                if item not in ('url', 'mountpoint', 'executable'):
                    return (False, 'invalid item %s in artifact' % item)

            if 'url' not in artifact:
                return (False, 'an artifact must contain a URL')

            if 'mountpoint' in artifact:
                if ':' in artifact['mountpoint']:
                    src = artifact['mountpoint'].split(':')[0]
                    dst = artifact['mountpoint'].split(':')[1]
                    if '/' in src:
                        return (False, 'invalid source in mountpoint for artifact')
                    if not dst.startswith('/'):
                        return (False, 'the mountpoint for an artifact must be an absolute path')
                else:
                    return (False, 'invalid mountpoint for artifact')

    # Output files
    if 'outputFiles' in job:
        if not isinstance(job['outputFiles'], list):
            return (False, 'an array of outputFiles must be provided')

    # Output directories
    if 'outputDirs' in job:
        if not isinstance(job['outputDirs'], list):
            return (False, 'an array of outputDirs must be provided')

    # Inputs
    if 'inputs' in job:
        if not isinstance(job['inputs'], list):
            return (False, 'an array of inputs must be provided')
        for inpt in job['inputs']:
            for item in inpt:
                if item not in ('filename', 'content', 'executable'):
                    return (False, 'invalid item %s in input' % item)

            if 'filename' not in inpt:
                return (False, 'each input must contain a filename')
            if 'content' not in inpt:
                return (False, 'each input must contain base64 encoded content')

    # Storage
    if 'storage' in job:
        if 'type' not in job['storage']:
            return (False, 'storage type must be defined')
        if job['storage']['type'] != 'onedata' and job['storage']['type'] != 'b2drop' and job['storage']['type'] != 'webdav':
            return (False, 'storage type must be either b2drop, webdav or onedata')
        if 'mountpoint' not in job['storage']:
            return (False, 'a mount point must be defined')
        if not job['storage']['mountpoint'].startswith('/'):
            return (False, 'the mountpoint must be an absolute path')

        if job['storage']['type'] == 'b2drop':
            if 'b2drop' not in job['storage']:
                return (False, 'B2DROP storage details must be defined')
            if 'app-username' not in job['storage']['b2drop']:
                return (False, 'B2DROP app username must be defined')
            if 'app-password' not in job['storage']['b2drop']:
                return (False, 'B2DROP app password must be defined')
        elif job['storage']['type'] == 'onedata':
            if 'onedata' not in job['storage']:
                return (False, 'OneData storage details must be defined')
            if 'provider' not in job['storage']['onedata']:
                return (False, 'OneData provider must be defined')
            if 'token' not in job['storage']['onedata']:
                return (False, 'OneData token must be defined')
        elif job['storage']['type'] == 'webdav':
            if 'webdav' not in job['storage']:
                return (False, 'WebDAV storage details must be defined')
            if 'url' not in job['storage']['webdav']:
                return (False, 'WebDAV URL must be defined')
            if 'username' not in job['storage']['webdav']:
                return (False, 'WebDAV username must be defined')
            if 'password' not in job['storage']['webdav']:
                return (False, 'WebDAV password must be defined')

    # Polices
    if 'policies' in job:
        for item in job['policies']:
            if item not in policies_valids:
                return (False, 'invalid item "%s" in policies' % item)

        if 'placement' in job['policies']:
            (status, msg) = validate_placement(job['policies']['placement'])
            if not status:
                return (status, msg)

        if 'maximumRetries' in job['policies']:
            if not str(job['policies']['maximumRetries']).isdigit():
                return (False, 'the number of retries must be an integer')

            if job['policies']['maximumRetries'] < 1:
                return (False, 'the number of retries must be greater than 0')

            if job['policies']['maximumRetries'] > 6:
                return (False, 'the number of retries must be less than 6')

        if 'maximumTimeInQueue' in job['policies']:
            if not str(job['policies']['maximumTimeInQueue']).isdigit():
                return (False, 'the maximum time in queue must be an integer')

            if job['policies']['maximumTimeInQueue'] < 1:
                return (False, 'the maximum time in queue must be greater than 0')

            if job['policies']['maximumTimeInQueue'] > 44640:
                return (False, 'the maximum time in queue must be less than 44640')

        if 'maximumIdleTimePerResource' in job['policies']:
            if not str(job['policies']['maximumIdleTimePerResource']).isdigit():
                return (False, 'the maximum idle time per resource must be an integer')

            if job['policies']['maximumIdleTimePerResource'] < 1:
                return (False, 'the maximum idle time per resource must be greater than 0')

            if job['policies']['maximumIdleTimePerResource'] > 44640:
                return (False, 'the maximum idle time per resource must be less than 44640')

    # Notifications
    if 'notifications' in job:
        if not isinstance(job['notifications'], list):
            return (False, 'notifications must be in the form of a list')
        for notification in job['notifications']:
            (status, msg) = validate_notification(notification, valid_events)
            if not status:
                return (status, msg)

    return (True, '')

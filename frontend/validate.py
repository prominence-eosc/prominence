""" Functions for validating jobs and workflows"""
import re
import requests

import retry

def validate_workflow(workflow):
    """
    Validate JSON workflow description
    """
    workflow_valids = ['name',
                       'labels',
                       'jobs',
                       'dependencies',
                       'factory',
                       'numberOfRetries']

    # Check for valid items in workflow
    for item in workflow:
        if item not in workflow_valids:
            return (False, 'invalid item "%s" in workflow description' % item)

    # Name
    if 'name' in workflow:
        if len(workflow['name']) > 256:
            return (False, 'workflow name must be less than 256 characters in length')

        if workflow['name'] != '' and not re.match(r'^[a-zA-Z0-9\-\_\s]+$', workflow['name']):
            return (False, 'invalid workflow name')

    # Jobs
    if 'jobs' in workflow:
        for job in workflow['jobs']:
            (status, msg) = validate_job(job)
            if not status:
                return (status, msg)
    else:
        return (False, 'a workflow must contain jobs')

    # Dependencies
    if 'dependencies' in workflow:
        if not isinstance(workflow['dependencies'], dict):
            return (False, 'dependencies must be a dict')

        jobs = []
        for job in workflow['jobs']:
            if 'name' in job:
                jobs.append(job['name'])
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
        valid_factories = ['parametricSweep']

        if 'type' not in workflow['factory']:
            return (False, 'a factory type must be specified')

        if workflow['factory']['type'] not in valid_factories:
            return (False, 'invalid factory type')

        if workflow['factory']['type'] == 'parametricSweep':
            if 'parameterSets' not in workflow['factory']:
                return (False, 'a factory of type parametricSweep must have parameterSets specified')

            if not isinstance(workflow['factory']['parameterSets'], list):
                return (False, 'parameterSets must be a list')

            for parameter_set in workflow['factory']['parameterSets']:
                if 'name' not in parameter_set:
                    return (False, 'a parameterSet must contain a name')
                if 'start' not in parameter_set:
                    return (False, 'a parameterSet must contain a start value')
                if 'end' not in parameter_set:
                    return (False, 'a parameterSet must contain an end value')
                if 'step' not in parameter_set:
                    return (False, 'a parameterSet must contain a step')

    # Retries
    if 'numberOfRetries' in workflow:
        if not str(workflow['numberOfRetries']).isdigit():
            return (False, 'the number of retries must be an integer')

        if workflow['numberOfRetries'] < 1:
            return (False, 'the number of retries must be greater than 0')

        if workflow['numberOfRetries'] > 6:
            return (False, 'the number of retries must be less than 6')

    # Labels
    if 'labels' in workflow:
        if not isinstance(workflow['labels'], dict):
            return (False, 'labels must be defined as a dict')
        for label in workflow['labels']:
            if len(label) > 64:
                return (False, 'label names must be less than 64 characters in length')
            if len(workflow['labels'][label]) > 64:
                return (False, 'label values must be less than 64 characters in length')

            if not re.match(r'^[a-zA-Z0-9]+$', label):
                return (False, 'label name "%s" is invalid' % label)
            if not re.match(r'^[\w\-\_\.\/]+$', workflow['labels'][label]):
                return (False, 'label value "%s" is invalid' % workflow['labels'][label])

    return (True, '')

def validate_job(job):
    """
    Validate JSON job description
    """
    job_valids = ['name',
                  'labels',
                  'preemptible',
                  'tasks',
                  'resources',
                  'inputs',
                  'artifacts',
                  'outputFiles',
                  'outputDirs',
                  'storage',
                  'numberOfRetries']

    task_valids = ['image',
                   'cmd',
                   'env',
                   'workdir',
                   'procsPerNode',
                   'type',
                   'runtime']

    resources_valids = ['nodes',
                        'cpus',
                        'memory',
                        'disk',
                        'walltime']

    # Check for valid items in job
    for item in job:
        if item not in job_valids:
            return (False, 'invalid item "%s" in job description' % item)

    # Name
    if 'name' in job:
        if len(job['name']) > 256:
            return (False, 'job name must be less than 256 characters in length')

        if job['name'] != '' and not re.match(r'^[a-zA-Z0-9\-\_\s]+$', job['name']):
            return (False, 'invalid job name')

    # Labels
    if 'labels' in job:
        if not isinstance(job['labels'], dict):
            return (False, 'labels must be defined as a dict')
        for label in job['labels']:
            if len(label) > 64:
                return (False, 'label names must be less than 64 characters in length')
            if len(job['labels'][label]) > 64:
                return (False, 'label values must be less than 64 characters in length')

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
                if task['type'] != 'openmpi' and task['type'] != 'mpich':
                    return (False, 'invalid task type')

    else:
        return (False, 'a job must contain tasks')

    # Artifacts
    if 'artifacts' in job:
        if not isinstance(job['artifacts'], list):
            return (False, 'an array of artifacts must be provided')

        for artifact in job['artifacts']:
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
            if 'filename' not in inpt:
                return (False, 'each input must contain a filename')
            if 'content' not in inpt:
                return (False, 'each input must contain base64 encoded content')

    # Storage
    if 'storage' in job:
        if 'type' not in job['storage']:
            return (False, 'storage type must be defined')
        if job['storage']['type'] != 'onedata' and job['storage']['type'] != 'b2drop':
            return (False, 'storage type must be either b2drop or onedata')
        if 'mountpoint' not in job['storage']:
            return (False, 'a mount point must be defined')
        if not job['storage']['mountpoint'].startswith('/'):
            return (False, 'the mountpoint must be an absolute path')

        if job['storage']['type'] == 'b2drop':
            if 'b2drop' not in job['storage']:
                return (False, 'b2drop storage details must be defined')
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

    # Retries
    if 'numberOfRetries' in job:
        if not str(job['numberOfRetries']).isdigit():
            return (False, 'the number of retries must be an integer')

        if job['numberOfRetries'] < 1:
            return (False, 'the number of retries must be greater than 0')

        if job['numberOfRetries'] > 6:
            return (False, 'the number of retries must be less than 6')

    return (True, '')

@retry.retry(tries=2, delay=1, backoff=1)
def validate_presigned_url(url):
    """
    Validate a presigned URL
    """
    try:
        response = requests.get(url, timeout=30)
    except requests.exceptions.RequestException:
        return False

    if response.status_code != 200:
        return False
    return True

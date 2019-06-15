import json
import re

def validate_job(job):
    """
    Validate JSON job description
    """
    # Name
    if 'name' in job:
        if len(job['name']) > 64:
            return (False, 'job name must be less than 64 characters in length')
        if job['name'] != '' and not re.match(r'^[a-zA-Z0-9\-\_]+$', job['name']):
            return (False, 'invalid job name')

    # Labels
    if 'labels' in job:
        if not isinstance(job['labels'], dict):
            return (False, 'labels must be defined as a dict')
        for label in job['labels']:
            print(label)
            if len(label) > 64:
                return (False, 'label names must be less than 64 characters in length')
            if len(job['labels'][label]) > 64:
                return (False, 'label values must be less than 64 characters in length')

            if not re.match(r'^[a-zA-Z0-9]+$', label):
                return (False, 'label name is invalid')
            if not re.match(r'^[\w\-\_\.\/]+$', job['labels'][label]):
                return (False, 'label value is invalid')

    # Resources
    if 'resources' in job:
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

        if 'memory' in job['resources']:
            if not str(job['resources']['memory']).isdigit():
                return (False, 'memory must be an integer')
            if job['resources']['memory'] < 1:
                return (False, 'memory must be at least 1')

        if 'disk' in job['resources']:
            if not str(job['resources']['disk']).isdigit():
                return (False, 'required disk must be an integer')
            if job['resources']['disk'] < 1:
                return (False, 'disk must be at least 1')
    else:
        return (False, 'a job must contain resources')

    # Tasks
    if 'tasks' in job:
        if not isinstance(job['tasks'], list):
            return (False, 'an array of tasks must be provided')

        for task in job['tasks']:
            if 'image' not in task:
                return (False, 'each task must specify a container image')
            if 'runtime' in task:
                if task['runtime'] != 'udocker' and task['runtime'] != 'singularity':
                    return (False, 'the container runtime must be either udocker or singularity')
            if 'env' in task:
                if not isinstance(task['env'], dict):
                    return (False, 'environment variables must be defined as a dict')
            if 'procsPerNode' in task:
                if not str(task['procsPerNode']).isdigit():
                    return (False, 'number of processes per node must be an integer')
                if task['procsPerNode'] < 1:
                    return (False, 'number of processes per node must be at least 1')
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
        for input in job['inputs']:
            if 'filename' not in input:
                return (False, 'each input must contain a filename')
            if 'content' not in input:
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

    return (True, '')

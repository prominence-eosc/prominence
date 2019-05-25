import json
import re

def validate_job(job):
    """
    Validate JSON job description
    """
    # Name
    if 'name' in job:
        match = re.match(r'([\w\-\_]+)', job['name'])
        if job['name'] != '' and (not match or len(job['name']) > 64):
            return (False, 'invalid job name specified')

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

    # Inputs
    if 'inputs' in job:
        if not isinstance(job['inputs'], list):
            return (False, 'an array of inputs must be provided')

    return (True, '')

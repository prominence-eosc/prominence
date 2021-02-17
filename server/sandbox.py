import base64
import json
import logging
import os

# Get an instance of a logger
logger = logging.getLogger(__name__)

def create_sandbox(uid, sandbox):
    """
    Create job sandbox
    """
    job_sandbox = os.path.join(sandbox, uid)
    try:
        os.makedirs(job_sandbox)
        os.makedirs(os.path.join(job_sandbox, 'input'))
    except IOError as err:
        logger.critical('Unable to create job sandbox due to: %s', err)
        return None
    return job_sandbox

def write_json(desc, path, name):
    """
    Write JSON description to disk
    """
    try:
        with open(os.path.join(path, name), 'w') as file:
            json.dump(desc, file)
    except IOError as err:
        logger.critical('Unable to write JSON file to disk due to: %s', err)
        return False

    return True

def write_inputs(job_desc, path):
    """
    Write any input files to sandbox directory
    """
    filenames = []
    input_files = []

    if 'inputs' in job_desc:
        filenames = []
        for file_input in job_desc['inputs']:
            filename_new = os.path.join(path + '/input', os.path.basename(file_input['filename']))
            try:
                with open(filename_new, 'w') as file:
                    file.write(base64.b64decode(file_input['content']).decode('utf-8'))
            except IOError as err:
                logger.critical('Unable to write input file to disk due to: %s', err)
                return (None, None)

            if 'executable' in file_input:
                if file_input['executable']:
                    try:
                        os.chmod(filename_new, 0o775)
                    except IOError as err:
                        logger.critical('Unable to change input file permissions to executable due to: %s', err)
                        return (None, None)

            filenames.append(file_input['filename'])
            input_files.append(filename_new)

    return (filenames, input_files)

def write_job(job_desc, uuid, sandbox):
    """
    Write JSON job description and any input files to the job sandbox
    """
    if not write_json(job_desc, os.path.join(sandbox, uuid), 'job.json'):
        return False

    if not write_inputs(job_desc, os.path.join(sandbox, uuid)):
        return False

    return True

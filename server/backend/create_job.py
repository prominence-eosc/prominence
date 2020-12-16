import logging
import os
import shutil

import classad
import htcondor

# Get an instance of a logger
logger = logging.getLogger(__name__)

def create_job(self, username, groups, email, uid, jjob):
    """
    Create a job
    """
    # Create the job sandbox
    job_sandbox = self.create_sandbox(uid)
    if job_sandbox is None:
        logger.critical('Unable to create job sandbox for user %s and job uid %s', username, uid)
        return (1, {"error":"Unable to create job sandbox"})

    # Copy executable to sandbox, change current working directory to the sandbox
    shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, 'promlet.py'))

    os.chdir(job_sandbox)
    os.chmod(os.path.join(job_sandbox, 'promlet.py'), 0o775)

    # Create dict containing HTCondor job
    (status, msg, cjob) = self._create_htcondor_job(username, groups, email, uid, jjob, job_sandbox)

    # Check if we have an error
    if status != 0:
        return (1, msg)

    # Submit the job to HTCondor
    data = {}
    retval = 0

    try:
        sub = htcondor.Submit(cjob)
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            cid = sub.queue(txn, 1)
        data['id'] = cid
    except Exception as err:
        logger.critical('Exception submitting job for user %s and uid %s to HTCondor due to %s', username, uid, err)
        retval = 1
        data = {"error":"Job submission failed with an exception"}

    return (retval, data)

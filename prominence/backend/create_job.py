import os
import shutil
import time

import classad
import htcondor

def submit_job(cjob):
    data = {}
    retval = 0

    try:
        sub = htcondor.Submit(cjob)
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            cid = sub.queue(txn, 1)
        data['id'] = cid
    except Exception as err:
        retval = 1
        data = {"error":"Job submission failed with an exception: %s" % err}

    return (retval, data)

def create_job(self, username, groups, email, uid, jjob):
    """
    Create a job
    """
    # Create the job sandbox
    job_sandbox = self.create_sandbox(uid)
    if job_sandbox is None:
        return (1, {"error":"Unable to create job sandbox"})

    # Copy executable to sandbox, change current working directory to the sandbox
    #if 'CCFE/Test' in groups:
    #    shutil.copyfile('/usr/local/libexec/promlet.py-29sep2021', os.path.join(job_sandbox, 'promlet.py'))
    #else:
    shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, 'promlet.py'))

    os.chdir(job_sandbox)
    os.chmod(os.path.join(job_sandbox, 'promlet.py'), 0o775)

    # Create dict containing HTCondor job
    (status, msg, cjob) = self._create_htcondor_job(username, groups, email, uid, jjob, job_sandbox)

    # Check if we have an error
    if status != 0:
        return (1, msg)

    # Submit the job to HTCondor, including up to 5 retries
    retval = 1
    count = 0
    while retval == 1 and count < 5:
        (retval, data) = submit_job(cjob)
        time.sleep(count*0.3)
        count += 1

    return (retval, data)

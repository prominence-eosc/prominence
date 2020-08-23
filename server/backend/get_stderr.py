import os

def get_stderr(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1):
    """
    Return the stdout from the specified job
    """
    if instance_id > -1:
        if os.path.isfile('%s/job.%d.err' % (iwd, instance_id)):
            with open('%s/job.%d.err' % (iwd, instance_id)) as fd:
                return fd.read()
    elif os.path.isfile('%s/%s' % (iwd, err)):
        with open('%s/%s' % (iwd, err)) as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.err' % (iwd, job_name)):
        with open('%s/%s/job.0.err' % (iwd, job_name)) as fd:
            return fd.read()
    elif os.path.isfile(err):
        with open(err) as fd:
            return fd.read()
    return None

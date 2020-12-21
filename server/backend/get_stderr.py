import os

def get_stderr(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1, content=True):
    """
    Return the stdout from the specified job
    """
    if instance_id > -1:
        if os.path.isfile('%s/job.%d.err' % (iwd, instance_id)):
            if not content:
                return '%s/job.%d.err' % (iwd, instance_id)
            with open('%s/job.%d.err' % (iwd, instance_id), 'rt') as fd:
                return fd.read()
    elif os.path.isfile('%s/%s' % (iwd, err)):
        if not content:
            return '%s/%s' % (iwd, err)
        with open('%s/%s' % (iwd, err), 'rt') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.err' % (iwd, job_name)):
        if not content:
            return '%s/%s/job.0.err' % (iwd, job_name)
        with open('%s/%s/job.0.err' % (iwd, job_name), 'rt') as fd:
            return fd.read()
    elif err and os.path.isfile(err):
        if not content:
            return err
        with open(err) as fd:
            return fd.read()
    return None

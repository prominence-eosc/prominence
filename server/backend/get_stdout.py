import os

def get_stdout(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1, content=True):
    """
    Return the stdout from the specified job
    """
    if instance_id > -1:
        if os.path.isfile('%s/job.%d.out' % (iwd, instance_id)):
            if not content:
                return '%s/job.%d.out' % (iwd, instance_id)
            with open('%s/job.%d.out' % (iwd, instance_id), 'rt') as fd:
                return fd.read()
    elif os.path.isfile('%s/%s' % (iwd, out)):
        if not content:
            return '%s/%s' % (iwd, out)
        with open('%s/%s' % (iwd, out), 'rt') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.out' % (iwd, job_name)):
        if not content:
            return '%s/%s/job.0.out' % (iwd, job_name)
        with open('%s/%s/job.0.out' % (iwd, job_name), 'rt') as fd:
            return fd.read()
    elif out and os.path.isfile(out):
        if not content:
            return out
        with open(out) as fd:
            return fd.read()
    return None

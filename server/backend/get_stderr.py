import os

def get_stderr(self, sandbox, job_name=None, instance_id=0, content=True):
    """
    Return the stderr (or filename only) from the specified job
    """
    if os.path.isfile('%s/job.%d.err' % (sandbox, instance_id)):
        if not content:
            return '%s/job.%d.err' % (sandbox, instance_id)
        with open('%s/job.%d.err' % (sandbox, instance_id), 'rt') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.%d.err' % (sandbox, job_name, instance_id)):
        if not content:
            return '%s/%s/job.%d.err' % (sandbox, job_name, instance_id)
        with open('%s/%s/job.%d.err' % (sandbox, job_name, instance_id), 'rt') as fd:
            return fd.read()

    return None

import os

def get_stderr(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1, node=0):
    """
    Return the stdout from the specified job
    """
    if '#pArAlLeLnOdE#' in err:
        err = err.replace('#pArAlLeLnOdE#', '%d' % node)

    if instance_id > -1:
        if os.path.isfile('%s/job.%d.err' % (iwd, instance_id)):
            with open('%s/job.%d.err' % (iwd, instance_id), 'rb') as fd:
                return fd.read()
        if os.path.isfile('%s/job.%d.err.%d' % (iwd, instance_id, node)):
            with open('%s/job.%d.err.%d' % (iwd, instance_id, node), 'rb') as fd:
                return fd.read()
    elif os.path.isfile('%s/%s' % (iwd, err)):
        with open('%s/%s' % (iwd, err), 'rb') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.err' % (iwd, job_name)):
        with open('%s/%s/job.0.err' % (iwd, job_name), 'rb') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.err.%d' % (iwd, job_name, node)):
        with open('%s/%s/job.0.err.%d' % (iwd, job_name, node), 'rb') as fd:
            return fd.read()
    elif os.path.isfile(err):
        with open(err, 'rb') as fd:
            return fd.read()
    return None

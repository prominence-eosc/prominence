import os

def get_stdout(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1, node=0):
    """
    Return the stdout from the specified job
    """
    if '#pArAlLeLnOdE#' in out:
        out = out.replace('#pArAlLeLnOdE#', '%d' % node)

    if instance_id > -1:
        if os.path.isfile('%s/job.%d.out' % (iwd, instance_id)):
            with open('%s/job.%d.out' % (iwd, instance_id), 'rb') as fd:
                return fd.read()
        if os.path.isfile('%s/job.%d.out.%d' % (iwd, instance_id, node)):
            with open('%s/job.%d.out.%d' % (iwd, instance_id, node), 'rb') as fd:
                return fd.read()
    elif os.path.isfile('%s/%s' % (iwd, out)):
        with open('%s/%s' % (iwd, out), 'rb') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.out' % (iwd, job_name)):
        with open('%s/%s/job.0.out' % (iwd, job_name), 'rb') as fd:
            return fd.read()
    elif os.path.isfile('%s/%s/job.0.out.%d' % (iwd, job_name, node)):
        with open('%s/%s/job.0.out.%d' % (iwd, job_name, node), 'rb') as fd:
            return fd.read()
    elif os.path.isfile(out):
        with open(out, 'rb') as fd:
            return fd.read()
    return None

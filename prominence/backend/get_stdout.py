import os
from .utilities import readfile

def get_stdout(self, uid, iwd, out, err, job_id, job_name=None, instance_id=-1, node=0, offset=0):
    """
    Return the stdout from the specified job
    """
    if '#pArAlLeLnOdE#' in out:
        out = out.replace('#pArAlLeLnOdE#', '%d' % node)

    if instance_id > -1:
        if os.path.isfile('%s/job.%d.out' % (iwd, instance_id)):
            return readfile('%s/job.%d.out' % (iwd, instance_id), offset)
        if os.path.isfile('%s/job.%d.out.%d' % (iwd, instance_id, node)):
            return readfile('%s/job.%d.out.%d' % (iwd, instance_id, node), offset)
    elif os.path.isfile('%s/%s' % (iwd, out)):
        return readfile('%s/%s' % (iwd, out), offset)
    elif os.path.isfile('%s/%s/job.0.out' % (iwd, job_name)):
        return readfile('%s/%s/job.0.out' % (iwd, job_name), offset)
    elif os.path.isfile('%s/%s/job.0.out.%d' % (iwd, job_name, node)):
        return readfile('%s/%s/job.0.out.%d' % (iwd, job_name, node), offset)
    elif os.path.isfile(out):
        return readfile(out, offset)
    return None

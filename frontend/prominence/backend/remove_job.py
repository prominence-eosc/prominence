import htcondor

def remove_job(self, job_id):
    """
    Remove the specified job from the queue
    """
    constraint = 'ProminenceType == "job" && ClusterId == %d' % int(job_id)

    schedd = htcondor.Schedd()
    schedd.edit(constraint, 'ProminenceRemoveFromQueue', 'True')

    return

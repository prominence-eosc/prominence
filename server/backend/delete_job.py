import htcondor

def delete_job(self, username, job_ids):
    """
    Delete the specified job(s)
    """
    constraints = []
    for job_id in job_ids:
        constraints.append('ClusterId == %d' % int(job_id))
    constraint = '(%s) && ProminenceIdentity == "%s" && ProminenceType == "job"' % (' || '.join(constraints), username)

    try:
        schedd = htcondor.Schedd()
        ret = schedd.act(htcondor.JobAction.Remove, constraint)
    except Exception as err:
        return (1, {"error": err})

    return (0, {})

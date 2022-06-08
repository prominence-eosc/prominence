import classad
import htcondor

def delete_job(self, username, job_ids):
    """
    Delete the specified job(s)
    """
    constraints = []
    for job_id in job_ids:
        constraints.append('ClusterId == %d' % int(job_id))
    constraint = '(%s) && ProminenceIdentity == "%s" && ProminenceType == "job"' % (' || '.join(constraints), username)

    schedd = htcondor.Schedd()
    ret = schedd.act(htcondor.JobAction.Remove, constraint)

    if ret["TotalSuccess"] > 0:
        return (0, {})
    return (1, {"error":"No such job(s)"})

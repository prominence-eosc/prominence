import classad
import htcondor

def delete_workflow(self, username, workflow_ids):
    """
    Delete the specified workflow(s)
    """
    constraints = []
    for workflow_id in workflow_ids:
        constraints.append('ClusterId == %d' % int(workflow_id))
    constraint = '(%s) && ProminenceIdentity == "%s" && ProminenceType == "workflow"' % (' || '.join(constraints), username)

    schedd = htcondor.Schedd()
    ret = schedd.act(htcondor.JobAction.Remove, constraint)

    if ret["TotalSuccess"] > 0:
        return (0, {})
    return (1, {"error":"No such workflow(s)"})

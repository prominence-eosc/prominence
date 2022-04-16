import htcondor

def remove_workflow(self, workflow_id):
    """
    Remove the specified workflow from the queue
    """
    constraint = 'ProminenceType == "workflow" && ClusterId == %d' % int(workflow_id)
    
    try:
        schedd = htcondor.Schedd()
        schedd.edit(constraint, 'ProminenceRemoveFromQueue', 'True')
    except:
        return False

    return True

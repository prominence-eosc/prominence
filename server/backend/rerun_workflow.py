import json
import re
import shutil
import uuid

import htcondor

from .utilities import run

def rerun_workflow(self, username, groups, email, workflow_id):
    """
    Re-run any failed jobs from a completed workflow
    """
    schedd = htcondor.Schedd()

    constraint = 'ProminenceIdentity =?= "%s" && ClusterId == %d' % (username, workflow_id)
    wfs = schedd.history('RoutedBy =?= undefined && ProminenceType == "workflow" && %s' % constraint,
                         ['JobStatus', 'Iwd', 'JobBatchName'],
                         1)

    job_status = None
    iwd = None
    name = ''

    for wf in wfs:
        if 'JobStatus' in wf:
            job_status = wf['JobStatus']
        if 'Iwd' in wf:
            iwd = wf['Iwd']
        if 'JobBatchName' in wf:
            name = wf['JobBatchName']

    if job_status != 3 and job_status != 4:
        return (1, {"error":"Unable to find re-run workflow as the original workflow is not in a suitable state"})

    # Read the workflow json description
    try:
        with open('%s/workflow.json' % iwd, 'r') as json_file:
            jjob = json.load(json_file)
    except IOError:
        pass

    # Handle labels
    dag_appends = []
    if 'labels' in jjob:
        for label in jjob['labels']:
            value = jjob['labels'][label]
            dag_appends.append("'+ProminenceUserMetadata_%s=\"%s\"'" % (label, value))

    # Create command to submit to DAGMan
    dag_appends.append("'+ProminenceType=\"workflow\"'")
    dag_appends.append("'+ProminenceIdentity=\"%s\"'" % username)
    dag_appends.append("'+ProminenceJobUniqueIdentifier=\"%s\"'" % str(uuid.uuid4()))
    dag_appends.append("'+ProminenceGroup=\"%s\"'" % groups)

    if email:
        dag_appends.append("'+ProminenceEmail=\"%s\"'" % email)

    cmd = "condor_submit_dag -maxidle %d -batch-name %s " % (int(self._config['WORKFLOW_MAX_IDLE']), name)
    for dag_append in dag_appends:
        cmd += " -append %s " % dag_append
    cmd += " job.dag "

    (return_code, stdout, stderr, timedout) = run(cmd, iwd, 30)

    m = re.search(r'submitted to cluster\s(\d+)', str(stdout))
    data = {}
    if m:
        retval = 0
        data['id'] = int(m.group(1))
    else:
        retval = 1
        data = {"error":"Workflow resubmission failed due to %s" % stderr}

    try:
        shutil.copyfile('%s/workflow.dag.status' % iwd,
                        '%s/workflow.dag.status-%d' % (iwd, workflow_id))
    except:
        pass

    return (retval, data)

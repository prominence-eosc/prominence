import json
import os
import re
import shutil
import uuid

import jwt
import classad
import htcondor

from .utilities import run
from .create_job_token import create_job_token

def get_failed_node_dirs(iwd):
    """
    Create list of failed nodes
    """
    failed_jobs = []
    filename = '%s/workflow.dag.status' % iwd
    with open(filename, 'r') as fd:
        ads = classad.parseAds(fd)
        for ad in ads:
            if 'Type' in ad:
                if ad['Type'] == "NodeStatus":
                    if 'NodeStatus' in ad:
                        node_status = int(ad['NodeStatus'])
                        if node_status == 6:
                            node = ad['Node']
                            if os.path.isdir(node):
                                failed_jobs.append(node)
                            else:
                                base_dir = node.rsplit('_', 1)[0]
                                if os.path.isdir('%s/%s' % (iwd, base_dir)):
                                    if base_dir not in failed_jobs:
                                        failed_jobs.append(base_dir)
    return failed_jobs

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

    # Get list of failed jobs
    failed_job_dirs = get_failed_node_dirs(iwd)

    # Write new token into job description files of failed jobs
    for job_dir in failed_job_dirs:
        filename = '%s/%s/job.jdl' % (iwd, job_dir)
        job_idl = None
        try:
            with open(filename, 'r') as fh:
                job_idl = fh.readlines()
        except IOError:
            pass

        if job_idl:
            new_token = None
            job_json_file = None

            try:
                with open('%s/%s/.job.json' % (iwd, job_dir), 'r') as json_file:
                    job_json_file = json.load(json_file)
            except:
                pass

            walltime = 0
            if job_json_file:
                if 'resources' in job_json_file:
                    if 'walltime' in job_json_file['resources']:
                        walltime = job_json_file['resources']['walltime']

            for line in job_idl:
                match = re.search(r'ProminenceJobToken\s=\s"(.*)"', line)
                if match:
                    old_token = match.group(1)
                    decoded_old_token = jwt.decode(old_token, options={"verify_signature": False})
                    new_token = create_job_token(decoded_old_token['username'],
                                                 decoded_old_token['groups'],
                                                 decoded_old_token['job'],
                                                 email,
                                                 10*24*60*60 + walltime*2*60*3)

            if not os.path.isfile('%s/%s/job.jdl.old' % (iwd, job_dir)) and new_token:
                try:
                    os.rename(filename, '%s/%s/job.jdl.old' % (iwd, job_dir))
                except IOError:
                    pass

            if new_token:
                with open(filename, 'w') as fh:
                    for line in job_idl:
                        if 'ProminenceJobToken = ' not in line:
                            fh.write(line)
                        else:
                            fh.write('+ProminenceJobToken = "%s"\n' % new_token)

    # Write new presigned URLs into workflow description file

    # Handle labels
    dag_appends = []
    if 'labels' in jjob:
        for label in jjob['labels']:
            value = jjob['labels'][label]
            dag_appends.append("'+ProminenceUserMetadata_%s=\"%s\"'" % (label, value))

    # Create command to submit to DAGMan
    dag_appends.append("'+ProminenceType=\"workflow\"'")
    dag_appends.append("'+ProminenceIdentity=\"%s\"'" % username)
    dag_appends.append("'+ProminenceGroup=\"%s\"'" % groups)
    dag_appends.append("'+ProminenceJobUniqueIdentifier=\"%s\"'" % str(uuid.uuid4()))

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
        data = {"error":"Workflow resubmission failed"}

    try:
        shutil.copyfile('%s/workflow.dag.status' % iwd,
                        '%s/workflow.dag.status-%d' % (iwd, workflow_id))
    except:
        pass

    return (retval, data)

import json
import os

import classad
import htcondor

from .utilities import redact_storage_creds

def list_workflows(self, workflow_ids, identity, active, completed, num, detail, constraint, name_constraint):
    """
    List workflows or describe a specified workflow
    """
    required_attrs = ['JobStatus',
                      'ClusterId',
                      'ProcId',
                      'DAGManJobId',
                      'JobBatchName',
                      'RemoveReason',
                      'QDate',
                      'JobStartDate',
                      'CompletionDate',
                      'Cmd',
                      'Iwd'
                      ]
    jobs_state_map = {1:'idle',
                      2:'running',
                      3:'failed',
                      4:'completed',
                      5:'failed'}

    schedd = htcondor.Schedd()

    wfs = []
    wfs_condor = []

    if constraint[0] is not None and constraint[1] is not None:
        restrict = str('ProminenceUserMetadata_%s =?= "%s"' % (constraint[0], constraint[1]))
    else:
        restrict = 'True'
    constraintc = 'ProminenceIdentity =?= "%s" && %s' % (identity, restrict)
    if len(workflow_ids) > 0:
        constraints = []
        for workflow_id in workflow_ids:
            constraints.append('ClusterId == %d' % int(workflow_id))
        constraintc = '(%s) && %s' % (' || '.join(constraints), constraintc)
        num = len(workflow_ids)

    if name_constraint is not None:
        constraintc = 'JobBatchName =?= "%s" && %s' % (str(name_constraint), constraintc)

    # Get completed workflows if necessary
    if completed:
        wfs_completed = schedd.history('RoutedBy =?= undefined && ProminenceType == "workflow" && %s' % constraintc, required_attrs, int(num))
        wfs_condor.extend(wfs_completed)

    # Get active workflows if necessary
    if active:
        wfs_active = schedd.xquery('RoutedBy =?= undefined && ProminenceType == "workflow" && %s' % constraintc, required_attrs)
        wfs_condor.extend(wfs_active)

    for wf in wfs_condor:
        wfj = {}

        if detail > 0:
            try:
                with open('%s/workflow.json' % wf['Iwd'], 'r') as json_file:
                    wfj = json.load(json_file)
            except IOError:
                continue
        else:
            wfj['name'] = wf['JobBatchName']

        # Redact credentials if necessary
        if 'storage' in wfj:
            wfj['storage'] = redact_storage_creds(wfj['storage'])
        if 'jobs' in wfj:
            for job in wfj['jobs']:
                if 'storage' in job:
                    job['storage'] = redact_storage_creds(job['storage'])

        wfj['id'] = wf['ClusterId']
        wfj['status'] = jobs_state_map[wf['JobStatus']]

        events = {}
        events['createTime'] = int(wf['QDate'])
        if 'JobStartDate' in wf:
            events['startTime'] = int(wf['JobStartDate'])

        # The end time of a DAG job does not appear to be in the job's ClassAd, so instead we
        # get the end time from the job.dag.metrics file
        dag_metrics = {}
        try:
            with open('%s/job.dag.metrics' % wf['Iwd'], 'r') as json_file:
                dag_metrics = json.load(json_file)
        except IOError:
            pass

        end_time = 0
        if 'CompletionDate' in wf:
            end_time = int(wf['CompletionDate'])
            if end_time > 0:
                events['endTime'] = end_time

        if 'end_time' in dag_metrics and end_time == 0:
            events['endTime'] = int(dag_metrics['end_time'])

            # For rescue DAGs, end_time might already exist, but it's from the original workflow
            if 'startTime' in events:
                if events['endTime'] < events['startTime']:
                    del events['endTime']

        wfj['events'] = events

        nodes_total = 0
        nodes_done = 0
        nodes_failed = 0
        nodes_queued = 0
        nodes_unready = 0
        dag_status = 0

        node_stats = {}

        file = '%s/workflow.dag.status-%d' % (wf['Iwd'], int(wf['ClusterId']))
        if not os.path.exists(file):
            file = '%s/workflow.dag.status' % wf['Iwd']

        node_state_map = {0:'waiting',
                          1:'idle',
                          3:'running',
                          5:'completed',
                          6:'failed'}

        try:
            class_ads = classad.parseAds(open(file, 'r'))
            for class_ad in class_ads:
                if class_ad['Type'] == 'DagStatus':
                    nodes_total = class_ad['NodesTotal']
                    nodes_done = class_ad['NodesDone']
                    nodes_failed = class_ad['NodesFailed']
                    nodes_queued = class_ad['NodesQueued']
                    nodes_unready = class_ad['NodesUnready']
                    dag_status = class_ad['DagStatus']
                if class_ad['Type'] == 'NodeStatus':
                    node = class_ad['Node']
                    stats = {}
                    stats['status'] = node_state_map[class_ad['NodeStatus']]
                    stats['retries'] = class_ad['RetryCount']
                    node_stats[node] = stats
        except Exception:
            pass

        nodes = {}
        jobs = {}

        # If no jobs have been created, report status as idle
        if nodes_queued == 0 and wfj['status'] != 'completed' and wfj['status'] != 'failed':
            wfj['status'] = 'idle'

        nodes['total'] = nodes_total
        nodes['done'] = nodes_done
        nodes['failed'] = nodes_failed
        nodes['queued'] = nodes_queued
        nodes['waiting'] = nodes_unready

        # Completed workflows with failed jobs should be reported as failed, not completed
        if wfj['status'] == 'completed' and nodes_failed > 0:
            wfj['status'] = 'failed'

        # Handle workflow deleted by user
        if 'RemoveReason' in wf:
            if 'Python-initiated action' in wf['RemoveReason']:
                wfj['statusReason'] = 'Workflow deleted by user'
                wfj['status'] = 'deleted'

        wfj['progress'] = nodes

        # Return a single pending state instead of idle, deploying & waiting
        if 'USE_PENDING_STATE' in self._config:
            if wfj['status'] in ('idle', 'waiting'):
                wfj['status'] = 'pending'

        wfs.append(wfj)

    return wfs

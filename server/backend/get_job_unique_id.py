import classad
import htcondor

def get_job_unique_id(self, job_id):
    """
    Return the uid and identity for a specified job id
    """
    uid = None
    identity = None
    name = None
    iwd = None
    out = None
    err = None
    status = -1

    attributes = ['ProminenceJobUniqueIdentifier', 'ProminenceIdentity', 'Iwd', 'Out', 'Err', 'DAGNodeName', 'JobStatus']

    schedd = htcondor.Schedd()
    jobs_condor = schedd.history('RoutedBy =?= undefined && ClusterId =?= %s' % job_id, attributes, 1)
    for job in jobs_condor:
        if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
            uid = job['ProminenceJobUniqueIdentifier']
            identity = job['ProminenceIdentity']
            iwd = job['Iwd']
            out = job['Out']
            err = job['Err']
            status = job['JobStatus']
            # If a job has a DAGNodeName it must be part of a workflow, and to get the stdout/err of a such
            # a job we need to know the job name
            if 'DAGNodeName' in job:
                name = job['DAGNodeName']

    if not uid or not identity:
        jobs_condor = schedd.xquery('RoutedBy =?= undefined && ClusterId =?= %s' % job_id, attributes, 1)
        for job in jobs_condor:
            if 'ProminenceJobUniqueIdentifier' in job and 'ProminenceIdentity' in job:
                uid = job['ProminenceJobUniqueIdentifier']
                identity = job['ProminenceIdentity']
                iwd = job['Iwd']
                out = job['Out']
                err = job['Err']
                status = job['JobStatus']
                # If a job has a DAGNodeName it must be part of a workflow, and to get the stdout/err of a such
                # a job we need to know the job name
                if 'DAGNodeName' in job:
                    name = job['DAGNodeName']

    return (uid, identity, iwd, out, err, name, status)

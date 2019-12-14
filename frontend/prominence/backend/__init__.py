import os

class ProminenceBackend(object):
    """
    PROMINENCE backend class
    """
    def __init__(self, config):
        self._config = config
        self._promlet_file = '/usr/local/libexec/promlet.py'

    from create_job import create_job
    from list_jobs import list_jobs
    from delete_job import delete_job
    from create_workflow import create_workflow
    from list_workflows import list_workflows
    from delete_workflow import delete_workflow
    from get_stdout import get_stdout
    from get_stderr import get_stderr
    from execute_command import execute_command
    from snapshots import create_snapshot, get_snapshot_url
    from list_objects import list_objects
    from delete_object import delete_object

    from get_job_unique_id import get_job_unique_id
    from create_presigned_url import create_presigned_url
    from create_htcondor_job import _create_htcondor_job
    from write_htcondor_job import write_htcondor_job

    def create_sandbox(self, uid):
        """
        Create job sandbox
        """
        job_sandbox = self._config['SANDBOX_PATH'] + '/' + uid
        try:
            os.makedirs(job_sandbox)
            os.makedirs(job_sandbox + '/input')
        except:
            return None
        return job_sandbox

import os

class ProminenceBackend(object):
    """
    PROMINENCE backend class
    """
    def __init__(self, config):
        self._config = config
        self._promlet_file = '/usr/local/libexec/promlet.py'

    from .create_job import create_job
    from .list_jobs import list_jobs
    from .delete_job import delete_job
    from .remove_job import remove_job
    from .remove_workflow import remove_workflow
    from .create_workflow import create_workflow, _output_urls
    from .list_workflows import list_workflows
    from .delete_workflow import delete_workflow
    from .rerun_workflow import rerun_workflow
    from .get_stdout import get_stdout
    from .get_stderr import get_stderr
    from .execute_command import execute_command
    from .snapshots import create_snapshot, get_snapshot_url, validate_snapshot_path
    from .data import create_presigned_url, list_objects, delete_object, get_object_size
    from .get_job_unique_id import get_job_unique_id
    from .create_htcondor_job import _create_htcondor_job
    from .health import get_health

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

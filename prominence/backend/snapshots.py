import json
import subprocess
import threading

from .utilities import get_routed_job_id, kill_proc

def _create_and_upload(self, job_id_routed, cwd, path, snapshot_url):
    """
    Create a tarball & upload to S3
    """
    cmd = 'condor_ssh_to_job %s "%s tar czf ../snapshot.tgz %s && curl --upload-file ../snapshot.tgz \\\"%s\\\""' % (job_id_routed, cwd, path, str(snapshot_url))
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    timeout = {"value": False}
    timer = threading.Timer(int(self._config['EXEC_TIMEOUT']), kill_proc, [process, timeout])
    timer.start()
    output = process.communicate()[0]
    timer.cancel()
    return output

def get_snapshot_url(self, uid):
    """
    Return a pre-signed URL to retrieve a snapshot
    """
    return str(self.create_presigned_url('get', 'snapshots/%s/snapshot.tgz' % uid, 3600))

def create_snapshot(self, uid, job_id, path, userhome):
    """
    Create a snapshot of the specified path
    """
    # Firstly create the PUT URL
    snapshot_url = self.create_presigned_url('put', 'snapshots/%s/snapshot.tgz' % uid, 1000)

    # Use the routed job id, but if there isn't one use the original job id
    job_id_routed = get_routed_job_id(job_id)
    if not job_id_routed:
        job_id_routed = job_id

    # Change directory if needed
    cwd = ''
    #if userhome:
    cwd = 'cd userhome &&'

    # Create a tarball & upload to S3
    output = self._create_and_upload('%d' % job_id_routed, cwd, path, str(snapshot_url))

    if 'This is a parallel job.  Please specify job' in str(output):
        output = self._create_and_upload('%d.0.0' % job_id_routed, cwd, path, str(snapshot_url))

    return 0

def validate_snapshot_path(self, iwd, path):
    """
    Validate the path used for a snapshot
    """
    try:
        with open(iwd + '/.job.json') as json_file:
            job = json.load(json_file)
    except:
        return (None, None)

    found = None
    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if 'mountpoint' in artifact:
                mountpoint = artifact['mountpoint'].split(':')[1]
                directory = artifact['mountpoint'].split(':')[0]
                if path == mountpoint:
                    found = directory

    if not found and path.startswith('/'):
        return (None, None)
    elif path.startswith('/'):
        return (found, False)

    return (path, True)

import json
import subprocess
import threading

from utilities import get_routed_job_id, kill_proc

def get_snapshot_url(self, uid):
    """
    Return a pre-signed URL to retrieve a snapshot
    """
    return str(self.create_presigned_url('get', self._config['S3_BUCKET'], 'snapshots/%s/snapshot.tgz' % uid, 3600))

def create_snapshot(self, uid, job_id, path):
    """
    Create a snapshot of the specified path
    """
    # Firstly create the PUT URL
    snapshot_url = self.create_presigned_url('put', self._config['S3_BUCKET'], 'snapshots/%s/snapshot.tgz' % uid, 1000)

    # Use the routed job id, but if there isn't one use the original job id
    job_id_routed = get_routed_job_id(job_id)
    if not job_id_routed:
        job_id_routed = job_id

    # Create a tarball & upload to S3
    cmd = 'condor_ssh_to_job %d "tar czf snapshot.tgz %s && curl --upload-file snapshot.tgz \\\"%s\\\""' % (job_id_routed, path, snapshot_url.encode('utf-8'))
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    timeout = {"value": False}
    timer = threading.Timer(int(self._config['EXEC_TIMEOUT']), kill_proc, [process, timeout])
    timer.start()
    output = process.communicate()[0]
    timer.cancel()

    return 0

def validate_snapshot_path(self, iwd, path):
    """
    Validate the path used for a snapshot
    """
    try:
        with open(iwd + '/.job.json') as json_file:
            job = json.load(json_file)
    except:
        return None

    found = None
    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if 'mountpoint' in artifact:
                mountpoint = artifact['mountpoint'].split(':')[1]
                directory = artifact['mountpoint'].split(':')[0]
                if path == mountpoint:
                    found = directory

    if not found and path.startswith('/'):
        return None
    elif path.startswith('/'):
        return found

    return path

import json
import subprocess
import threading

from .utilities import get_routed_job_id, kill_proc

def modify_exec_command(iwd, command):
    """
    Replace any artifact mounts with actual path
    """
    try:
        with open(iwd + '/.job.json') as json_file:
            job = json.load(json_file)
    except:
        return None

    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if 'mountpoint' in artifact:
                mountpoint = artifact['mountpoint'].split(':')[1]
                directory = artifact['mountpoint'].split(':')[0]
                command = [item.replace(mountpoint, directory) for item in command]

    return command

def execute_command(self, job_id, iwd, command):
    """
    Execute a command inside a job
    """
    # Use the routed job id, but if there isn't one use the original job id
    job_id_routed = get_routed_job_id(job_id)
    if not job_id_routed:
        job_id_routed = job_id

    args = ['condor_ssh_to_job', '%d' % job_id_routed]
    args.extend(modify_exec_command(iwd, command))

    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    timeout = {"value": False}
    timer = threading.Timer(int(self._config['EXEC_TIMEOUT']), kill_proc, [process, timeout])
    timer.start()
    output = process.communicate()[0]
    timer.cancel()

    if process.returncode == 0:
        return output
    return None

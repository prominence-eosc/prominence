#!/usr/bin/python
from __future__ import print_function
import argparse
import getpass
import glob
import json
import logging
import os
import re
import shlex
import signal
from string import Template
import subprocess
import sys
import tarfile
import time
from functools import wraps
from resource import getrusage, RUSAGE_CHILDREN
from threading import Timer
import requests

CURRENT_SUBPROCS = set()
FINISH_NOW = False

def handle_signal(signum, frame):
    """
    Send signal to current subprocesses
    """
    global FINISH_NOW
    FINISH_NOW = True
    for proc in CURRENT_SUBPROCS:
        if proc.poll() is None:
            proc.send_signal(signum)

def kill_proc(proc, timeout):
    """
    Helper function used by run_with_timeout
    """
    timeout["value"] = True
    proc.kill()

def run_with_timeout(cmd, env, timeout_sec):
    """
    Run a process with a timeout
    """
    proc = subprocess.Popen(shlex.split(cmd), env=env)
    timeout = {"value": False}
    timer = Timer(timeout_sec, kill_proc, [proc, timeout])
    timer.start()
    CURRENT_SUBPROCS.add(proc)
    proc.wait()
    CURRENT_SUBPROCS.remove(proc)
    timer.cancel()
    return proc.returncode, timeout["value"]

def retry(tries=4, delay=3, backoff=2):
    """
    Retry calling the decorated function using an exponential backoff
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                rv = f(*args, **kwargs)
                if not rv:
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                else:
                    return rv
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

@retry(tries=3, delay=2, backoff=2)
def upload(filename, url):
    """
    Upload a file to a URL
    """
    try:
        with open(filename, 'rb') as file_obj:
            response = requests.put(url, data=file_obj, timeout=120)
    except requests.exceptions.RequestException as exc:
        logging.warning('RequestException when trying to upload file', filename)
        return None
    except IOError:
        logging.warning('IOError when trying to upload file', filename)
        return None

    if response.status_code == 200:
        return True
    return None

def stageout(job_file, path, base_dir):
    """
    Copy any required output files and/or directories to S3 storage
    """
    try:
        with open(job_file, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        return False

    # Upload any output files
    if 'outputFiles' in job:
        for output in job['outputFiles']:
            out_file = glob.glob(output['name'])[0]
            if upload(out_file, output['url']):
                logging.info('Successfully uploaded file %s to cloud storage', out_file)
            else:
                logging.error('Unable to upload file %s to cloud storage', out_file)
                return False

    # Upload any output directories
    if 'outputDirs' in job:
        for output in job['outputDirs']:
            output_filename = os.path.basename(output['name']) + ".tgz"
            try:
                with tarfile.open(output_filename, "w:gz") as tar:
                    tar.add(output['name'])
            except Exception as exc:
                logging.error('Got exception on tar creation for directory %s: %s', output['name'], exc)
                return False
            if upload(output_filename, output['url']):
                logging.info('Successfully uploaded directory %s to cloud storage', output['name'])
            else:
                logging.error('Unable to upload directory %s to cloud storage', output['name'])
                return False
    return True

def get_usage_from_cgroup():
    """
    Read memory usage from cgroup (work in progress!)
    """
    max_usage_in_bytes = -1
    files = glob.glob('/sys/fs/cgroup/memory/htcondor/*/memory.max_usage_in_bytes')
    for file in files:
        with open(file) as cgroup:
            max_usage_in_bytes = int(cgroup.read())
    return max_usage_in_bytes

class ProcessMetrics(object):
    """ 
    Class for storing metrics associated with running a process
    """
    def __init__(self):
        self.exit_code = None
        self.timed_out = False
        self.time_wall = None
        self.time_user = None
        self.time_sys = None
        self.max_rss = None

def monitor(function, *args, **kwargs):
    """
    Monitor CPU, wall time and memory usage of a function which runs a child process
    """
    metrics = ProcessMetrics()

    start_time, start_resources = time.time(), getrusage(RUSAGE_CHILDREN)
    metrics.exit_code, metrics.timed_out = function(*args, **kwargs)
    end_time, end_resources = time.time(), getrusage(RUSAGE_CHILDREN)

    metrics.time_wall = end_time - start_time
    metrics.time_user = end_resources.ru_utime - start_resources.ru_utime
    metrics.time_sys = end_resources.ru_stime - start_resources.ru_stime
    metrics.max_rss = end_resources.ru_maxrss

    return metrics

def get_info():
    """
    Get information to be passed to job
    """
    try:
        with open('/etc/prominence.json', 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.error('Unable to read job info due to %s', ex)
        return {}
    return job
 
def mount_storage(job_file):
    """
    Mount user-specified storage
    """
    try:
        with open(job_file, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        return False

    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        storage_provider = None
        storage_token = None
        if storage_type == 'onedata':
            logging.info('Mounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            process = subprocess.Popen('/usr/bin/oneclient -t %s -H %s %s' % (storage_token,
                                                                              storage_provider,
                                                                              storage_mountpoint),
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.wait()
    return True

def get_storage_mountpoint():
    """
    Get mount point for fuse filesystem from job JSON
    """
    try:
        with open('.job.mapped.json', 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read .job.mapped.json due to %s', ex)
        return None

    if 'storage' in job:
        return job['storage']['mountpoint']

    return None

def download_singularity(image, image_new, location, base_dir):
    """
    Download a Singularity image from a URL or pull an image from Docker Hub
    """
    start = time.time()
    if re.match(r'^http', image):
        try:
            response = requests.get(image, allow_redirects=True, stream=True)
            if response.status_code == 200:
                with open(image_new, 'wb') as file_image:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            file_image.write(chunk)
            else:
                logging.error('Unable to download Singularity image')
                return 1, False
        except requests.exceptions.RequestException as ex:
            logging.error('Unable to download Singularity image due to a RequestException: %s', ex)
            return 1, False
        except IOError as ex:
            logging.error('Unable to download Singularity image due to an IOError: %s', ex)
            return 1, False
    else:
        # We set SINGULARITY_LOCALCACHEDIR & SINGULARITY_TMPDIR in order to avoid Singularity errors
        if not os.path.isdir(base_dir + '/.singularity'):
            try:
                os.mkdir(base_dir + '/.singularity')
            except Exception as ex:
                logging.error('Unable to create .singularity directory due to: %s', ex)
                return 1, False
        if not os.path.isdir(base_dir + '/.tmp'):
            try:
                os.mkdir(base_dir + '/.tmp')
            except Exception as ex:
                logging.error('Unable to create .tmp directory due to: %s', ex)
                return 1, False

        # Handle both Singularity Hub & Docker Hub, with Docker Hub the default
        if re.match(r'^shub:', image):
            cmd = 'singularity pull --name "image.simg" %s' % image
        else:
            cmd = 'singularity pull --name "image.simg" docker://%s' % image

        process = subprocess.Popen(cmd,
                                   cwd=os.path.dirname(image_new),
                                   shell=True,
                                   env=dict(os.environ,
                                            PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                            SINGULARITY_LOCALCACHEDIR='%s/.singularity' % base_dir,
                                            SINGULARITY_TMPDIR='%s/.tmp' % base_dir),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        logging.info('singularity pull stdout: "%s"', stdout)
        logging.info('singularity pull stderr: "%s"', stderr)

        if return_code != 0:
            return 1, False

    logging.info('Time to pull image: %d', time.time() - start)

    return 0, False

def download_udocker(image, location, label, base_dir):
    """
    Download an image from a URL and create a udocker container named 'image'
    """
    # Firstly setup the .udocker directory
    if not os.path.isdir(base_dir + '/.udocker'):
        try:
            os.mkdir(base_dir + '/.udocker')
        except Exception as ex:
            logging.error('Unable to create .udocker directory due to: %s', ex)
            return 1, False

    start = time.time()
    if re.match(r'^http', image):
        # Download tarball
        try:
            response = requests.get(image, allow_redirects=True, stream=True)
            if response.status_code == 200:
                with open('%s/image.tar' % location, 'wb') as tar_file:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            tar_file.write(chunk)
            else:
                logging.error('Unable to download udocker image')
                return 1, False
        except requests.exceptions.RequestException as e:
            logging.error('Unable to download udocker image due to: %s', e)
            return 1, False
        except IOError as e:
            logging.error('Unable to download udocker image due to: %s', e)
            return 1, False

        # Install udocker
        process = subprocess.Popen('udocker install',
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        logging.info('udocker install stdout: "%s"', stdout)
        logging.info('udocker install stderr: "%s"', stderr)

        # Load image
        process = subprocess.Popen('udocker load -i %s/image.tar' % location,
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        logging.info('udocker load stdout: "%s"', stdout)
        logging.info('udocker load stderr: "%s"', stderr)

        if return_code != 0:
            logging.error('Unable to load udocker tarball')
            return 1, False

        # Determine image name
        process = subprocess.Popen('udocker images',
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir),
                                   shell=True,
                                   stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        if return_code != 0:
            logging.error('Unable to determine container image name')
            return 1, False

        image = None
        for line in stdout.split('\n'):
            match_obj_name = re.search(r'([\w\/\.\-\_\:]+)', line)
            if match_obj_name and 'REPOSITORY' not in line:
                image = match_obj_name.group(1)

        if image is None:
            logging.error('No image found')
            return 1, False

        # Delete tarball
        os.unlink('%s/image.tar' % location)
    else:
        # Pull image
        process = subprocess.Popen('udocker pull %s' % image,
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        logging.info('udocker pull stdout: "%s"', stdout)
        logging.info('udocker pull stderr: "%s"', stderr)

        if return_code != 0:
            return 1, False

    # Create container
    process = subprocess.Popen('udocker create --name=image%d %s' % (label, image),
                               env=dict(os.environ,
                                        UDOCKER_DIR='%s/.udocker' % base_dir),
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    logging.info('udocker create stdout: "%s"', stdout)
    logging.info('udocker create stderr: "%s"', stderr)

    if return_code != 0:
        return 1, False

    logging.info('Time to pull image: %d', time.time() - start)

    return 0, False

def run_udocker(image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit):
    """
    Execute a task using udocker
    """
    extras = ''
    if cmd is None:
        cmd = ''
#    else:
#        extras = '--nometa '

    extras += " ".join('--env=%s=%s' % (key, env[key]) for key in env)

    if base_dir == '/mnt/beeond/prominence':
        extras += " -v /mnt/beeond "
    elif base_dir == '/home/prominence':
        extras += " -v /home/prominence "

    mpi_per_node = ''
    if mpi_procs_per_node > 0:
        mpi_per_node = '-N %d' % mpi_procs_per_node

    if mpi == 'openmpi':
        mpi_env = " -x UDOCKER_DIR -x PROMINENCE_PWD -x TMP -x TEMP -x TMPDIR "
        mpi_env += " ".join('-x %s' % key for key in env)
        cmd = ("mpirun --hostfile /home/user/.hosts-openmpi"
               " -np %d"
               " %s"
               " %s"
               " -mca btl_base_warn_component_unused 0"
               " -mca plm_rsh_agent /mnt/beeond/prominence/ssh_container %s") % (mpi_processes, mpi_per_node, mpi_env, cmd)
    elif mpi == 'mpich':
        env_list = ['PROMINENCE_PWD', 'UDOCKER_DIR', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/user/.hosts-mpich"
               " -np %d"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec /mnt/beeond/prominence/ssh_container %s") % (mpi_processes, mpi_env, cmd)

    # Get storage mountpoint
    mountpoint = get_storage_mountpoint()
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '-v %s ' % mountpoint

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' -v %s/%s:%s ' % (path, artifact, artifacts[artifact])

    if base_dir in ('/home/prominence', '/mnt/beeond/prominence'):
        # Used on clouds
        run_command = ("udocker -q run %s"
                       " --env=HOME=%s"
                       " --env=TMP=%s"
                       " --env=TEMP=%s"
                       " --env=TMPDIR=%s"
                       " --env=PROMINENCE_PWD=%s"
                       " --env=UDOCKER_DIR=%s/.udocker"
                       " --hostauth"
                       " --user=user"
                       " --bindhome"
                       " %s"
                       " --workdir=%s"
                       " -v /tmp"
                       " -v /var/tmp"
                       " %s %s") % (extras, path, path, path, path, workdir, base_dir, mounts, workdir, image, cmd)
    else:
        # Used on existing HPC systems
        run_command = ("udocker -q run %s"
                       " --env=HOME=%s"
                       " --env=TMP=%s"
                       " --env=TEMP=%s"
                       " --env=TMPDIR=%s"
                       " --hostauth"
                       " --user=%s"
                       " --bindhome"
                       " %s"
                       " -v %s"
                       " --workdir=%s"
                       " -v /tmp"
                       " -v /var/tmp"
                       " %s %s") % (extras, path, path, path, path, getpass.getuser(), mounts, path, workdir, image, cmd)

    job_cpus = -1
    job_memory = -1
    num_retries = 0
    job_info = get_info()
    if 'cpus' in job_info:
        job_cpus = job_info['cpus']
    if 'memory' in job_info:
        job_memory = job_info['memory']

    logging.info('Running: "%s"', run_command)

    start = time.time()
    return_code, timed_out = run_with_timeout(run_command,
                                              dict(os.environ,
                                                   UDOCKER_DIR='%s/.udocker' % base_dir,
                                                   PROMINENCE_CPUS='%d' % job_cpus,
                                                   PROMINENCE_MEMORY='%d' % job_memory),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_singularity(image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit):
    """
    Execute a task using Singularity
    """
    mpi_per_node = ''
    if mpi_procs_per_node > 0:
        mpi_per_node = '-N %d' % mpi_procs_per_node

    if mpi == 'openmpi':
        mpi_env = " -x PROMINENCE_CONTAINER_LOCATION -x PROMINENCE_PWD -x HOME -x TEMP -x TMP "
        mpi_env += " ".join('-x %s' % key for key in env)
        cmd = ("mpirun --hostfile /home/user/.hosts-openmpi"
               " -np %d"
               " %s"
               " %s"
               " -mca btl_base_warn_component_unused 0"
               " -mca plm_rsh_agent /mnt/beeond/prominence/ssh_container %s") % (mpi_processes, mpi_per_node, mpi_env, cmd)
    elif mpi == 'mpich':
        env_list = ['PROMINENCE_CONTAINER_LOCATION', 'PROMINENCE_PWD', 'HOME', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/user/.hosts-mpich"
               " -np %d"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec /mnt/beeond/prominence/ssh_container %s") % (mpi_processes, mpi_env, cmd)

    command = 'exec'
    if cmd is None:
        cmd = ''
        command = 'run'

    # Artifact mounts
    mounts = ''
    for artifact in artifacts:
        mounts = mounts + ' --bind %s/%s:%s ' % (path, artifact, artifacts[artifact])

    if base_dir in ('/home/prominence', '/mnt/beeond/prominence'):
        run_command = ("singularity %s"
                       " --no-home"
                       " --bind /home"
                       " --bind /mnt"
                       " --home %s"
                       " %s"
                       " --pwd %s %s %s") % (command, path, mounts, workdir, image, cmd)
    else:
        run_command = 'singularity %s --home %s %s --pwd %s %s %s' % (command, path, mounts, workdir, image, cmd)

    job_cpus = -1
    job_memory = -1
    num_retries = 0
    job_info = get_info()
    if 'cpus' in job_info:
        job_cpus = job_info['cpus']
    if 'memory' in job_info:
        job_memory = job_info['memory']

    logging.info('Running: "%s"', run_command)

    start = time.time()
    return_code, timed_out = run_with_timeout(run_command,
                                              dict(env,
                                                   PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                                   TMP='%s' % path,
                                                   TEMP='%s' % path,
                                                   TMPDIR='%s' % path,
                                                   PROMINENCE_CONTAINER_LOCATION='%s' % os.path.dirname(image),
                                                   PROMINENCE_PWD='%s' % workdir,
                                                   PROMINENCE_CPUS='%d' % job_cpus,
                                                   PROMINENCE_MEMORY='%d' % job_memory),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_tasks(job_file, path, base_dir, is_batch):
    """
    Execute sequential tasks
    """
    try:
        with open(job_file, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        return False

    num_retries = 0
    if 'numberOfRetries' in job:
        num_retries = job['numberOfRetries']

    # Number of nodes
    if 'nodes' in job['resources']:
        num_nodes = job['resources']['nodes']
    else:
        num_nodes = 1

    # Number of CPUs
    num_cpus = job['resources']['cpus']

    # MPI processes
    mpi_processes = num_cpus*num_nodes

    # Walltime limit
    walltime_limit = 12*60*60
    if 'walltime' in job['resources']:
        walltime_limit = job['resources']['walltime']*60

    # Artifact mounts
    artifacts = {}
    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if 'mountpoint' in artifact:
                source = artifact['mountpoint'].split(':')[0]
                dest = artifact['mountpoint'].split(':')[1]
                artifacts[source] = dest

    count = 0
    tasks_u = []
    success = True
    job_start_time = time.time()
    total_pull_time = 0

    for task in job['tasks']:
        logging.info('Working on task %d', count)

        image = task['image']

        cmd = None
        if 'cmd' in task:
            cmd = task['cmd']

        workdir = None
        if 'workdir' in task:
            workdir = task['workdir']

        if workdir is None:
            workdir = path
        elif not workdir.startswith('/'):
            workdir = path + '/' + workdir

        env = {}
        if 'env' in task:
            env = task['env']

        if args.param:
            for pair in args.param:
                key = pair.split('=')[0]
                value = pair.split('=')[1]
                env['PROMINENCE_PARAMETER_%s' % key] = value
                cmd = Template(cmd).safe_substitute({key:value})

        location = '%s/%d' % (base_dir, count)
        try:
            os.mkdir(location)
        except Exception as err:
            logging.error('Unable to create directory %s', location)
            return False

        mpi = None
        if 'type' in task:
            if task['type'] == 'openmpi':
                mpi = 'openmpi'
            elif task['type'] == 'mpich':
                mpi = 'mpich'

        if 'procsPerNode' in task:
            procs_per_node = task['procsPerNode']
        else:
            procs_per_node = 0
 
        if procs_per_node > 0:
            mpi_processes = procs_per_node*num_nodes

        metrics_download = ProcessMetrics()
        metrics_task = ProcessMetrics()

        retry_count = 0
        task_was_run = False
        image_pull_status = 'completed'

        # Check if a previous task used the same image: in that case use the previous image if the same container
        # runtime was used
        image_count = 0
        found_image = False
        for task_check in job['tasks']:
            if image == task_check['image'] and image_count < count and task['runtime'] == task_check['runtime']:
                found_image = True
                logging.info('Will use cached image from task %d for this task', image_count)
                break
            image_count += 1

        if task['runtime'] == 'udocker':
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image = 'image%d' % image_count
                image_pull_status = 'cached'
            elif not FINISH_NOW:
                logging.info('Pulling image for task')
                metrics_download = monitor(download_udocker, image, location, count, base_dir)
                if metrics_download.time_wall > 0:
                    total_pull_time += metrics_download.time_wall
                if metrics_download.exit_code != 0:
                    logging.error('Unable to pull image')
                    image_pull_status = 'failed'
                else:
                    image = 'image%d' % count
            # Run task
            if (found_image or metrics_download.exit_code == 0) and not FINISH_NOW:
                task_was_run = True
                while metrics_task.exit_code != 0 and retry_count < num_retries + 1 and not metrics_task.timed_out and not FINISH_NOW:
                    logging.info('Running task, attempt %d', retry_count + 1)
                    task_time_limit = walltime_limit - (time.time() - job_start_time) + total_pull_time
                    metrics_task = monitor(run_udocker, image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, procs_per_node, artifacts, task_time_limit)
                    retry_count += 1
        else:
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image_new = '%s/%d/image.simg' % (base_dir, image_count)
                image_pull_status = 'cached'
            elif not FINISH_NOW:
                image_new = '%s/image.simg' % location
                logging.info('Pulling image for task')
                metrics_download = monitor(download_singularity, image, image_new, location, base_dir)
                if metrics_download.time_wall > 0:
                    total_pull_time += metrics_download.time_wall
                if metrics_download.exit_code != 0:
                    logging.error('Unable to pull image')
                    image_pull_status = 'failed'
            # Run task
            if (found_image or metrics_download.exit_code == 0) and not FINISH_NOW:
                task_was_run = True
                while metrics_task.exit_code != 0 and retry_count < num_retries + 1 and not metrics_task.timed_out and not FINISH_NOW:
                    logging.info('Running task, attempt %d', retry_count + 1)
                    task_time_limit = walltime_limit - (time.time() - job_start_time) + total_pull_time
                    metrics_task = monitor(run_singularity, image_new, cmd, workdir, env, path, base_dir, mpi, mpi_processes, procs_per_node, artifacts, task_time_limit)
                    retry_count += 1

        task_u = {}
        task_u['imagePullStatus'] = image_pull_status
        task_u['imagePullTime'] = metrics_download.time_wall
        if task_was_run:
            task_u['exitCode'] = metrics_task.exit_code
            task_u['wallTimeUsage'] = metrics_task.time_wall
            task_u['maxResidentSetSizeKB'] = metrics_task.max_rss
            task_u['retries'] = retry_count - 1
            if metrics_task.time_user > -1 and metrics_task.time_sys > -1:
                task_u['cpuTimeUsage'] = metrics_task.time_user + metrics_task.time_sys
        tasks_u.append(task_u)

        count += 1

        if metrics_task.exit_code != 0 or metrics_task.timed_out or FINISH_NOW:
            success = False
            break

    if FINISH_NOW:
        logging.info('Received signal, aborting')

    # Get overall max memory usage
    max_usage_in_bytes = get_usage_from_cgroup()
    if max_usage_in_bytes > -1:
        task_u = {}
        task_u['maxMemoryUsageKB'] = max_usage_in_bytes/1000
        tasks_u.append(task_u)

    # Handle timeout
    if metrics_task.timed_out:
        task_u = {}
        task_u['error'] = 'WallTimeLimitExceeded'
        tasks_u.append(task_u)

    # Write json job details
    promlet_json = 'promlet.json'
    if args.param:
        promlet_json = 'promlet.%d.json' % args.id
    try:
        with open(promlet_json, 'w') as file:
            json.dump(tasks_u, file)
    except Exception as exc:
        logging.critical('Unable to write promlet.json due to: %s', exc)

    return success

def create_parser():
    """
    Create the arguments parser
    """
    parser = argparse.ArgumentParser(description='promlet')
    parser.add_argument('--batch',
                        dest='batch',
                        default=False,
                        action='store_true',
                        help='Running on a batch system')
    parser.add_argument('--job',
                        dest='job',
                        help='JSON job description file')
    parser.add_argument('--id',
                        dest='id',
                        default=0,
                        type=int,
                        help='Id for this job')
    parser.add_argument('--param',
                        dest='param',
                        action='append',
                        help='Parameters for the job')

    return parser.parse_args()

if __name__ == "__main__":
    # Extract arguments from the command line
    args = create_parser()

    # Handle signals
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Initial directory
    path = os.getcwd()
    base_dir = '/home/prominence'

    # Setup logging
    logging.basicConfig(filename='%s/promlet.%d.log' % (path, args.id), level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info('Started promlet using path "%s"' % path)

    # Handle BeeOND
    if not os.path.isdir(base_dir):
        if os.path.isdir('/mnt/beeond/prominence'):
            base_dir = '/mnt/beeond/prominence'

    # Handle HPC systems
    if args.batch or (not os.path.isdir('/home/prominence') and not os.path.isdir('/mnt/beeond/prominence')):
        base_dir = os.path.join(path, 'prominence')
        os.mkdir(base_dir)
        batch = True

    # Write empty json job details, so no matter what happens next, at least an empty file exists
    try:
        with open('promlet.json', 'w') as file:
            json.dump({}, file)
    except Exception as exc:
        logging.critical('Unable to write promlet.json due to: %s', exc)
        exit(1)

    # Check if we have been run before
    if os.path.isfile('.lock'):
        logging.critical('Lock file detected - promlet is being re-run, exiting...')
        exit(1)

    # Create a lock file
    try:
        open('.lock', 'a').close()
    except Exception as exc:
        logging.critical('Unable to write lock file, exiting...')
        exit(1)

    # Mount user-specified storage if necessary
    mount_storage(args.job)

    # Run tasks
    success_tasks = run_tasks(args.job, path, base_dir, args.batch)

    # Upload output files if necessary
    success_stageout = stageout(args.job, path, base_dir)

    # Return appropriate exit code - necessary for retries of DAG nodes
    if not success_tasks or not success_stageout:
        logging.info('Exiting promlet with failure')
        exit(1)

    logging.info('Exiting promlet with success')
    exit(0)

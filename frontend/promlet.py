#!/usr/bin/python
from __future__ import print_function
import argparse
import getpass
import glob
import json
import logging
import os
import re
import subprocess
import sys
import tarfile
import time
from functools import wraps
from resource import getrusage, RUSAGE_CHILDREN
import requests

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

def monitor(function, *args, **kwargs):
    """
    Monitor CPU and wall time usage of a function which runs a child process
    """
    start_time, start_resources = time.time(), getrusage(RUSAGE_CHILDREN)
    exit_code = function(*args, **kwargs)
    end_time, end_resources = time.time(), getrusage(RUSAGE_CHILDREN)

    time_real = end_time - start_time
    time_user = end_resources.ru_utime - start_resources.ru_utime
    time_sys = end_resources.ru_stime - start_resources.ru_stime

    return (exit_code, time_real, time_user, time_sys, end_resources.ru_maxrss)

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

def eprint(*args, **kwargs):
    """
    Print to stderr
    """
    print(*args, file=sys.stderr, **kwargs)

def update_classad(attr, value):
    """
    Update the job's ClassAd if possible
    """
    # condor_chirp is installed in different places on CentOS & Ubuntu
    cmds = ['/usr/libexec/condor/condor_chirp', '/usr/lib/condor/libexec/condor_chirp']
    for cmd in cmds:
        if os.path.isfile(cmd):
            process = subprocess.Popen("%s set_job_attr '%s' '%d'" % (cmd, attr, value),
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.wait()
            return
    return

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
                return 1
        except requests.exceptions.RequestException as ex:
            logging.error('Unable to download Singularity image due to a RequestException: %s', ex)
            return 1
        except IOError as ex:
            logging.error('Unable to download Singularity image due to an IOError: %s', ex)
            return 1
    else:
        # We set SINGULARITY_LOCALCACHEDIR & SINGULARITY_TMPDIR in order to avoid Singularity errors
        if not os.path.isdir(base_dir + '/.singularity'):
            try:
                os.mkdir(base_dir + '/.singularity')
            except Exception as ex:
                logging.error('Unable to create .singularity directory due to: %s', ex)
                return 1
        if not os.path.isdir(base_dir + '/.tmp'):
            try:
                os.mkdir(base_dir + '/.tmp')
            except Exception as ex:
                logging.error('Unable to create .tmp directory due to: %s', ex)
                return 1

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
            return 1

    update_classad('ProminenceImagePullTime', time.time() - start)
    logging.info('Time to pull image: %d', time.time() - start)

    return 0

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
            return 1

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
                return 1
        except requests.exceptions.RequestException as e:
            logging.error('Unable to download udocker image due to: %s', e)
            return 1
        except IOError as e:
            logging.error('Unable to download udocker image due to: %s', e)
            return 1

        # Load image
        process = subprocess.Popen('udocker load -i %s/image.tar' % location,
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir),
                                   shell=True,
                                   stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        logging.info('udocker load stdout: "%s"', stdout)
        logging.info('udocker load stderr: "%s"', stderr)

        if return_code != 0:
            logging.error('Unable to load udocker tarball')
            return 1

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
            return 1

        image = None
        for line in stdout.split('\n'):
            match_obj_name = re.search(r'([\w\/\.\-\_\:]+)', line)
            if match_obj_name and 'REPOSITORY' not in line:
                image = match_obj_name.group(1)

        if image is None:
            logging.error('No image found')
            return 1

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
            return 1

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
        return 1

    update_classad('ProminenceImagePullTime', time.time() - start)
    logging.info('Time to pull image: %d', time.time() - start)

    return 0

def run_udocker(image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, mpi_procs_per_node, artifacts):
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
    try:
        process = subprocess.Popen(run_command,
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % base_dir,
                                            PROMINENCE_CPUS='%d' % job_cpus,
                                            PROMINENCE_MEMORY='%d' % job_memory),
                                   shell=True,
                                   stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode
    except Exception as err:
        logging.error('Exception running udocker: ', err)
    logging.info('Task had exit code %d', return_code)

    update_classad('ProminenceExecuteTime', time.time() - start)
    update_classad('ProminenceExitCode', return_code)

    if stdout is not None:
        print(stdout)
    if stderr is not None:
        eprint(stderr)

    return return_code

def run_singularity(image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, mpi_procs_per_node, artifacts):
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
    process = subprocess.Popen(run_command,
                               env=dict(env,
                                        PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                        TMP='%s' % path,
                                        TEMP='%s' % path,
                                        TMPDIR='%s' % path,
                                        PROMINENCE_CONTAINER_LOCATION='%s' % os.path.dirname(image),
                                        PROMINENCE_PWD='%s' % workdir,
                                        PROMINENCE_CPUS='%d' % job_cpus,
                                        PROMINENCE_MEMORY='%d' % job_memory),
                               shell=True,
                               stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    logging.info('Task had exit code %d', return_code)

    update_classad('ProminenceExecuteTime', time.time() - start)
    update_classad('ProminenceExitCode', return_code)

    if stdout is not None:
        print(stdout)
    if stderr is not None:
        eprint(stderr)

    return return_code

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

    # Create directory which will be the home directory inside the container
    #try:
    #    os.mkdir(base_dir + '/myhome')
    #except Exception as ex:
    #    logging.error('Unable to create the myhome directory due to: %s', ex)
    #    return False

    # Number of nodes
    if 'nodes' in job['resources']:
        num_nodes = job['resources']['nodes']
    else:
        num_nodes = 1

    # Number of CPUs
    num_cpus = job['resources']['cpus']

    # MPI processes
    mpi_processes = num_cpus*num_nodes

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

        exit_code = 1
        download_exit_code = -1
        retry_count = 0
        image_pull_time = -1
        time_real = -1
        time_user = -1
        time_sys = -1
        max_rss = -1
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
            else:
                logging.info('Pulling image for task')
                (download_exit_code, image_pull_time, _, _, _) = monitor(download_udocker, image, location, count, base_dir)
                if download_exit_code != 0:
                    update_classad('ProminenceImagePullSuccess', 1)
                    logging.error('Unable to pull image')
                    image_pull_status = 'failed'
                else:
                    image = 'image%d' % count
            # Run task
            if found_image or download_exit_code == 0:
                task_was_run = True
                while exit_code != 0 and retry_count < num_retries + 1:
                    logging.info('Running task, attempt %d', retry_count + 1)
                    (exit_code, time_real, time_user, time_sys, max_rss) = monitor(run_udocker, image, cmd, workdir, env, path, base_dir, mpi, mpi_processes, procs_per_node, artifacts)
                    logging.info('Resources real: %d, user: %d, sys: %d, maxrss: %d', time_real, time_user, time_sys, max_rss)
                    retry_count += 1
            else:
                # If we didn't try running the task, set exit code to fill value
                exit_code = -1
        else:
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image_new = '%s/%d/image.simg' % (base_dir, image_count)
                image_pull_status = 'cached'
            else:
                image_new = '%s/image.simg' % location
                logging.info('Pulling image for task')
                (download_exit_code, image_pull_time, _, _, _) = monitor(download_singularity, image, image_new, location, base_dir)
                if download_exit_code != 0:
                    update_classad('ProminenceImagePullSuccess', 1)
                    logging.error('Unable to pull image')
                    image_pull_status = 'failed'
            if found_image or download_exit_code == 0:
                task_was_run = True
                while exit_code != 0 and retry_count < num_retries + 1:
                    (exit_code, time_real, time_user, time_sys, max_rss) = monitor(run_singularity, image_new, cmd, workdir, env, path, base_dir, mpi, mpi_processes, procs_per_node, artifacts)
                    logging.info('Resources real: %d, user: %d, sys: %d, maxrss: %d', time_real, time_user, time_sys, max_rss)
                    retry_count += 1
            else:
                # If we didn't try running the task, set exit code to fill value
                exit_code = -1

        task_u = {}
        task_u['imagePullStatus'] = image_pull_status
        task_u['imagePullTime'] = image_pull_time
        if task_was_run:
            task_u['exitCode'] = exit_code
            task_u['wallTimeUsage'] = time_real
            task_u['maxResidentSetSizeKB'] = max_rss
            task_u['retries'] = retry_count - 1
            if time_user > -1 and time_sys > -1:
                task_u['cpuTimeUsage'] = time_user + time_sys
        tasks_u.append(task_u)

        count += 1

        if exit_code != 0:
            success = False
            break

    # Get overall max memory usage
    max_usage_in_bytes = get_usage_from_cgroup()
    if max_usage_in_bytes > -1:
        task_u = {}
        task_u['maxMemoryUsageKB'] = max_usage_in_bytes/1000
        tasks_u.append(task_u)

    # Write json job details
    try:
        with open('promlet.json', 'w') as file:
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

    return parser.parse_args()

if __name__ == "__main__":
    # Extract arguments from the command line
    args = create_parser()

    # Initial directory
    path = os.getcwd()
    base_dir = '/home/prominence'

    # Setup logging
    logging.basicConfig(filename='%s/promlet.log' % path, level=logging.INFO, format='%(asctime)s %(message)s')
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


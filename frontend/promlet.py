#!/usr/bin/python
from __future__ import print_function
import argparse
import getpass
import glob
import json
import logging
import os
import posixpath
import re
import shlex
import shutil
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

try:
    from urlparse import urlsplit
    from urllib import unquote
except ImportError: # Python 3
    from urllib.parse import urlsplit, unquote

CURRENT_SUBPROCS = set()
FINISH_NOW = False

def replace_output_urls(job, outfiles, outdirs):
    """
    Replace output file & directory URLs if necessary
    """
    logging.info('Checking if we need to update any output file & directories URLs')

    if outfiles and 'outputFiles' in job:
        for outfile in outfiles:
            pieces = outfile.split('=', 1)
            filename = pieces[0]
            url = pieces[1]
            for i in range(0, len(job['outputFiles'])):
                pair = job['outputFiles'][i]
                if filename == pair['name']:
                    logging.info('Updating URL for file %s with %s', filename, url)
                    job['outputFiles'][i]['url'] = url

        if outdirs and 'outputDirs' in job:
            for outdir in outdirs:
                pieces = outdir.split('=', 1)
                filename = pieces[0]
                url = pieces[1]
                for i in range(0, len(job['outputDirs'])):
                    pair = job['outputDirs'][i]
                    if filename == pair['name']:
                        logging.info('Updating URL for dir %s with %s', filename, url)
                        job['outputDirs'][i]['url'] = url

def image_name(image):
    """
    Normalise image names
    """
    if image.startswith('http'):
        return url2filename(image)
    return image

def url2filename(url):
    """
    Return basename corresponding to a URL
    """
    urlpath = urlsplit(url).path
    basename = posixpath.basename(unquote(urlpath))
    if (os.path.basename(basename) != basename or unquote(posixpath.basename(urlpath)) != basename):
        raise ValueError
    return basename

def check_beeond():
    """
    Ensure each host in a multi-node job has BeeGFS mounted
    """
    beeond_valid_hosts = 0
    total_hosts = 0
    with open('/home/user/.hosts-openmpi', 'r') as hosts:
        for mpi_host in hosts.readlines():
            host = mpi_host.split(' ')[0]
            process = subprocess.Popen('ssh -i /home/user/.ssh/id_rsa -o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null %s "df -h | grep beeond"' % host, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = process.communicate()[0]
            if 'beegfs_ondemand' in output:
                beeond_valid_hosts += 1
            total_hosts += 1

    if beeond_valid_hosts != total_hosts:
        return False
    return True

def create_mpi_files(path):
    """
    Create MPI hosts file if necessary
    """
    if 'PBS_NODEFILE' in os.environ:
        logging.info('Environment variable PBS_NODEFILE detected')
        try:
            with open(os.environ['PBS_NODEFILE'], 'r') as pbs_nodefile:
                pbs_lines = pbs_nodefile.readlines()
        except IOError as exc:
            logging.critical('Unable to open PBS_NODEFILE due to: %s', exc)
            return False
  
        try:
            with open(os.path.join(path, '.hosts-openmpi'), 'w') as mpi_file:
                for line in pbs_lines:
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write MPI hosts file')
            return False

        return True

    elif 'SLURM_JOB_NODELIST' in os.environ:
        logging.info('Environment variable SLURM_JOB_NODELIST detected')
        process = subprocess.Popen('scontrol show hostnames $SLURM_JOB_NODELIST',
                                   env=os.environ,
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
    
        try:
            with open(os.path.join(path, '.hosts-openmpi'), 'w') as mpi_file:
                for line in stdout.split('/n'):
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write MPI hosts file')
            return False

        return True

    return False

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
        logging.warning('RequestException when trying to upload file %s', filename)
        return None
    except IOError:
        logging.warning('IOError when trying to upload file %s', filename)
        return None

    if response.status_code == 200:
        return True
    return None

def stageout(job, path):
    """
    Copy any required output files and/or directories to S3 storage
    """
    success = True

    # Upload any output files
    json_out_files = []
    if 'outputFiles' in job:
        for output in job['outputFiles']:
            json_out_file = {'name':output['name']}
            out_files = glob.glob(output['name'])
            if out_files: 
                out_file = out_files[0]
            else:
                logging.error('Output file %s does not exist', output['name'])
                json_out_file['status'] = 'failedNoSuchFile'
                out_file = None   
                success = False
            if out_file:
                if upload(out_file, output['url']):
                    logging.info('Successfully uploaded file %s to cloud storage', out_file)
                    json_out_file['status'] = 'success'
                else:
                    logging.error('Unable to upload file %s to cloud storage', out_file)
                    json_out_file['status'] = 'failedUpload'
                    success = False
            json_out_files.append(json_out_file)

    # Upload any output directories
    json_out_dirs = []
    if 'outputDirs' in job:
        for output in job['outputDirs']:
            tar_file_created = True
            output_filename = os.path.basename(output['name']) + ".tgz"
            json_out_dir = {'name':output['name']}
            try:
                with tarfile.open(output_filename, "w:gz") as tar:
                    tar.add(output['name'])
            except Exception as exc:
                logging.error('Got exception on tar creation for directory %s: %s', output['name'], exc)
                json_out_dir['status'] = 'failedTarCreation'
                success = False
                tar_file_created = False
            if tar_file_created and os.path.isfile(output_filename):
                if upload(output_filename, output['url']):
                    logging.info('Successfully uploaded directory %s to cloud storage', output['name'])
                    json_out_dir['status'] = 'success'
                else:
                    logging.error('Unable to upload directory %s to cloud storage', output['name'])
                    json_out_dir['status'] = 'failedUpload'
                    success = False
            json_out_dirs.append(json_out_dir)

    return success, {'files': json_out_files, 'directories': json_out_dirs}

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
 
def mount_storage(job):
    """
    Mount user-specified storage
    """
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

def unmount_storage(job):
    """
    Unmount user-specified storage
    """
    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        storage_provider = None
        storage_token = None
        if storage_type == 'onedata':
            logging.info('Mounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            process = subprocess.Popen('/usr/bin/oneclient -t %s -H %s -u %s' % (storage_token,
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

def download_singularity(image, image_new, location, path):
    """
    Download a Singularity image from a URL or pull an image from Docker Hub
    """
    logging.info('Pulling Singularity image for task')

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

        logging.info('Singularity image downloaded from URL and written to file %s', image_new)
    else:
        # Handle both Singularity Hub & Docker Hub, with Docker Hub the default
        if re.match(r'^shub:', image):
            cmd = 'singularity pull --name "image.simg" %s' % image
        else:
            cmd = 'singularity pull --name "image.simg" docker://%s' % image

        process = subprocess.Popen(cmd,
                                   cwd=os.path.dirname(image_new),
                                   shell=True,
                                   env=dict(os.environ,
                                            PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin'),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        if return_code != 0:
            logging.error(stdout)
            logging.error(stderr)
            return 1, False

    if os.path.exists(image_new):
        logging.info('Image file %s exists', image_new)
    else:
        logging.info('Image file %s does not exist', image_new)

    return 0, False

def get_udocker(path):
    """
    Check if udocker is installed
    """
    if os.path.exists('/home/user/.udocker/bin/proot-x86_64'):
        logging.info('Found existing udocker installation in /home/user')
        return '/home/user'
    else:
        if install_udocker(path):
            return path

    return None

def install_udocker(location):
    """
    Install  udocker if necessary
    """
    if not os.path.exists('%s/.udocker/bin/proot-x86_64' % location):
        logging.info('Installing udockertools')

        attempt = 0
        installed = False
        while not installed and attempt < 10:
            # Firstly setup the .udocker directory if necessary
            if not os.path.isdir(location + '/.udocker'):
                try:
                    os.mkdir(location + '/.udocker')
                except Exception as ex:
                    logging.error('Unable to create .udocker directory due to: %s', ex)
                    return False

            # Install udocker if necessary
            process = subprocess.Popen('udocker install',
                                       env=dict(os.environ,
                                                UDOCKER_DIR='%s/.udocker' % location),
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            output = process.communicate()[0]
            return_code = process.returncode

            if 'Error: installation of udockertools failed' in output or return_code != 0:
                logging.error('Installation of udockertools failed')
            else:
                logging.info('udockertools installation successful')
                installed = True
            attempt += 1

    else:
        logging.info('Found existing udocker installation')
        installed = True

    if not installed:
        return False

    return True

def download_udocker(image, location, label, path):
    """
    Download an image from a URL and create a udocker container named 'image'
    """
    logging.info('Pulling udocker image for task')

    udocker_location = get_udocker(path)
    if not udocker_location:
        logging.error('Unable to install udockertools')
        return 1, False

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

        logging.info('udocker tarball downloaded from URL and written to file %s/image.tar' % location)

        # Load image
        process = subprocess.Popen('udocker load -i %s/image.tar' % location,
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % udocker_location),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        if return_code != 0:
            logging.error('Unable to load udocker tarball')
            logging.error(stdout)
            logging.error(stderr)
            return 1, False

        # Determine image name
        process = subprocess.Popen('udocker images',
                                   env=dict(os.environ,
                                            UDOCKER_DIR='%s/.udocker' % udocker_location),
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
                                            UDOCKER_DIR='%s/.udocker' % udocker_location),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        if return_code != 0:
            return 1, False

    # Create container
    process = subprocess.Popen('udocker create --name=image%d %s' % (label, image),
                               env=dict(os.environ,
                                        UDOCKER_DIR='%s/.udocker' % udocker_location),
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    if return_code != 0:
        return 1, False

    return 0, False

def run_udocker(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit, is_batch):
    """
    Execute a task using udocker
    """
    udocker_location = get_udocker(path)
    if not udocker_location:
        logging.error('Unable to install udockertools')
        return 1, False

    extras = ''
    if cmd is None:
        cmd = ''
#    else:
#        extras = '--nometa '

    if os.path.isdir('/mnt/beeond/prominence'):
        extras += " -v /mnt/beeond "
    elif os.path.isdir('/home/prominence'):
        extras += " -v /home/prominence "

    extras += " ".join('--env=%s=%s' % (key, env[key]) for key in env)

    job_info = get_info()
    if 'cpus' in job_info:
        extras += " --env=PROMINENCE_CPUS=%d" % job_info['cpus']
    if 'memory' in job_info:
        extras += " --env=PROMINENCE_MEMORY=%d" % job_info['memory']

    mpi_per_node = ''

    mpi_ssh = '/mnt/beeond/prominence/ssh_container'
    if '_PROMINENCE_SSH_CONTAINER' in os.environ:
        mpi_ssh = os.environ['_PROMINENCE_SSH_CONTAINER']

    mpi_hosts = '/home/user'
    if create_mpi_files(path):
        mpi_hosts = path

    if mpi == 'openmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-N %d --bind-to none' % mpi_procs_per_node
        mpi_env = " -x UDOCKER_DIR -x PROMINENCE_PWD -x TMP -x TEMP -x TMPDIR "
        mpi_env += " ".join('-x %s' % key for key in env)
        cmd = ("mpirun --hostfile %s/.hosts-openmpi"
               " -np %d"
               " %s"
               " %s"
               " -mca btl_base_warn_component_unused 0"
               " -mca plm_rsh_agent %s %s") % (mpi_hosts, mpi_processes, mpi_per_node, mpi_env, mpi_ssh, cmd)
    elif mpi == 'intelmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-N %d' % mpi_procs_per_node
        env_list = ['PROMINENCE_PWD', 'UDOCKER_DIR', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -machine /home/user/.hosts-mpich"
               " -np %d"
               " %s"
               " -envlist %s"
               " -bootstrap-exec %s %s") % (mpi_processes, mpi_per_node, mpi_env, mpi_ssh, cmd)
    elif mpi == 'mpich':
        env_list = ['PROMINENCE_PWD', 'UDOCKER_DIR', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/user/.hosts-mpich"
               " -np %d"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec %s %s") % (mpi_processes, mpi_env, mpi_ssh, cmd)

    # Get storage mountpoint
    mountpoint = get_storage_mountpoint()
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '-v %s ' % mountpoint

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'
    if os.path.isdir('/home/user/tmp'):
        user_tmp_dir = '/home/user/tmp'

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' -v %s/%s:%s ' % (path, artifact, artifacts[artifact])

    if not is_batch:
        # Used on clouds
        run_command = ("/usr/local/bin/udocker -q run %s"
                       " --env=HOME=%s"
                       " --env=USER=user"
                       " --env=TMP=/tmp"
                       " --env=TEMP=/tmp"
                       " --env=TMPDIR=/tmp"
                       " --env=PROMINENCE_PWD=%s"
                       " --env=UDOCKER_DIR=%s/.udocker"
                       " --env=PROMINENCE_CONTAINER_RUNTIME=udocker"
                       " --hostauth"
                       " --user=user"
                       " --bindhome"
                       " %s"
                       " --workdir=%s"
                       " -v %s:/tmp"
                       " %s %s") % (extras, path, workdir, path, mounts, workdir, user_tmp_dir, image, cmd)
    else:
        # Used on existing HPC systems
        run_command = ("udocker -q run %s"
                       " --env=HOME=%s"
                       " --env=TMP=/tmp"
                       " --env=TEMP=/tmp"
                       " --env=TMPDIR=/tmp"
                       " --env=PROMINENCE_PWD=%s"
                       " --env=UDOCKER_DIR=%s/.udocker"
                       " --env=PROMINENCE_CONTAINER_RUNTIME=udocker"
                       " --hostauth"
                       " --user=%s"
                       " --env=USER=%s"
                       " --bindhome"
                       " %s"
                       " -v %s"
                       " --workdir=%s"
                       " -v %s:/tmp"
                       " %s %s") % (extras, path, workdir, path, getpass.getuser(), getpass.getuser(), mounts, path, workdir, user_tmp_dir, image, cmd)

    logging.info('Running: "%s"', run_command)

    return_code, timed_out = run_with_timeout(run_command,
                                              dict(os.environ,
                                                   UDOCKER_DIR='%s/.udocker' % udocker_location),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_singularity(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit, is_batch):
    """
    Execute a task using Singularity
    """
    mpi_ssh = '/mnt/beeond/prominence/ssh_container'
    if '_PROMINENCE_SSH_CONTAINER' in os.environ:
        mpi_ssh = os.environ['_PROMINENCE_SSH_CONTAINER']

    mpi_hosts = '/home/user'
    if create_mpi_files(path):
        mpi_hosts = path

    mpi_per_node = ''
    if mpi == 'openmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-N %d --bind-to none' % mpi_procs_per_node
        mpi_env = " -x PROMINENCE_CONTAINER_LOCATION -x PROMINENCE_PWD -x HOME -x TEMP -x TMP -x TMPDIR "
        mpi_env += " ".join('-x %s' % key for key in env)
        cmd = ("mpirun --hostfile %s/.hosts-openmpi"
               " -np %d"
               " %s"
               " %s"
               " -mca btl_base_warn_component_unused 0"
               " -mca plm_rsh_no_tree_spawn 1"
               " -mca plm_rsh_agent %s %s") % (mpi_hosts, mpi_processes, mpi_per_node, mpi_env, mpi_ssh, cmd)
    elif mpi == 'intelmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-perhost %d' % mpi_procs_per_node
        env_list = ['PROMINENCE_CONTAINER_LOCATION', 'PROMINENCE_PWD', 'HOME', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -machine /home/user/.hosts-mpich"
               " -np %d"
               " %s"
               " -envlist %s"
               " -bootstrap-exec %s %s") % (mpi_processes, mpi_per_node, mpi_env, mpi_ssh, cmd)
    elif mpi == 'mpich':
        env_list = ['PROMINENCE_CONTAINER_LOCATION', 'PROMINENCE_PWD', 'HOME', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/user/.hosts-mpich"
               " -np %d"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec %s %s") % (mpi_processes, mpi_env, mpi_ssh, cmd)

    command = 'exec'
    if cmd is None:
        cmd = ''
        command = 'run'

    # Get storage mountpoint
    mountpoint = get_storage_mountpoint()
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '--bind %s ' % mountpoint

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' --bind %s/%s:%s ' % (path, artifact, artifacts[artifact])

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'
    if os.path.isdir('/home/user/tmp'):
        user_tmp_dir = '/home/user/tmp'

    if not is_batch:
        run_command = ("singularity %s"
                       " --no-home"
                       " --bind /home"
                       " --bind /mnt"
                       " --home %s"
                       " --bind %s:/tmp"
                       " %s"
                       " --pwd %s %s %s") % (command, path, user_tmp_dir, mounts, workdir, image, cmd)
    else:
        run_command = 'singularity %s --home %s %s --pwd %s --bind %s:/tmp %s %s' % (command, path, mounts, workdir, user_tmp_dir, image, cmd)

    job_cpus = -1
    job_memory = -1
    num_retries = 0
    job_info = get_info()
    if 'cpus' in job_info:
        job_cpus = job_info['cpus']
    if 'memory' in job_info:
        job_memory = job_info['memory']

    logging.info('Running: "%s"', run_command)

    return_code, timed_out = run_with_timeout(run_command,
                                              dict(env,
                                                   PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                                   TMP='/tmp',
                                                   TEMP='/tmp',
                                                   TMPDIR='/tmp',
                                                   USER='%s' % getpass.getuser(),
                                                   PROMINENCE_CONTAINER_LOCATION='%s' % os.path.dirname(image),
                                                   PROMINENCE_CONTAINER_RUNTIME='singularity',
                                                   PROMINENCE_PWD='%s' % workdir,
                                                   PROMINENCE_CPUS='%d' % job_cpus,
                                                   PROMINENCE_MEMORY='%d' % job_memory),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_tasks(job, path, is_batch):
    """
    Execute sequential tasks
    """
    num_retries = 0
    if 'policies' in job:
        if 'maximumRetries' in job['policies']:
            num_retries = job['policies']['maximumRetries']

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

    # Check shared filesystem for multi-node jobs before doing anything
    if num_nodes > 1 and not is_batch and os.path.isfile('/home/user/.beeond'):
        if check_beeond():
            logging.info('BeeGFS shared filesystem is mounted on all nodes')
        else:
            logging.critical('BeeGFS shared filesystem is not mounted on all nodes')
            return False

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

        location = '%s/images/%d' % (path, count)
        try:
            os.makedirs(location)
        except Exception as err:
            logging.error('Unable to create directory %s', location)
            return False

        mpi = None
        if 'type' in task:
            if task['type'] == 'openmpi':
                mpi = 'openmpi'
            elif task['type'] == 'mpich':
                mpi = 'mpich'
            elif task['type'] == 'intelmpi':
                mpi = 'intelmpi'

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
            if image_name(image) == image_name(task_check['image']) and image_count < count and task['runtime'] == task_check['runtime']:
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
                metrics_download = monitor(download_udocker, image, location, count, path)
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
                    metrics_task = monitor(run_udocker,
                                           image,
                                           cmd,
                                           workdir,
                                           env,
                                           path,
                                           mpi,
                                           mpi_processes,
                                           procs_per_node,
                                           artifacts,
                                           task_time_limit,
                                           is_batch)
                    retry_count += 1
        else:
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image_new = '%s/images/%d/image.simg' % (path, image_count)
                image_pull_status = 'cached'
            elif not FINISH_NOW:
                image_new = '%s/image.simg' % location
                metrics_download = monitor(download_singularity, image, image_new, location, path)
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
                    metrics_task = monitor(run_singularity,
                                           image_new,
                                           cmd,
                                           workdir,
                                           env,
                                           path,
                                           mpi,
                                           mpi_processes,
                                           procs_per_node,
                                           artifacts,
                                           task_time_limit,
                                           is_batch)
                    retry_count += 1

        task_u = {}
        task_u['imagePullStatus'] = image_pull_status
        if metrics_download.time_wall:
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

    return success, tasks_u

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
    parser.add_argument('--outfile',
                        dest='outfile',
                        action='append',
                        help='Output file url put addresses')
    parser.add_argument('--outdir',
                        dest='outdir',
                        action='append',
                        help='Output directory url put addresses')

    return parser.parse_args()

if __name__ == "__main__":
    # Extract arguments from the command line
    args = create_parser()

    # Handle signals
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Initial directory
    path = os.getcwd()

    # Handle HPC systems
    is_batch = False
    if args.batch or (not os.path.isdir('/home/prominence') and not os.path.isdir('/mnt/beeond/prominence')):
        if 'HOME' in os.environ:
            path = os.environ['HOME']
        is_batch = True

    # Setup logging
    logging.basicConfig(filename='%s/promlet.%d.log' % (path, args.id), level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info('Started promlet using path "%s"' % path)

    # Write empty json job details, so no matter what happens next, at least an empty file exists
    try:
        with open('promlet.%d.json' % args.id, 'w') as file:
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

    # Create the usertmp directory if necessary
    if not os.path.isdir('/home/user/tmp'):
        logging.info('Creating usertmp directory')
        if not os.path.isdir(path + '/usertmp'):
            try:
                os.mkdir(path + '/usertmp')
            except Exception as ex:
                logging.error('Unable to create usertmp directory due to: %s', ex)
                exit(1)
    else:
        logging.info('Using existing tmp directory')

    # Read job description
    try:
        with open(args.job, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        exit(1)

    # Replace output file/dir URL addresses if necessary
    replace_output_urls(job, args.outfile, args.outdir)

    # Mount user-specified storage if necessary
    mount_storage(job)

    # Run tasks
    try:
        (success_tasks, json_tasks) = run_tasks(job, path, is_batch)
    except OSError as exc:
        logging.critical('Got exception running tasks: %s', exc)
        success_tasks = False
        json_tasks = {}

    # Upload output files if necessary
    (success_stageout, json_stageout) = stageout(job, path)

    # Write json job details
    json_output = {}
    json_output['tasks'] = json_tasks
    json_output['stageout'] = json_stageout

    try:
        with open('promlet.%d.json' % args.id, 'w') as file:
            json.dump(json_output, file)
    except Exception as exc:
        logging.critical('Unable to write promlet.json due to: %s', exc)

    # Unmount user-specified storage if necessary
    unmount_storage(job)

    # Return appropriate exit code - necessary for retries of DAG nodes
    if not success_tasks or not success_stageout:
        logging.info('Exiting promlet with failure')
        exit(1)

    logging.info('Exiting promlet with success')
    exit(0)

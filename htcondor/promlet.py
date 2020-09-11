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
DOWNLOAD_CONN_TIMEOUT = 10
DOWNLOAD_MAX_RETRIES = 2
DOWNLOAD_BACKOFF = 1

MPI_SSH_SCRIPT = \
"""#!/bin/bash

_LOCALHOST='localhost'
_LOCALIP='127.0.0.1'
_HAS_PARAMS="bcDeFIiLlmOopRSWw"

while (( "$#" )); do
        _ONE=`echo $@|cut -f1 -d' '`
        _TWO=`echo $@|cut -f2 -d' '`
        # easy. if a word starts with an hyphen it's an option and it might come with a parameter
        if [ "`echo $_ONE | cut -b1`" == "-" ]; then
                _PARAM=$_PARAM' '$_ONE
                _PREV='option'
                if [ "$(echo $_HAS_PARAMS | grep `echo $_ONE | cut -b2`)" ] && [ "`echo $_ONE | cut -b3`" == "" ]; then
                        _PARAM=$_PARAM' '$_TWO
                        shift
                fi
        else
                # if the current word does not have a hyphen (no option) then we have two possibilities
                #  a: previous word wasn't an option (hyphen)
                #  b: or the second word doesn't have a hyphen (part of command)
                # both cases then assume that the host must be the first word
                if [ "$_PREV" != "option" ] || [ "`echo $_TWO | cut -b1`" != "-" ]; then
                        _HOST=$_ONE
                        shift
                        _COMMAND=$@
                        break
                else
                        _PARAM=$_PARAM' '$_ONE
                        _PREV=''
                fi
        fi
        shift
done

_COMMAND=`echo $_COMMAND | tr '"' "'"`

# with no params start an singularity container remotely with a pseudy TTY
if [ "$_COMMAND" == "" ]; then
    _PARAM="-t $_PARAM"
    _COMMAND="/bin/bash"
else
    _COMMAND='/bin/bash -c "'$_COMMAND'"'
fi

# Some versions of Singularity have full path in SINGULARITY_CONTAINER, but other's don't (!)
PROMINENCE_IMAGE=$PROMINENCE_CONTAINER_LOCATION/$SINGULARITY_CONTAINER
if [[ $SINGULARITY_CONTAINER == *$PROMINENCE_CONTAINER_LOCATION* ]]; then
    PROMINENCE_IMAGE=$SINGULARITY_CONTAINER
fi

if [[ -z "${SINGULARITY_CONTAINER}" ]]; then
    ssh -i /home/user/.id_rsa -o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $_PARAM $_HOST env UDOCKER_DIR=$UDOCKER_DIR \$\(which udocker\) -q run --hostauth --hostenv -v /etc/ssh -v $PROMINENCE_PATH:/home/user --workdir=$PROMINENCE_PWD --user=user -v /home/user/tmp:/tmp $container_names $_COMMAND
else
    ssh -o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $_PARAM $_HOST env -i /usr/bin/singularity exec --home $PROMINENCE_PATH:/home/user --pwd $PROMINENCE_PWD $PROMINENCE_IMAGE $_COMMAND
fi

exit $?
"""

def create_sif_from_archive(image_out, image_in):
    """
    Create a Singularity image from a Docker archive
    """
    cmd = 'singularity build %s docker-archive://%s' % (image_out, image_in)

    try:
        process = subprocess.Popen(cmd,
                                   cwd=os.path.dirname(image_out),
                                   shell=True,
                                   env=dict(os.environ,
                                            PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin'),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode
    except Exception as exc:
        logging.error('Unable to build Singularity image from Docker archive due to %s', exc)
    else:
        if return_code == 0:
            return True
        else:
            logging.error(stdout)
            logging.error(stderr)
            return False

    return False

def download_from_url_with_retries(url, filename, max_retries=DOWNLOAD_MAX_RETRIES, backoff=DOWNLOAD_BACKOFF):
    """
    Download a file from a URL with retries and backoff
    """
    count = 0
    success = False

    while count < 1 + max_retries and not success:
        success = download_from_url(url, filename)

        # Delete anything if necessary
        if not success and os.path.exists(filename):
            try:
                os.remove(filename)
            except Exception:
                pass

        count += 1
        time.sleep(count*backoff)

    return success, count

def download_from_url(url, filename):
    """
    Download from a URL to a file
    """
    try:
        response = requests.get(url, allow_redirects=True, stream=True, timeout=DOWNLOAD_CONN_TIMEOUT)
        if response.status_code == 200:
            with open(filename, 'wb') as tar_file:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        tar_file.write(chunk)
        else:
            logging.error('Unable to download file from URL %s, status code is %d', url, response.status_code)
            return False
    except requests.exceptions.RequestException as exc:
        logging.error('Unable to download file from URL %s due to: %s', url, exc)
        return False
    except IOError as exc:
        logging.error('Unable to download file from URL %s due to: %s', url, exc)
        return False

    logging.info('File downloaded successfully from URL %s', url)

    return True

def process_file(filename, cmd):
    """
    Process a file
    """
    process = subprocess.Popen('%s %s' % (cmd, filename),
                               cwd=os.path.dirname(filename),
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
        return False

    return True

def download_artifacts(job, path):
    """
    Download any artifacts
    """
    json_out_files = []
    success = True

    if 'artifacts' in job:
        for artifact in job['artifacts']:
            logging.info('Downloading URL %s', artifact['url'])

            # Create filename
            urlpath = urlsplit(artifact['url']).path
            filename = posixpath.basename(unquote(urlpath))
            json_out_file = {'name':filename}
            filename = os.path.join(path, 'userhome', filename)

            # Download file
            json_out_file['status'] = 'success'
            time_begin = time.time()

            if not download_from_url(artifact['url'], filename):
                json_out_file['status'] = 'failedDownload'

            duration = time.time() - time_begin
            json_out_file['time'] = duration

            if json_out_file['status'] != 'success':
                json_out_files.append(json_out_file)
                return False, json_out_files

            if 'executable' in artifact:
                if artifact['executable']:
                    try:
                        os.chmod(filename, 0o775)
                    except IOError:
                        pass

            # Process file
            success = False
            remove_file = True
            if filename.endswith('.tgz') or filename.endswith('.tar.gz'):
                if process_file(filename, 'tar xzf'):
                    success = True
            elif filename.endswith('.tar'):
                if process_file(filename, 'tar xf'):
                    success = True
            elif filename.endswith('.gz'):
                if process_file(filename, 'gunzip'):
                    success = True
            elif filename.endswith('.tar.bz2'):
                if process_file(filename, 'tar xjf'):
                    success = True
            elif filename.endswith('.bz2'):
                if process_file(filename, 'bunzip2'):
                    success = True
            elif filename.endswith('.zip'):
                if process_file(filename, 'unzip'):
                    success = True
            else:
                remove_file = False
                success = True

            duration = time.time() - time_begin
            json_out_file['time'] = duration

            if not success:
                json_out_file['status'] = 'failedUncompress'

            if remove_file:
                if os.path.exists(filename):
                    try:
                        os.remove(filename)
                    except Exception as err:
                        logging.critical('Unable to delete file %s due to %s', filename, err)

            json_out_files.append(json_out_file)

    return success, json_out_files

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

def create_dirs(path):
    """
    Create the empty user home and tmp directories
    """
    # Create the userhome directory if necessary
    logging.info('Creating userhome directory')
    if not os.path.isdir(path + '/userhome'):
        try:
            os.mkdir(path + '/userhome')
        except Exception as exc:
            logging.error('Unable to create userhome directory due to: %s', exc)
            exit(1)

    # Create the usertmp directory if necessary
    if not os.path.isdir('/home/user/tmp'):
        logging.info('Creating usertmp directory')
        if not os.path.isdir(path + '/usertmp'):
            try:
                os.mkdir(path + '/usertmp')
            except Exception as exc:
                logging.error('Unable to create usertmp directory due to: %s', exc)
                exit(1)
    else:
        logging.info('Using existing tmp directory')

    # Create the mounts directory if necessary
    if not os.path.isdir('/home/user/mounts'):
        logging.info('Creating user mounts directory')
        if not os.path.isdir('/home/user/mounts'):
            try:
                os.mkdir('/home/user/mounts')
            except Exception as ex:
                logging.error('Unable to create user mounts directory due to: %s', ex)
                exit(1)
    else:
        logging.info('Using existing mounts directory')

    # Create mpi hosts directory
    logging.info('Creating mpihome directory')
    if not os.path.isdir(path + '/mpihome'):
        try:
            os.mkdir(path + '/mpihome')
        except Exception as exc:
            logging.error('Unable to create mpihome directory due to: %s', exc)
            exit(1)

def move_inputs(job, path):
    """
    Move any input files to the userhome directory
    """
    if 'inputs' in job:
        for input_file in job['inputs']:
            if 'filename' in input_file:
                filename = os.path.basename(input_file['filename'])
                logging.info('Moving input file %s into userhome directory', filename)
                try:
                    shutil.move('%s/%s' % (path, filename),
                                '%s/userhome/%s' % (path, filename))
                except:
                    logging.critical('Unable to move input file %s', filename)
                    return False

    return True

def find_mpi_hosts():
    """
    Return the path to the MPI hosts files
    """
    src_dir = None
    if os.path.isfile('/home/user/.hosts-openmpi'):
        src_dir = '/home/user'
    elif os.path.isfile('/mnt/beeond/.hosts-openmpi'):
        src_dir = '/mnt/beeond'
   
    return src_dir

def mpi_setup(runtime, cmd, env, path, mpi, mpi_processes, mpi_procs_per_node, num_nodes, workdir):
    """
    Setup for MPI jobs and create MPI command
    """
    # Create ssh command
    mpi_ssh = '/mnt/beeond/prominence/ssh_container'
    if '_PROMINENCE_SSH_CONTAINER' in os.environ:
        mpi_ssh = os.environ['_PROMINENCE_SSH_CONTAINER']

    if mpi:
        logging.info('This is an MPI task, so writing ssh_container script')
        mpi_ssh = '/home/user/.ssh_container'
        with open('%s/userhome/.ssh_container' % path, "w") as text_file:
            text_file.write(MPI_SSH_SCRIPT)
        os.chmod('%s/userhome/.ssh_container' % path, 0o775)
    else:
        logging.info('This is not an MPI task')

    # Copy MPI hosts files into userhome directory
    mpi_hosts = '%s/mpihome' % path
    if mpi:
        if not is_batch and num_nodes > 1:
            logging.info('Writing new MPI files...')

            src_dir = find_mpi_hosts()

            mpi_hosts = '/home/user'
            try:
                shutil.copyfile('%s/.hosts-openmpi' % src_dir,
                                '%s/mpihome/.hosts-openmpi' % path)
                shutil.copyfile('%s/.hosts-mpich' % src_dir,
                                '%s/mpihome/.hosts-mpich' % path)
                shutil.copyfile('/home/user/.ssh/id_rsa',
                                '%s/mpihome/.id_rsa' % path)
            except:
                logging.error('Unable to copy MPI files')
                exit(1)

            os.chmod('%s/mpihome/.id_rsa' % path, 0o600)

        else:
            create_mpi_files(mpi_hosts, num_nodes)

    # Setup differences between runtimes
    extras = ''
    if runtime == 'singularity':
        container_env_name = 'PROMINENCE_CONTAINER_LOCATION'
        extras = " -mca plm_rsh_no_tree_spawn 1"
    else:
        container_env_name = 'UDOCKER_DIR'
        
    # Create command
    mpi_per_node = ''
    if mpi == 'openmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-N %d --bind-to none' % mpi_procs_per_node
        mpi_env = " -x %s -x PROMINENCE_PWD -x HOME -x TEMP -x TMP -x PROMINENCE_PATH " % container_env_name
        mpi_env += " ".join('-x %s' % key for key in env)
        cmd = ("mpirun --hostfile /home/mpi/.hosts-openmpi"
               " -np %d"
               " %s"
               " %s"
               " %s"
               " -mca btl_base_warn_component_unused 0"
               " -mca plm_rsh_agent %s %s") % (mpi_processes, mpi_per_node, mpi_env, extras, mpi_ssh, cmd)
    elif mpi == 'intelmpi':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-ppn %d' % mpi_procs_per_node
        env_list = [container_env_name, 'PROMINENCE_PWD', 'HOME', 'TMP', 'TEMP', 'TMPDIR', 'PROMINENCE_PATH']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -machine /home/mpi/.hosts-mpich"
               " -np %d %s"
               " -gtool \"bash -c\":all=exclusive@aldi" # hack to disable Intel's Singularity magic
               " -wdir %s"
               " -envlist %s"
               " -launcher ssh"
               " -bootstrap-exec %s %s") % (mpi_processes, mpi_per_node, workdir, mpi_env, mpi_ssh, cmd)
    elif mpi == 'mpich':
        if mpi_procs_per_node > 0:
            mpi_per_node = '-ppn %d' % mpi_procs_per_node
        env_list = [container_env_name, 'PROMINENCE_PWD', 'HOME', 'TMP', 'TEMP', 'TMPDIR', 'PROMINENCE_PATH']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/mpi/.hosts-mpich"
               " -np %d"
               " %s"
               " -wdir %s"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec %s %s") % (mpi_processes, mpi_per_node, workdir, mpi_env, mpi_ssh, cmd)

    return cmd 

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

    src_dir = find_mpi_hosts()

    with open('%s/.hosts-openmpi' % src_dir, 'r') as hosts:
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

def create_mpi_files(path, num_nodes):
    """
    Create MPI hosts file if necessary
    """
    if num_nodes == 1:
        logging.info('Single node MPI, will write localhost into MPI machine files')

        try:
            with open(os.path.join(path, '.hosts-openmpi'), 'w') as mpi_file:
                mpi_file.write('localhost')
        except IOError as exc:
            logging.critical('Unable to write OpenMPI hosts file')
            return False

        try:
            with open(os.path.join(path, '.hosts-mpich'), 'w') as mpi_file:
                mpi_file.write('localhost')
        except IOError as exc:
            logging.critical('Unable to write MPICH hosts file')
            return False

    elif 'PBS_NODEFILE' in os.environ:
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
            logging.critical('Unable to write OpenMPI hosts file')
            return False

        try:
            with open(os.path.join(path, '.hosts-mpich'), 'w') as mpi_file:
                for line in pbs_lines:
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write MPICH hosts file')
            return False

        return True

    elif '_PROMINENCE_OPENMPI_HOSTS_FILE' in os.environ:
        logging.info('Environment variable _PROMINENCE_OPENMPI_HOSTS_FILE detected')
        try:
            with open(os.environ['_PROMINENCE_OPENMPI_HOSTS_FILE'], 'r') as pbs_nodefile:
                pbs_lines = pbs_nodefile.readlines()
        except IOError as exc:
            logging.critical('Unable to open _PROMINENCE_OPENMPI_HOSTS_FILE due to: %s', exc)
            return False

        try:
            with open(os.path.join(path, '.hosts-openmpi'), 'w') as mpi_file:
                for line in pbs_lines:
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write OpenMPI hosts file')
            return False

        try:
            with open(os.path.join(path, '.hosts-mpich'), 'w') as mpi_file:
                for line in pbs_lines:
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write MPICH hosts file')
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
            logging.critical('Unable to write OpenMPI hosts file')
            return False

        try:
            with open(os.path.join(path, '.hosts-mpich'), 'w') as mpi_file:
                for line in stdout.split('/n'):
                    mpi_file.write(line)
        except IOError as exc:
            logging.critical('Unable to write MPICH hosts file')
            return False

        return True

    return False

def handle_signal(signum, frame):
    """
    Send signal to current subprocesses
    """
    logging.info('Received signal %d', signum)

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
        logging.warning('RequestException when trying to upload file %s, error is: %s', filename, exc)
        return None
    except IOError as exc:
        logging.warning('IOError when trying to upload file %s, error is: %s', filename, exc)
        return None

    if response.status_code == 200:
        return True
    return None

def stageout(job, path):
    """
    Copy any required output files and/or directories to S3 storage
    """
    success = True

    # Change directory to the userhome directory
    os.chdir('%s/userhome' % path)

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
    json_mounts = []

    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        storage_provider = None
        storage_token = None

        if storage_type == 'onedata':
            logging.info('Mounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            # Create mount point if necessary
            if not os.path.isdir('/home/user/mounts%s' % storage_mountpoint):
                try:
                    os.mkdir('/home/user/mounts%s' % storage_mountpoint)
                except Exception as ex:
                    logging.error('Unable to create mount directory due to: %s', ex)
                    return False
            else:
                logging.info('Mounts directory already exists, no need to create it')

            try:
                process = subprocess.Popen('/usr/bin/oneclient -t %s -H %s /home/user/mounts%s' % (storage_token,
                                                                                       storage_provider,
                                                                                       storage_mountpoint),
                                           shell=True,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                return_code = process.returncode
            except Exception as exc:
                logging.critical('Unable to mount OneData due to %s', exc)
                json_mounts.append({'mountpoint':storage_mountpoint,
                                    'type':'onedata',
                                    'status':'failed'})
                return False, json_mounts

            logging.info('mount-stdout=%s', stdout)
            logging.info('mount-stderr=%s', stderr)

            if return_code == 0:
                json_mounts.append({'mountpoint':storage_mountpoint,
                                    'type':'onedata',
                                    'status':'success'})
            else:
                logging.critical('Unable to mount OneData')
                json_mounts.append({'mountpoint':storage_mountpoint,
                                    'type':'onedata',
                                    'status':'failed'})
                return False, json_mounts

    return True, json_mounts

def unmount_storage(job):
    """
    Unmount user-specified storage
    """ 
    path = '/home/user'
    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        storage_provider = None
        storage_token = None
        if storage_type == 'onedata':
            logging.info('Mounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            process = subprocess.Popen('/usr/bin/oneclient -t %s -H %s -u /home/user/mounts%s' % (storage_token,
                                                                                      storage_provider,
                                                                                      storage_mountpoint),
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.wait()
    return True

def get_storage_mountpoint(job):
    """
    Get mount point for fuse filesystem from job JSON
    """
    if 'storage' in job:
        return job['storage']['mountpoint']

    return None

def download_singularity(image, image_new, location, path):
    """
    Download a Singularity image from a URL or pull an image from Docker Hub
    """
    logging.info('Pulling Singularity image for task')

    if re.match(r'^http', image):
        if image_name(image).endswith('.tar') or image_name(image).endswith('.tgz'):
            # We need to download the Docker tarball then convert it to the Singularity format
            if image_name(image).endswith('.tar'):
                image_new_tmp = image_new.replace('image.simg', 'image.tar')
            else:
                image_new_tmp = image_new.replace('image.simg', 'image.tgz')

            (success, attempts) = download_from_url_with_retries(image, image_new_tmp)
            logging.info('Number of attempts to download file %s was %d', image, attempts)

            if not success:
                return 1, False

            # Create singularity image from Docker archive
            success = create_sif_from_archive(image_new, image_new_tmp)

            if not success:
                return 1, False

            # Remove temporary file
            try:
                os.remove(image_new_tmp)
            except Exception:
                pass

        else:
            (success, attempts) = download_from_url_with_retries(image, image_new)
            logging.info('Number of attempts to download file %s was %d', image, attempts)
            if not success:
                return 1, False

        logging.info('Singularity image downloaded from URL and written to file %s', image_new)
    elif image.startswith('/'):
        # Handle image stored on attached POSIX-like storage

        if image.endswith('.tar') or image.endswith('.tgz'):
            # Create singularity image from Docker archive

            logging.info('Creating sif image from Docker archive')
            success = create_sif_from_archive(image_new, '/home/user/mounts/%s' % image)

            if not success:
                return 1, False
        else:
            logging.info('Copying Singularity image from source on attached storage')
            try:
                shutil.copyfile('/home/user/mounts/%s' % image, image_new)
            except:
                logging.error('Unable to copy container image from source location on attached storage')
                return 1, False
    else:
        # Handle both Singularity Hub & Docker Hub, with Docker Hub the default
        if re.match(r'^shub:', image):
            cmd = 'singularity pull --name "image.simg" %s' % image
        else:
            cmd = 'singularity pull --name "image.simg" docker://%s' % image

        count = 0
        success = False

        while count < DOWNLOAD_MAX_RETRIES and not success:
            try:
                process = subprocess.Popen(cmd,
                                           cwd=os.path.dirname(image_new),
                                           shell=True,
                                           env=dict(os.environ,
                                                    PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin'),
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                return_code = process.returncode
            except Exception as exc:
                logging.error('Unable to pull Singularity image due to %s', exc)
            else:
                if return_code == 0:
                    success = True
                else:
                    logging.error(stdout)
                    logging.error(stderr)

            count += 1

        if not success:
            logging.error('Unable to pull Singularity image successfully after %d attempts', count)
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
        logging.info('Installing udockertools in %s', location)

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

            if 'Error: installation of udockertools failed' in str(output) or return_code != 0:
                logging.error('Installation of udockertools failed')
            else:
                logging.info('udockertools installation successful')
                installed = True
            attempt += 1

    else:
        logging.info('Found existing udocker installation in %s', location)
        installed = True

    if not installed:
        return False

    return True

def download_udocker(image, location, label, path):
    """
    Download an image from a URL and create a udocker container named 'image'
    """
    udocker_location = get_udocker(path)
    if not udocker_location:
        logging.error('Unable to install udockertools')
        return 1, False

    if re.match(r'^http', image):
        # Download tarball
        logging.info('Downloading udocker image from URL')
        if not download_from_url(image, '%s/image.tar' % location):
            return 1, False

    if image.startswith('/') and image.endswith('.tar'):
        # Handle image stored on attached POSIX-like storage
        logging.info('Copying udocker image from source on attached storage')
        try:
            shutil.copyfile('/home/user/mounts/%s' % image, '%s/image.tar' % location)
        except:
            logging.error('Unable to copy container image from source location on attached storage')
            return 1, False

    if re.match(r'^http', image) or (image.startswith('/') and image.endswith('.tar')):
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

def run_udocker(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, num_nodes, artifacts, walltime_limit, mountpoint, is_batch):
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

    extras += " ".join('--env=%s=%s' % (key, env[key]) for key in env)

    # Provide job CPU and memory available to job
    job_info = get_info()
    if 'cpus' in job_info:
        extras += " --env=PROMINENCE_CPUS=%d" % job_info['cpus']
    if 'memory' in job_info:
        extras += " --env=PROMINENCE_MEMORY=%d" % job_info['memory']

    # Setup storage mountpoint for B2SAFE/OneData if necessary
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '-v /home/user/mounts%s:%s ' % (mountpoint, mountpoint)

    # Setup for MPI
    if mpi:
        cmd = mpi_setup('udocker', cmd, env, path, mpi, mpi_processes, mpi_procs_per_node, num_nodes, workdir)

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'
    if os.path.isdir('/home/user/tmp'):
        user_tmp_dir = '/home/user/tmp'

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' -v %s/userhome/%s:%s ' % (path, artifact, artifacts[artifact])

    # Prepare udocker command
    run_command = ("udocker -q run %s"
                   " --env=HOME=/home/user"
                   " --env=TMP=/tmp"
                   " --env=TEMP=/tmp"
                   " --env=PROMINENCE_PWD=%s"
                   " --env=PROMINENCE_PATH=%s/userhome"
                   " --env=UDOCKER_DIR=%s/.udocker"
                   " --env=PROMINENCE_CONTAINER_RUNTIME=udocker"
                   " --hostauth"
                   " --user=%s"
                   " --env=USER=%s"
                   " -v %s/userhome:/home/user"
                   " %s"
                   " --workdir=%s"
                   " -v %s:/tmp"
                   " %s %s") % (extras, workdir, path, udocker_location, getpass.getuser(), getpass.getuser(), path, mounts, workdir, user_tmp_dir, image, cmd)

    # Run container
    logging.info('Running: "%s"', run_command)
    return_code, timed_out = run_with_timeout(run_command,
                                              dict(os.environ,
                                                   PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                                   UDOCKER_DIR='%s/.udocker' % udocker_location,
                                                   PROMINENCE_PATH='%s/userhome' % path,
                                                   PROMINENCE_PWD='%s' % workdir),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_singularity(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, num_nodes, artifacts, walltime_limit, mountpoint, is_batch):
    """
    Execute a task using Singularity
    """
    # Create command to run
    home_type = '--home'
    mpi_home = ''
    if mpi:
        cmd = mpi_setup('singularity', cmd, env, path, mpi, mpi_processes, mpi_procs_per_node, num_nodes, workdir)
        mpi_home = '--bind %s/mpihome:/home/mpi' % path
        if num_nodes > 1:
            home_type = '--bind'

    command = 'exec'
    if cmd is None:
        cmd = ''
        command = 'run'
  
    autofork = 'on'
    if mpi and num_nodes > 1:
        autofork = 'off'

    # Get storage mountpoint
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '--bind /home/user/mounts%s:%s ' % (mountpoint, mountpoint)

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' --bind %s/userhome/%s:%s ' % (path, artifact, artifacts[artifact])

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'
    if os.path.isdir('/home/user/tmp'):
        user_tmp_dir = '/home/user/tmp'

    # Prepare Singularity command
    run_command = ("singularity %s"
                   " %s %s/userhome:/home/user"
                   " %s"
                   " --bind %s:/tmp"
                   " %s"
                   " --pwd %s"
                   " %s %s") % (command, home_type, path, mpi_home, user_tmp_dir, mounts, workdir, image, cmd)

    # Setup job CPUs & memory available
    job_cpus = -1
    job_memory = -1
    num_retries = 0
    job_info = get_info()
    if 'cpus' in job_info:
        job_cpus = job_info['cpus']
    if 'memory' in job_info:
        job_memory = job_info['memory']

    # Run container
    logging.info('Running: "%s"', run_command)
    return_code, timed_out = run_with_timeout(run_command,
                                              dict(env,
                                                   USER='%s' % getpass.getuser(),
                                                   PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                                                   TMP='/tmp',
                                                   TEMP='/tmp',
                                                   TMPDIR='/tmp',
                                                   PROMINENCE_CONTAINER_LOCATION='%s' % os.path.dirname(image),
                                                   PROMINENCE_PATH='%s/userhome' % path,
                                                   PROMINENCE_CONTAINER_RUNTIME='singularity',
                                                   I_MPI_HYDRA_BOOTSTRAP_AUTOFORK='%s' % autofork,
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

    # Move input files into userhome directory
    move_inputs(job, path)

    # Get storage mountpoint
    mountpoint = get_storage_mountpoint(job)

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
            elif 'executable' in artifact:
                if artifact['executable'] == 'true':
                    filename = url2filename(artifact['url'])
                    if os.path.exists(filename):
                        logging.info('Making artifact file %s executable', filename)
                        os.chmod(os.path.join(path, filename), 0o775)

    # Check shared filesystem for multi-node jobs before doing anything
    if num_nodes > 1 and not is_batch and os.path.isfile('/home/user/.beeond'):
        if check_beeond():
            logging.info('BeeGFS shared filesystem is mounted on all nodes')
        else:
            logging.critical('BeeGFS shared filesystem is not mounted on all nodes')
            return False, {}

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
            workdir = '/home/user'
        elif not workdir.startswith('/'):
            workdir = '/home/user/' + workdir

        env = {}
        if 'env' in task:
            for item in task['env']:
                env[item] = task['env'][item]

        if args.param:
            for pair in args.param:
                key = pair.split('=')[0]
                value = pair.split('=')[1]
                env['PROMINENCE_PARAMETER_%s' % key] = value
                if cmd:
                    cmd = Template(cmd).safe_substitute({key:value})

        location = '%s/images/%d' % (path, count)
        try:
            os.makedirs(location)
        except Exception as err:
            logging.error('Unable to create directory %s', location)
            return False, {}

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
        else:
            # Set processes per node to the number of CPUs
            procs_per_node = num_cpus

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
                logging.info('Pulling image for task')
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
                                           num_nodes,
                                           artifacts,
                                           task_time_limit,
                                           mountpoint,
                                           is_batch)
                    retry_count += 1
        else:
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image_new = '%s/images/%d/image.simg' % (path, image_count)
                image_pull_status = 'cached'
            elif not FINISH_NOW:
                image_new = '%s/image.simg' % location
                logging.info('Pulling image for task')
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
                                           num_nodes,
                                           artifacts,
                                           task_time_limit,
                                           mountpoint,
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
        if 'PWD' in os.environ:
            path = os.environ['PWD']
        is_batch = True

    # Setup logging
    logging.basicConfig(filename='%s/promlet.%d.log' % (path, args.id), level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info('Started promlet using path "%s"' % path)

    if is_batch:
        logging.info('Running on a batch system')

    # Write empty json job details, so no matter what happens next, at least an empty file exists
    try:
        with open('%s/promlet.%d.json' % (path, args.id), 'w') as file:
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

    # Read job description
    try:
        with open(args.job, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        exit(1)

    # Initialization
    json_mounts = []
    json_stagein = []
    json_tasks = []
    json_stageout = []

    success_stagein = False
    success_tasks = False
    success_stageout = False

    # Create the user home, tmp and mounts directories
    create_dirs(path)

    # Mount user-specified storage if necessary
    (success_mounts, json_mounts) = mount_storage(job)

    if success_mounts:
        # Replace output file/dir URL addresses if necessary
        if args.outfile or args.outdir:
            replace_output_urls(job, args.outfile, args.outdir)

        # Download any artifacts
        logging.info('Stagein any input files if necessary')
        (success_stagein, json_stagein) = download_artifacts(job, path)

        if not success_stagein:
            logging.error('Got error downloading artifact(s)')
        else: 

            # Run tasks
            try:
                (success_tasks, json_tasks) = run_tasks(job, path, is_batch)
            except OSError as exc:
                logging.critical('Got exception running tasks: %s', exc)
                success_tasks = False

            # Upload output files if necessary, even if tasks failed
            logging.info('Stageout any output files/dirs if necessary')
            (success_stageout, json_stageout) = stageout(job, path)

    # Write json job details
    json_output = {}
    json_output['mounts'] = json_mounts
    json_output['stagein'] = json_stagein
    json_output['tasks'] = json_tasks
    json_output['stageout'] = json_stageout

    try:
        with open('%s/promlet.%d.json' % (path, args.id), 'w') as file:
            json.dump(json_output, file)
    except Exception as exc:
        logging.critical('Unable to write promlet.json due to: %s', exc)

    # Unmount user-specified storage if necessary
    unmount_storage(job)

    # Return appropriate exit code - necessary for retries of DAG nodes
    if not success_mounts or not success_stagein or not success_tasks or not success_stageout:
        logging.info('Exiting promlet with failure')
        exit(1)

    logging.info('Exiting promlet with success')
    exit(0)
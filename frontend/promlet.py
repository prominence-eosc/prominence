#!/usr/bin/python
from __future__ import print_function
import argparse
import distutils.spawn
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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CURRENT_SUBPROCS = set()
FINISH_NOW = False
DOWNLOAD_CONN_TIMEOUT = 10
DOWNLOAD_MAX_RETRIES = 2
DOWNLOAD_BACKOFF = 1

def generate_envs():
    """
    Generate PATH & any other env variables to use for running udocker
    """
    use_path = '/usr/local/bin:/usr/bin:/bin'

    # Get path to Python
    path = None
    try:
        path = os.path.dirname(distutils.spawn.find_executable('python'))
    except:
        pass
    else:
        if path not in use_path.split(':'):
            use_path = '%s:%s' % (path, use_path)

    # Get path to udocker
    path = None
    try:
        path = distutils.spawn.find_executable('udocker')
    except:
        pass
    else:
        if path:
            path = os.path.dirname(path)
            if path not in use_path.split(':'):
                use_path = '%s:%s' % (path, use_path)

    additional_envs = {}
    for key in os.environ:
        if key.startswith('UDOCKER'):
            additional_envs[key] = os.environ[key]

    return (use_path, additional_envs)

def create_directories(token, base_url, directory, job_id, workflow_id):
    headers = {}
    headers['X-Auth-Token'] = token
    headers['X-CDMI-Specification-Version'] = '1.1.1'

    if job_id:
        directory = Template(directory).safe_substitute({"PROMINENCE_JOB_ID": job_id})
    if workflow_id:
        directory = Template(directory).safe_substitute({"PROMINENCE_WORKFLOW_ID": workflow_id})

    if directory[0] == '/':
        new_directory = directory[1:]
    else:
        new_directory = directory

    pieces = new_directory.split('/')
    combined = ''
    count = 0
    for piece in pieces:
        combined = '%s/%s' % (combined, piece)
        check = '%s%s/' % (base_url, combined)
        if count > 0:
            try:
                resp = requests.get(check, headers=headers, verify=False)
            except Exception as err:
                logging.info('Got exception trying to check for dirctory existence: %s', err)
                return None
            if resp.status_code == 404:
                logging.info('Directory %s doesnt exist, creating it...', check)
                # Create directory
                try:
                    resp_dir = requests.put(check, headers=headers, verify=False)
                except Exception as err:
                    logging.error('Got exception trying to create directory: %s', err)
                    return None
                if resp_dir.status_code != 201:
                    logging.error('Unable to create directory %s', check)
                    return None

        count = count + 1

    return '%s%s' % (base_url, directory)

def get_base_url(job):
    """
    If a storage system is defined and set as the default, return the default base URL
    to be used to gets/puts
    """
    base_url = None
    token = None
    directory = None

    if 'storage' in job:
        if 'default' in job['storage']:
            if 'onedata' in job['storage']:
                base_url = 'https://%s/cdmi' % job['storage']['onedata']['provider']
                token = str(job['storage']['onedata']['token'])
                directory = str(job['storage']['directory'])
    return (token, base_url, directory)

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
    if not os.path.isdir(path + '/usertmp'):
        logging.info('Creating usertmp directory')
        try:
            os.mkdir(path + '/usertmp')
        except Exception as exc:
            logging.error('Unable to create usertmp directory due to: %s', exc)
            exit(1)
    else:
        logging.info('Using existing tmp directory')

    # Create the mounts directory if necessary 
    if not os.path.isdir(path + '/mounts'):
        logging.info('Creating user mounts directory')
        try:
            os.mkdir(path + '/mounts')
        except Exception as ex:
            logging.error('Unable to create user mounts directory due to: %s', ex)
            exit(1)
    else:
        logging.info('Using existing mounts directory')

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

def get_cpu_info():
    """
    Get CPU details
    """
    data = (subprocess.check_output("lscpu", shell=True).strip()).decode()
    dict = {}
    for line in data.split('\n'):
        pieces = line.split(':')
        dict[pieces[0]] = pieces[1].lstrip()

    if 'Model name' not in dict:
        return (dict['Vendor ID'], dict['Model'], dict['CPU MHz'])

    return (dict['Vendor ID'], dict['Model name'], dict['CPU MHz'])

def get_job_ids(path):
    filename = '.job.ad'
    job_id = None
    workflow_id = None
    try:
        with open(filename, 'r') as fd:
            for line in fd.readlines():
                match = re.match(r'DAGManJobId = ([\d]+)', line)
                if match:
                    workflow_id = int(match.group(1))
                match = re.match(r'ClusterId = ([\d]+)', line)
                if match:
                    job_id = int(match.group(1))
    except Exception:
        pass

    return (job_id, workflow_id)

def get_token(path):
    filename = '.job.ad'
    token = None
    url = None
    try:
        with open(filename, 'r') as fd:
            for line in fd.readlines():
                match = re.match(r'ProminenceJobToken = "([\w\.\-]+)"', line)
                if match:
                    token = match.group(1)
                match = re.match(r'ProminenceURL = "([\w\.\-:\/]+)"', line)
                if match:
                    url = match.group(1)
    except Exception:
        pass

    return (token, url)

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

def download_from_url_with_retries(url, filename, token=None, max_retries=DOWNLOAD_MAX_RETRIES, backoff=DOWNLOAD_BACKOFF):
    """
    Download a file from a URL with retries and backoff
    """
    count = 0
    success = False

    while count < 1 + max_retries and not success:
        success = download_from_url(url, filename, token)

        # Delete anything if necessary
        if not success and os.path.exists(filename):
            try:
                os.remove(filename)
            except Exception:
                pass

        count += 1
        time.sleep(count*backoff)

    return success, count
        
def download_from_url(url, filename, token=None):
    """
    Download from a URL to a file
    """
    headers = {}
    if token:
        headers['X-Auth-Token'] = token

    try:
        response = requests.get(url, allow_redirects=True, stream=True, headers=headers, timeout=DOWNLOAD_CONN_TIMEOUT, verify=False)
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
    try:
        process = subprocess.Popen('%s %s' % (cmd, filename),
                                   cwd=os.path.dirname(filename),
                                   shell=True,
                                   env=dict(os.environ,
                                            PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin'),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode
    except Exception as ex:
        logging.error('Failed to run "%s" due to: %s', cmd, ex)
        return False

    if return_code != 0:
        logging.error('Failed to run "%s", stdout: %s', cmd, stdout)
        logging.error('Failed to run "%s", stderr: %s', cmd, stderr)
        return False

    return True

def download_artifacts(job, path):
    """
    Download any artifacts
    """
    json_artifacts = []
    success = True

    (token, base_url, _) = get_base_url(job)

    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if base_url and not artifact['url'].startswith('http'):
                artifact['url'] = '%s%s' % (base_url, artifact['url'])

            logging.info('Downloading URL %s', artifact['url'])

            artifact_path = os.path.join(path, 'userhome')

            # Create filename
            urlpath = urlsplit(artifact['url']).path
            filename = posixpath.basename(unquote(urlpath))
            json_artifact = {'name':filename}
            filename = os.path.join(artifact_path, filename)

            # Download file
            json_artifact['status'] = 'success'
            time_begin = time.time()

            (success, attempts) = download_from_url_with_retries(artifact['url'], filename, token)
            logging.info('Number of attempts to download file %s was %d', filename, attempts)
            if not success:
                json_artifact['status'] = 'failedDownload'

            duration = time.time() - time_begin
            json_artifact['time'] = duration

            if json_artifact['status'] != 'success':
                json_artifacts.append(json_artifact)
                return False, json_artifacts

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
            json_artifact['time'] = duration

            if not success:
                json_artifact['status'] = 'failedUncompress'

            if remove_file:
                if os.path.exists(filename):
                    try:
                        os.remove(filename)
                    except Exception as err:
                        logging.critical('Unable to delete file %s due to %s', filename, err)

            json_artifacts.append(json_artifact)

    return success, json_artifacts

def replace_output_urls(job, outfiles, outdirs):
    """
    Replace output file & directory URLs if necessary
    """
    # With user-provided storage, don't need to update URLs
    (storage, _, _) = get_base_url(job)
    if storage:
        return

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

def run_with_timeout(cmd, env, timeout_sec, capture_std=False):
    """
    Run a process with a timeout
    """
    if capture_std:
        proc = subprocess.Popen(shlex.split(cmd), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
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
def upload(filename, url, token=None):
    """
    Upload a file to a URL
    """
    headers = {}
    if token:
        headers['X-Auth-Token'] = token

    try:
        with open(filename, 'rb') as file_obj:
            response = requests.put(url, data=file_obj, timeout=120, headers=headers, verify=False)
    except requests.exceptions.RequestException as err:
        logging.warning('RequestException when trying to upload file %s: %s', filename, err)
        return None
    except IOError as err:
        logging.warning('IOError when trying to upload file %s: %s', filename, err)
        return None

    if response.status_code == 200 or response.status_code == 201:
        return True
    return None

def test_upload(url):
    """
    Check if a presigned URL is valid
    """
    try:
        response = requests.put(url, data='aa', timeout=120)
    except:
        return None

    if response.status_code == 403:
        return False

    logging.info('Successfully tested upload url')
    return True

@retry(tries=3, delay=2, backoff=2)
def get_new_url(path, name):
    """
    Get a new presigned URL
    """
    logging.info('Getting new presigned URL for file: %s', name)
    # Get token
    (token, url) = get_token(path)

    data = {'name': name}
    headers = {'Authorization': 'Bearer %s' % token}
    try:
        resp = requests.post('%s/data/output' % url, headers=headers, json=data, verify=False)
    except Exception as err:
        logging.error('Got exception when trying to get new presigned URL: %s', err)
        return None

    if resp.status_code == 201:
        if 'url' in resp.json():
            return resp.json()['url']

    logging.error('Unable to get new presigned URL for file: %s', name)
    return None

def check_url(url):
    """
    Check if a presigned URL is valid
    """
    match = re.search(r'.*Expires=(\d+).*', url)
    if match:
        expires = int(match.group(1))
        if expires - time.time() < 3600:
            return False
        else:
            if test_upload(url):
                return True
            return False
    return None

def stageout(job, path):
    """
    Copy any required output files and/or directories to S3 storage
    """
    success = True

    (token, base_url, directory) = get_base_url(job)
    (job_id, workflow_id) = get_job_ids(path)

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
                if 'url' in output:
                    url = output['url']
                    if not check_url(url):
                        url = get_new_url(path, os.path.basename(out_file))
                elif token and base_url and directory:
                    url = create_directories(token, base_url, directory, job_id, workflow_id)
                    if not url:
                        logging.error('Unable to upload file %s to cloud storage with url %s', out_file, url)
                        json_out_file['status'] = 'failedUpload'
                        success = False
                    else:
                        url = '%s/%s' % (url, out_file)

                if url:
                    if upload(out_file, url, token):
                        logging.info('Successfully uploaded file %s to cloud storage', out_file)
                        json_out_file['status'] = 'success'
                    else:
                        logging.error('Unable to upload file %s to cloud storage with url %s', out_file, url)
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
                if 'url' in output:
                    url = output['url']
                    if not check_url(url):
                        url = get_new_url(path, output_filename)
                elif token and base_url and directory:
                    url = create_directories(token, base_url, directory, job_id, workflow_id)
                    if not url:
                        logging.error('Unable to upload directory %s to cloud storage with url %s', output['name'], url)
                        json_out_dir['status'] = 'failedUpload'
                        success = False
                    else:
                        url = '%s/%s.tgz' % (url, output['name'])

                if url:
                    if upload(output_filename, url, token):
                        logging.info('Successfully uploaded directory %s to cloud storage', output['name'])
                        json_out_dir['status'] = 'success'
                    else:
                        logging.error('Unable to upload directory %s to cloud storage with url %s', output['name'], url)
                        json_out_dir['status'] = 'failedUpload'
                        success = False
            json_out_dirs.append(json_out_dir)

    # Change directory back to the original
    os.chdir(path)

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
 
def mount_storage(job, path):
    """
    Mount user-specified storage
    """
    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        if not storage_mountpoint:
            logging.info('No need to mount storage')
            return True

        storage_provider = None
        storage_token = None
        if storage_type == 'onedata':
            logging.info('Mounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            if not os.path.isdir('%s/mounts%s' % (path, storage_mountpoint)):
                try:
                    os.makedirs('%s/mounts%s' % (path, storage_mountpoint))
                except Exception as ex:
                    logging.error('Unable to create mount directory due to: %s', ex)
                    return False
            else:
                logging.info('Mounts directory already exists, no need to create it')

            options = ''
            if 'options' in job['storage']['onedata']:
                options = job['storage']['onedata']['options']

            cmd = '/usr/bin/oneclient -o allow_other -t %s -H %s %s %s/mounts%s' % (storage_token, storage_provider, options, path, storage_mountpoint)

            count = 0
            return_code = -1
            while count < 3 and return_code != 0:
                return_code, timed_out = run_with_timeout(cmd, os.environ, 60, True)
                if timed_out:
                    logging.error('Timeout running oneclient')
                count = count + 1

            logging.info('Return code from oneclient is %d', return_code)
            if return_code != 0:
                return False

    return True

def unmount_storage(job, path):
    """
    Unmount user-specified storage
    """
    if 'storage' in job:
        storage_type = job['storage']['type']
        storage_mountpoint = job['storage']['mountpoint']
        if not storage_mountpoint:
            logging.info('No need to unmount storage')
            return True

        storage_provider = None
        storage_token = None
        if storage_type == 'onedata':
            logging.info('Unmounting OneData provider at %s', storage_mountpoint)
            storage_provider = job['storage']['onedata']['provider']
            storage_token = job['storage']['onedata']['token']

            process = subprocess.Popen('/usr/bin/oneclient -t %s -H %s -u %s/mounts%s' % (storage_token,
                                                                                          storage_provider,
                                                                                          path,
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

def download_singularity(image, image_new, location, path, credential, job):
    """
    Download a Singularity image from a URL or pull an image from Docker Hub
    """
    logging.info('Pulling Singularity image for task')

    (token, base_url, _) = get_base_url(job)
    if base_url:
        image = '%s%s' % (base_url, image)

    if re.match(r'^http', image):
        if image_name(image).endswith('.tar') or image_name(image).endswith('.tgz'):
            # We need to download the Docker tarball then convert it to the Singularity format
            if image_name(image).endswith('.tar'):
                image_new_tmp = image_new.replace('image.simg', 'image.tar')
            else:
                image_new_tmp = image_new.replace('image.simg', 'image.tgz')

            (success, attempts) = download_from_url_with_retries(image, image_new_tmp, token)
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
            (success, attempts) = download_from_url_with_retries(image, image_new, token)
            logging.info('Number of attempts to download file %s was %d', image, attempts)
            if not success:
                return 1, False

        logging.info('Singularity image downloaded from URL and written to file %s', image_new)
    elif image.startswith('/') and os.path.exists(image):
        # Handle image stored on attached POSIX-like storage

        if image.endswith('.tar') or image.endswith('.tgz'):
            # Create singularity image from Docker archive

            logging.info('Creating sif image from Docker archive')
            # TODO: Mountpoint needs to be specified
            success = create_sif_from_archive(image_new, '%s/mounts/%s' % (path, image))

            if not success:
                return 1, False
        else:
            logging.info('Copying Singularity image from source on attached storage')
            try:
                # TODO: Mountpoint needs to be specified
                shutil.copyfile('%s/mounts/%s' % (path, image), image_new)
            except:
                logging.error('Unable to copy container image from source location on attached storage')
                return 1, False
    else:
        # Handle both Singularity Hub & Docker Hub, with Docker Hub the default
        if re.match(r'^shub:', image):
            cmd = 'singularity pull --name "image.simg" %s' % image
        else:
            cmd = 'singularity pull --name "image.simg" docker://%s' % image

        env = dict(os.environ, PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin')
        if credential['username'] and credential['token']:
            env['SINGULARITY_DOCKER_USERNAME'] = credential['username']
            env['SINGULARITY_DOCKER_PASSWORD'] = credential['token']

        count = 0
        success = False

        while count < DOWNLOAD_MAX_RETRIES and not success:
            try:
                process = subprocess.Popen(cmd,
                                           cwd=os.path.dirname(image_new),
                                           shell=True,
                                           env=env,
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
    (udocker_path, additional_envs) = generate_envs()
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

            envs = dict(PATH=udocker_path, UDOCKER_DIR='%s/.udocker' % location)
            if additional_envs:
                envs.update(additional_envs)

            # Install udocker if necessary
            process = subprocess.Popen('udocker --debug install',
                                       env=envs,
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            output, error = process.communicate()
            return_code = process.returncode

            if 'Error: installation of udockertools failed' in str(output) or return_code != 0:
                logging.error('Installation of udockertools failed')
                logging.error(output)
                logging.error(error)
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

def download_udocker(image, location, label, path, credential, job):
    """
    Download an image from a URL and create a udocker container named 'image'
    """
    udocker_location = get_udocker(path)
    if not udocker_location:
        logging.error('Unable to install udockertools')
        return 1, False

    (token, base_url, _) = get_base_url(job)
    if base_url:
        image = '%s%s' % (base_url, image)

    logging.info('Getting udocker image: %s', image)

    if re.match(r'^http', image):
        # Download tarball
        logging.info('Downloading udocker image from URL')
        (success, attempts) = download_from_url_with_retries(image, '%s/image.tar' % location, token)
        logging.info('Number of attempts to download file %s was %d', '%s/image.tar' % location, attempts)
        if not success:
            return 1, False

    (udocker_path, additional_envs) = generate_envs()

    if image.startswith('/') and image.endswith('.tar'):
        # Handle image stored on attached POSIX-like storage
        logging.info('Copying udocker image from source (%s) on attached storage', image)
        try:
            # TODO: Mountpoint needs to be specified
            shutil.copyfile('%s/mounts/%s' % (path, image), '%s/image.tar' % location)
        except Exception as err:
            logging.error('Unable to copy container image from source location on attached storage due to "%s"', err)
            return 1, False

    if re.match(r'^http', image) or (image.startswith('/') and image.endswith('.tar')):
        logging.info('Loading udocker image')
        # Load image
        process = subprocess.Popen('udocker load -i %s/image.tar' % location,
                                   env=dict(PATH=udocker_path,
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
        image = stdout.split('\n')[len(stdout.split('\n')) - 2]
        logging.info('Image name used is: %s', image)

        # Delete tarball
        os.unlink('%s/image.tar' % location)

    else:
        # Pull image
        process = subprocess.Popen('udocker pull %s' % image,
                                   env=dict(PATH=udocker_path,
                                            UDOCKER_DIR='%s/.udocker' % udocker_location),
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return_code = process.returncode

        if return_code != 0:
            return 1, False

    # Create container
    logging.info('Creating udocker container')
    process = subprocess.Popen('udocker create --name=image%d %s' % (label, image),
                               env=dict(PATH=udocker_path,
                                        UDOCKER_DIR='%s/.udocker' % udocker_location),
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    if return_code != 0:
        logging.error('Got error creating udocker container')
        logging.error('stdout=%s', stdout)
        logging.error('stderr=%s', stderr)
        return 1, False

    return 0, False

def run_udocker(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit, job):
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

    (job_id, workflow_id) = get_job_ids(path)
    if job_id:
        extras += " --env=PROMINENCE_JOB_ID=%d" % job_id
    if workflow_id:
        extras += " --env=PROMINENCE_WORKFLOW_ID=%d" % workflow_id

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
        if mpi_procs_per_node > 0:
            mpi_per_node = '-ppn %d' % mpi_procs_per_node
        env_list = ['PROMINENCE_PWD', 'UDOCKER_DIR', 'TMP', 'TEMP', 'TMPDIR']
        env_list.extend(env.keys())
        mpi_env = ",".join('%s' % item for item in env_list)
        cmd = ("mpirun -f /home/user/.hosts-mpich"
               " -np %d"
               " %s"
               " -envlist %s"
               " -launcher ssh"
               " -launcher-exec %s %s") % (mpi_processes, mpi_per_node, mpi_env, mpi_ssh, cmd)

    # Get storage mountpoint
    mountpoint = get_storage_mountpoint(job)
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '-v %s/mounts%s:%s ' % (path, mountpoint, mountpoint)

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' -v %s/userhome/%s:%s ' % (path, artifact, artifacts[artifact])

    run_command = ("udocker -q run %s"
                   " --env=HOME=/home/user"
                   " --env=USER=%s"
                   " --env=TMP=/tmp"
                   " --env=TEMP=/tmp"
                   " --env=TMPDIR=/tmp"
                   " --env=PROMINENCE_PWD=%s"
                   " --env=UDOCKER_DIR=%s/.udocker"
                   " --env=PROMINENCE_CONTAINER_RUNTIME=udocker"
                   " --hostauth"
                   " --user=%s"
                   " -v %s/userhome:/home/user"
                   " %s"
                   " --workdir=%s"
                   " -v %s:/tmp"
                   " %s %s") % (extras, getpass.getuser(), workdir, path, getpass.getuser(), path, mounts, workdir, user_tmp_dir, image, cmd)

    logging.info('Running: "%s"', run_command)

    (udocker_path, additional_envs) = generate_envs()
    return_code, timed_out = run_with_timeout(run_command,
                                              dict(PATH=udocker_path,
                                                   UDOCKER_DIR='%s/.udocker' % udocker_location),
                                              walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_singularity(image, cmd, workdir, env, path, mpi, mpi_processes, mpi_procs_per_node, artifacts, walltime_limit, job):
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
    mountpoint = get_storage_mountpoint(job)
    mounts = ''
    if mountpoint is not None:
        logging.info('Mount point is %s', mountpoint)
        mounts = '--bind %s/mounts%s:%s ' % (path, mountpoint, mountpoint)

    # Artifact mounts
    for artifact in artifacts:
        mounts = mounts + ' --bind %s/userhome/%s:%s ' % (path, artifact, artifacts[artifact])

    # Set source directory for /tmp in container
    user_tmp_dir = path + '/usertmp'

    run_command = ("singularity %s"
                   " --bind /mnt"
                   " --home %s/userhome:/home/user"
                   " --bind %s:/tmp"
                   " %s"
                   " --pwd %s %s %s") % (command, path, user_tmp_dir, mounts, workdir, image, cmd)

    job_cpus = -1
    job_memory = -1
    num_retries = 0
    job_info = get_info()
    if 'cpus' in job_info:
        job_cpus = job_info['cpus']
    if 'memory' in job_info:
        job_memory = job_info['memory']

    (job_id, workflow_id) = get_job_ids(path)

    logging.info('Running: "%s"', run_command)

    env_vars = dict(env,
                    PATH='/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin',
                    TMP='/tmp',
                    TEMP='/tmp',
                    TMPDIR='/tmp',
                    USER='%s' % getpass.getuser(),
                    PROMINENCE_CONTAINER_LOCATION='%s' % os.path.dirname(image),
                    PROMINENCE_CONTAINER_RUNTIME='singularity',
                    PROMINENCE_PWD='%s' % workdir,
                    PROMINENCE_CPUS='%d' % job_cpus,
                    PROMINENCE_MEMORY='%d' % job_memory,
                    PROMINENCE_JOB_ID='%d' % job_id)

    if workflow_id:
        env_vars['PROMINENCE_WORKFLOW_ID'] = '%d' % workflow_id

    return_code, timed_out = run_with_timeout(run_command, env_vars, walltime_limit)

    logging.info('Task had exit code %d', return_code)

    return return_code, timed_out

def run_tasks(job, path):
    """
    Execute sequential tasks
    """
    # Get token
    (token, url) = get_token(path)

    num_retries = 0
    ignore_failures = False
    if 'policies' in job:
        if 'maximumRetries' in job['policies']:
            num_retries = job['policies']['maximumRetries']
        if 'ignoreTaskFailures' in job['policies']:
            ignore_failures = job['policies']['ignoreTaskFailures']

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

    # Move input files into userhome directory
    move_inputs(job, path)

    # Artifact mounts
    artifacts = {}
    if 'artifacts' in job:
        for artifact in job['artifacts']:
            if 'mountpoint' in artifact:
                source = artifact['mountpoint'].split(':')[0]
                dest = artifact['mountpoint'].split(':')[1]
                artifacts[source] = dest

    # Check shared filesystem for multi-node jobs before doing anything
    if num_nodes > 1 and os.path.isfile('/home/user/.beeond'):
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

        (job_id, workflow_id) = get_job_ids(path)
        if job_id:
            workdir = Template(workdir).safe_substitute({"PROMINENCE_JOB_ID": job_id})
            if cmd:
                cmd = Template(cmd).safe_substitute({"PROMINENCE_JOB_ID": job_id})
        if workflow_id:
            workdir = Template(workdir).safe_substitute({"PROMINENCE_WORKFLOW_ID": workflow_id})
            if cmd:
                cmd = Template(cmd).safe_substitute({"PROMINENCE_WORKFLOW_ID": workflow_id})

        env = {}
        if 'env' in task:
            env = task['env']

        if token and url:
            env['PROMINENCE_TOKEN'] = token
            env['PROMINENCE_URL'] = url

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

        metrics_download = ProcessMetrics()
        metrics_task = ProcessMetrics()

        retry_count = 0
        task_was_run = False
        image_pull_status = 'completed'

        credential = {'username': None, 'token': None}
        if 'imagePullCredential' in task:
            if 'username' in task['imagePullCredential'] and 'token' in task['imagePullCredential']:
                credential = task['imagePullCredential']

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
                metrics_download = monitor(download_udocker, image, location, count, path, credential, job)
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
                                           job)
                    retry_count += 1
        else:
            # Pull image if necessary or use a previously pulled image
            if found_image:
                image_new = '%s/images/%d/image.simg' % (path, image_count)
                image_pull_status = 'cached'
            elif not FINISH_NOW:
                image_new = '%s/image.simg' % location
                metrics_download = monitor(download_singularity, image, image_new, location, path, credential, job)
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
                                           job)
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

        # Stop now if task ran for too long or we are told to finish
        if metrics_task.timed_out or FINISH_NOW:
            success = False
            break

        # Stop now if task had non-zero exit code, but continue if user wants to ignore failures
        if not ignore_failures and metrics_task.exit_code != 0:
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

    # Create the user home & tmp directories
    create_dirs(path)

    # Read job description
    try:
        with open(args.job, 'r') as json_file:
            job = json.load(json_file)
    except Exception as ex:
        logging.critical('Unable to read job description due to %s', ex)
        exit(1)

    # Replace output file/dir URL addresses if necessary
    if args.outfile or args.outdir:
        replace_output_urls(job, args.outfile, args.outdir)

    # Mount user-specified storage if necessary
    success_mount = mount_storage(job, path)
    
    json_mounts = []
    
    mount = {}
    if success_mount:
        mount['status'] = 'success'
    else:
        mount['status'] = 'failed'
    json_mounts.append(mount)

    json_stagein = []
    json_tasks = []
    json_stageout = []

    success_stagein = False
    success_tasks = False
    success_stageout = False

    if success_mount:
        # Download any artifacts
        logging.info('Stagein any input files if necessary')
        (success_stagein, json_stagein) = download_artifacts(job, path)
        if not success_stagein:
            logging.error('Got error downloading artifact')

        # Run tasks
        try:
            (success_tasks, json_tasks) = run_tasks(job, path)
        except OSError as exc:
            logging.critical('Got exception running tasks: %s', exc)
            success_tasks = False
            json_tasks = {}

        # Upload output files if necessary
        logging.info('Stageout any output files/dirs if necessary')
        (success_stageout, json_stageout) = stageout(job, path)

    # Write json job details
    json_output = {}
    json_output['mounts'] = json_mounts
    json_output['tasks'] = json_tasks
    json_output['stageout'] = json_stageout
    json_output['stagein'] = json_stagein

    # Get site
    job_info = get_info()
    if 'site' in job_info:
        json_output['site'] = job_info['site']

    # Get CPU info
    (cpu_vendor, cpu_model, cpu_clock) = get_cpu_info()
    json_output['cpu_vendor'] = cpu_vendor
    json_output['cpu_model'] = cpu_model
    json_output['cpu_clock'] = cpu_clock

    try:
        with open('%s/promlet.%d.json' % (path, args.id), 'w') as file:
            json.dump(json_output, file)
    except Exception as exc:
        logging.critical('Unable to write promlet.json due to: %s', exc)

    # Unmount user-specified storage if necessary
    unmount_storage(job, path)

    # Return appropriate exit code - necessary for retries of DAG nodes
    if not success_tasks or not success_stageout or not success_stagein:
        logging.info('Exiting promlet with failure')
        exit(1)

    logging.info('Exiting promlet with success')
    exit(0)

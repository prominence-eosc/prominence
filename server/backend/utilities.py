from functools import wraps
import shlex
import subprocess
import threading
import time

import classad
import htcondor

def run(cmd, cwd, timeout_sec):
    """
    Run a subprocess, capturing stdout & stderr, with a timeout
    """
    proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    timeout = {"value": False}
    timer = threading.Timer(timeout_sec, kill_proc, [proc, timeout])
    timer.start()
    stdout, stderr = proc.communicate()
    timer.cancel()
    return proc.returncode, stdout, stderr, timeout["value"]

def kill_proc(proc, timeout):
    """
    Helper function used by "run"
    """
    timeout["value"] = True
    proc.kill()

def redact_storage_creds(storage):
    """
    Redact storage credentials
    """
    if 'b2drop' in storage:
        if 'app-username' in storage['b2drop']:
            storage['b2drop']['app-username'] = '***'
        if 'app-password' in storage['b2drop']:
            storage['b2drop']['app-password'] = '***'
    elif 'onedata' in storage:
        if 'token' in storage['onedata']:
            storage['onedata']['token'] = '***'
    elif 'webdav' in storage:
        if 'username' in storage['webdav']:
            storage['webdav']['username'] = '***'
        if 'password' in storage['webdav']:
            storage['webdav']['password'] = '***'
    return storage

def condor_str(str_in):
    """
    Returns a double-quoted string
    """
    return str('"%s"' % str_in)

def get_routed_job_id(job_id):
    """
    Return the routed job id
    """
    schedd = htcondor.Schedd()
    jobs_condor = schedd.xquery('RoutedBy =?= undefined && ClusterId =?= %s' % job_id, ['RoutedToJobId'], 1)
    for job in jobs_condor:
        if 'RoutedToJobId' in job:
            return int(float(job['RoutedToJobId']))
    return None

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

def datetime_format(epoch):
    """
    Convert a unix epoch in a formatted date/time string
    """
    datetime_fmt = '%Y-%m-%d %H:%M:%S'
    return time.strftime(datetime_fmt, time.gmtime(epoch))

def elapsed(events):
    """
    Print elapsed job runtime in a nice way
    """
    time_fmt = ''
    if 'startTime' in events:
        if 'endTime' in events:
            elapsed_time = events['endTime'] - events['startTime']
        else:
            elapsed_time = time.time() - events['startTime']

        minutes, seconds = divmod(elapsed_time, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)

        if elapsed_time < 60:
            time_fmt = '%ds' % seconds
        elif elapsed_time < 60*60:
            time_fmt = '%dm' % minutes
        elif elapsed_time < 24*60*60:
            time_fmt = '%dh %dm' % (hours, minutes)
        else:
            time_fmt = '%dd %dh %dm' % (days, hours, minutes)

    time_fmt = time_fmt.replace(' 0s', '')
    time_fmt = time_fmt.replace(' 0m', '')
    time_fmt = time_fmt.replace(' 0h', '')

    return time_fmt

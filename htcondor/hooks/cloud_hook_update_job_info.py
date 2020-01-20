#!/usr/bin/python
from __future__ import print_function
import logging
from logging.handlers import RotatingFileHandler
import sys
import time
import ConfigParser
import requests
from requests.auth import HTTPBasicAuth
import classad
import htcondor
import os

def get_from_classad(name, class_ad, default=None):
    """
    Get the value of the specified item from a job ClassAd
    """
    value = default
    if name in class_ad:
        value = class_ad[name]
    return value

def check_startd(host):
    """
    Check that running the job has a startd
    """
    coll = htcondor.Collector()
    startds = coll.query(htcondor.AdTypes.Startd, 'Machine =?= "%s"' % host, ["State", "Activity"])
    if len(startds) != 1:
        return False
    return True

def get_infrastructure_status_with_retries(infra_id):
    """
    Get infrastructure status with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    status = None
    cloud = None
    reason = None
    while count < max_retries and status is None:
        (status, reason, cloud) = get_infrastructure_status(infra_id)
        count += 1
        time.sleep(count/2)
    return (status, reason, cloud)

def get_infrastructure_status(infra_id):
    """
    Get infrastructure status
    """
    try:
        response = requests.get('%s/%s' % (CONFIG.get('imc', 'url'), infra_id),
                                auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                   CONFIG.get('imc', 'password')),
                                cert=(CONFIG.get('imc', 'ssl-cert'),
                                      CONFIG.get('imc', 'ssl-key')),
                                verify=CONFIG.get('imc', 'ssl-cert'),
                                timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return (None, None)
    except requests.exceptions.RequestException:
        return (None, None)
    if response.status_code == 200:
        return (response.json()['status'], response.json()['status_reason'], response.json()['cloud'])
    return (None, None)

def update_classad():
    """
    Update job ClassAd with current infrastructure status if necessary
    """
    lock_file = '/tmp/.lock-update'

    # Read ClassAd
    job_ad = classad.parseOne(sys.stdin, parser=classad.Parser.Old)

    cluster_id = int(get_from_classad('ClusterId', job_ad, -1))
    job_status = int(get_from_classad('JobStatus', job_ad, -1))
    proc_id = int(get_from_classad('ProcId', job_ad, 0))
    infra_id = get_from_classad('ProminenceInfrastructureId', job_ad)
    infra_state = get_from_classad('ProminenceInfrastructureState', job_ad)
    infra_type = get_from_classad('ProminenceInfrastructureType', job_ad)
    infra_site = get_from_classad('ProminenceInfrastructureSite', job_ad)
    uid = get_from_classad('ProminenceJobUniqueIdentifier', job_ad)
    remote_host = get_from_classad('RemoteHost', job_ad)

    state = None
    job_id = '%s.%s' % (cluster_id, proc_id)

    if infra_id is not None and str(infra_type) == 'cloud' and infra_state != 'configured':
        (state, reason, cloud) = get_infrastructure_status_with_retries(infra_id)
        logger.info('[%s] Infrastructure with id %s is in state %s with reason %s on cloud %s', job_id, infra_id, state, reason, cloud)

        if (infra_site is None and cloud is not None) or infra_site != cloud:
            print('ProminenceInfrastructureSite = "%s"' % cloud)

        if infra_state != state and state != 'timedout':
            print('ProminenceInfrastructureState = "%s"' % state)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))

        if state == 'failed':
            logger.info('[%s] Infrastructure with id %s is in state failed', job_id, infra_id)
            print('ProminenceInfrastructureState = "failed"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))

        if state == 'unable':
            logger.info('[%s] Infrastructure with id %s is in state "unable"', job_id, infra_id)
            print('ProminenceInfrastructureState = "unable"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))
    elif str(infra_type) == 'batch':
        logger.info('[%s] Batch job', job_id)
    elif infra_id is not None and str(infra_type) == 'cloud' and infra_state == 'configured':
        logger.info('[%s] Infrastructure already known to be configured, no longer checking infrastructure state', job_id)

        if remote_host is not None and job_status == 2:
            run_check = False
            if not os.path.isfile(lock_file):
                run_check = True
            else:
                last_update = os.path.getmtime(lock_file)
                if time.time() - last_update > 600:
                    run_check = True
            if run_check:
                startd_status = check_startd(remote_host)
                if not startd_status:
                    logger.critical('[%s] Consistency check failed: running job has no startd known to the collector', job_id)
                #else:
                #    logger.info('[%s] Consistency check success: running job has a startd known to the collector', job_id)
    else:
        logger.info('[%s] No infrastructure id, creation must have failed', job_id)

    # Write status file
    #filename = '/opt/sandbox/%s/status' % uid
    #with open(filename, 'w') as status_file:
    #    status_file.write('deploying')

    # Create a lock file
    try:
        open(lock_file, 'a').close()
    except Exception as exc:
        logger.critical('[%s] Unable to write lock file, exiting...', job_id)

if __name__ == "__main__":
    # Read config file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    handler = RotatingFileHandler(CONFIG.get('logs', 'update'),
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('cloud_hook_update_job_info')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Update job ClassAd
    update_classad()

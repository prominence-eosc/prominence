#!/usr/bin/python3
from __future__ import print_function
import configparser
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
import classad
import htcondor

import update_presigned_urls

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
        return (None, None, None)
    except requests.exceptions.RequestException:
        return (None, None, None)
    if response.status_code == 200:
        return (response.json()['status'], response.json()['status_reason'], response.json()['cloud'])
    return (None, None, None)

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
    iwd = get_from_classad('Iwd', job_ad)
    args = get_from_classad('Args', job_ad)
    job_type = get_from_classad('ProminenceType', job_ad)
    start_time = get_from_classad('JobStartDate', job_ad, 0)
    original_job = get_from_classad('RoutedFromJobId', job_ad)
    original_job = int(original_job.split('.')[0])

    if start_time:
        start_time = int(start_time)

    job_id = '%s.%s' % (cluster_id, proc_id)

    logger.info('[%s] Job status is %d', job_id, job_status)

    if job_status == 2:
        logger.info('[%s] Updating DB with jobid=%d, starttime=%d, site=%s', job_id, original_job, start_time, infra_site)
        try:
            run = subprocess.run(["/usr/local/bin/update_job_status.sh %d %d %s 0" % (original_job, start_time, infra_site)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        except Exception as err:
            logger.info('[%s] Got exception: %s', job_id, err)

    state = None

    # Update any presigned URLs as necessary
    if job_status == 1:
        new_args = update_presigned_urls.update_presigned_urls(args, '%s/job.mapped.json' % iwd)
        if new_args:
            print('Args = "%s"' % new_args)

    if infra_id is not None and str(infra_type) == 'cloud' and infra_state != 'configured':
        (state, reason, cloud) = get_infrastructure_status_with_retries(infra_id)
        logger.info('[%s] Infrastructure with id %s is in state %s with reason %s on cloud %s', job_id, infra_id, state, reason, cloud)

        reason_id = None
        if reason:
            if reason == 'DeploymentFailed_ImageNotFound':
                reason_id = 18
            elif reason == 'DeploymentFailed_QuotaExceeded':
                reason_id = 17

        if job_status == 1 and cloud:
            if not reason_id:
                logger.info('[%s] Updating DB for idle job with jobid=%d, starttime=%d, site=%s', job_id, original_job, start_time, cloud)
                try:
                    run = subprocess.run(["/usr/local/bin/update_job_status.sh %d %d %s 0" % (original_job, start_time, cloud)],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                except Exception as err:
                    logger.info('[%s] Got exception: %s', job_id, err)
            else:
                logger.info('[%s] Updating DB for idle job with jobid=%d, starttime=%d, site=%s, reason=%d', job_id, original_job, start_time, cloud, reason_id)
                try:
                    run = subprocess.run(["/usr/local/bin/update_job_status.sh %d %d %s %d" % (original_job, start_time, cloud, reason_id)],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                except Exception as err:
                    logger.info('[%s] Got exception: %s', job_id, err)

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
            
        if state == 'waiting':
            logger.info('[%s] Infrastructure with id %s is in state waiting', job_id, infra_id)
            print('ProminenceInfrastructureState = "waiting"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))

        if state == 'unable':
            logger.info('[%s] Infrastructure with id %s is in state "unable"', job_id, infra_id)
            print('ProminenceInfrastructureState = "unable"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))
    elif str(infra_type) == 'batch':
        logger.info('[%s] Batch job', job_id)
    elif job_type == 'workflow':
        logger.info('[%s] Updates for workflow', job_id)
    elif infra_id is not None and str(infra_type) == 'cloud' and infra_state == 'configured':
        logger.info('[%s] Infrastructure already known to be configured, no longer checking infrastructure state', job_id)

        #if remote_host is not None and job_status == 2:
        #    run_check = False
        #    if not os.path.isfile(lock_file):
        #        run_check = True
        #    else:
        #        last_update = os.path.getmtime(lock_file)
        #        if time.time() - last_update > 600:
        #            run_check = True
        #    if run_check:
        #        startd_status = check_startd(remote_host)
        #        if not startd_status:
        #            logger.critical('[%s] Consistency check failed: running job has no startd known to the collector', job_id)
        #        #else:
        #        #    logger.info('[%s] Consistency check success: running job has a startd known to the collector', job_id)
    else:
        logger.info('[%s] No infrastructure id, creation must have failed', job_id)

    # Write status file
    #filename = '/opt/sandbox/%s/status' % uid
    #with open(filename, 'w') as status_file:
    #    status_file.write('deploying')

    # Create a lock file
    try:
        open(lock_file, 'a').close()
    except Exception as err:
        logger.critical('[%s] Unable to write lock file due to %s, exiting...', job_id, err)

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
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

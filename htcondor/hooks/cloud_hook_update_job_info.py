#!/usr/bin/python
from __future__ import print_function
import logging
import sys
import time
import ConfigParser
import requests
from requests.auth import HTTPBasicAuth
import classad
import htcondor
import os

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
    lock_file = '/opt/sandbox/.lock-update'

    infra_id = None
    infra_state = None
    infra_type = None
    state = None
    infra_site = None
    uid = None
    job_status = -1
    remote_host = None

    # Read ClassAd
    job_ad = classad.parseOne(sys.stdin, parser=classad.Parser.Old)
    if 'ClusterId' in job_ad:
        cluster_id = int(job_ad['ClusterId'])
    if 'JobStatus' in job_ad:
        job_status = int(job_ad['JobStatus'])
    if 'ProcId' in job_ad:
        proc_id = int(job_ad['ProcId'])
    if 'ProminenceInfrastructureId' in job_ad:
        infra_id = job_ad['ProminenceInfrastructureId']
    if 'ProminenceInfrastructureState' in job_ad:
        infra_state = job_ad['ProminenceInfrastructureState']
    if 'ProminenceInfrastructureType' in job_ad:
        infra_type = job_ad['ProminenceInfrastructureType']
    if 'ProminenceInfrastructureSite' in job_ad:
        infra_site = job_ad['ProminenceInfrastructureSite']
    if 'ProminenceJobUniqueIdentifier' in job_ad:
        uid = job_ad['ProminenceJobUniqueIdentifier']
    if 'RemoteHost' in job_ad:
        remote_host = job_ad['RemoteHost']

    job_id = '%s.%s' % (cluster_id, proc_id)

    logging.basicConfig(filename=CONFIG.get('logs', 'update'),
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s [%(name)s] %(message)s')

    if infra_id is not None and str(infra_type) == 'cloud' and infra_state != 'configured':
        (state, reason, cloud) = get_infrastructure_status_with_retries(infra_id)
        logging.info('[%s] Infrastructure with id %s is in state %s with reason %s on cloud %s', job_id, infra_id, state, reason, cloud)

        if (infra_site is None and cloud is not None) or infra_site != cloud:
            print('ProminenceInfrastructureSite = "%s"' % cloud)

        if infra_state != state and state != 'timedout':
            print('ProminenceInfrastructureState = "%s"' % state)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))

        if state == 'failed':
            logging.info('[%s] Infrastructure with id %s is in state failed', job_id, infra_id)
            print('ProminenceInfrastructureState = "failed"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))

        if state == 'unable':
            logging.info('[%s] Infrastructure with id %s is in state "unable"', job_id, infra_id)
            print('ProminenceInfrastructureState = "unable"')
            print('ProminenceInfrastructureStateReason = "%s"' % reason)
            print('ProminenceInfrastructureEnteredCurrentStatus = %d' % int(time.time()))
    elif str(infra_type) == 'batch':
        logging.info('[%s] Batch job', job_id)
    elif infra_id is not None and str(infra_type) == 'cloud' and infra_state == 'configured':
        logging.info('[%s] Infrastructure already known to be configured, no longer checking infrastructure state', job_id)

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
                    logging.critical('[%s] Consistency check failed: running job has no startd known to the collector', job_id)
                #else:
                #    logging.info('[%s] Consistency check success: running job has a startd known to the collector', job_id)
    else:
        logging.info('[%s] No infrastructure id, creation must have failed', job_id)

    # Write status file
    #filename = '/opt/sandbox/%s/status' % uid
    #with open(filename, 'w') as status_file:
    #    status_file.write('deploying')

    # Create a lock file
    try:
        open(lock_file, 'a').close()
    except Exception as exc:
        logging.critical('[%s] Unable to write lock file, exiting...', job_id)

if __name__ == "__main__":
    # Read config file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Update job ClassAd
    update_classad()

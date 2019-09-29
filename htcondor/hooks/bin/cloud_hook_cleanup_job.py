#!/usr/bin/python
import logging
import sys
import time
import ConfigParser
import requests
from requests.auth import HTTPBasicAuth
import classad

def delete_infrastructure_with_retries(infra_id):
    """
    Delete infrastructure with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    success = -1
    while count < max_retries and success != 0:
        success = delete_infrastructure(infra_id)
        count += 1
        time.sleep(count/2)
    return success

def delete_infrastructure(infra_id):
    """
    Delete infrastructure
    """
    try:
        response = requests.delete('%s/%s' % (CONFIG.get('imc', 'url'), infra_id),
                                   auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                      CONFIG.get('imc', 'password')),
                                   timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return 2
    except requests.exceptions.RequestException:
        return 1
    if response.status_code == 200:
        return 0
    return 1

def cleanup_infrastructure():
    """
    Cleanup infrastructure associated with the job if necessary
    """
    cluster_id = None
    proc_id = None
    infra_id = None
    infra_state = None
    infra_site = None
    infra_type = None
    uid = None
    job_status = -1
    iwd = None

    # Read ClassAd
    job_ad = classad.parseOne(sys.stdin, parser=classad.Parser.Old)
    if 'Iwd' in job_ad:
        iwd = job_ad['Iwd']
    if 'ClusterId' in job_ad:
        cluster_id = int(job_ad['ClusterId'])
    if 'ProcId' in job_ad:
        proc_id = int(job_ad['ProcId'])
    if 'JobStatus' in job_ad:
        job_status = int(job_ad['JobStatus'])
    if 'ProminenceInfrastructureId' in job_ad:
        infra_id = job_ad['ProminenceInfrastructureId']
    if 'ProminenceInfrastructureState' in job_ad:
        infra_state = job_ad['ProminenceInfrastructureState']
    if 'ProminenceInfrastructureSite' in job_ad:
        infra_site = job_ad['ProminenceInfrastructureSite']
    if 'ProminenceInfrastructureType' in job_ad:
        infra_type = job_ad['ProminenceInfrastructureType']
    if 'ProminenceJobUniqueIdentifier' in job_ad:
        uid = job_ad['ProminenceJobUniqueIdentifier']

    logging.basicConfig(filename=CONFIG.get('logs', 'cleanup'),
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s [%(name)s] %(message)s')

    job_id = '%s.%s' % (cluster_id, proc_id)
    exit_code = -1

    logging.info('[%s] Started working on infrastructure with id %s of type %s on site %s with state %s', job_id, infra_id, infra_type, infra_site, infra_state)

    if str(infra_type) == 'batch':
        logging.info('[%s] Batch infrastructure, so no need to do anything', job_id)
        exit_code = 0
    elif infra_id is not None:
        logging.info('[%s] Will destroy infrastructure with id %s on site %s which has state %s', job_id, infra_id, infra_site, infra_state)

        # Delete the infrastructure
        exit_code = delete_infrastructure_with_retries(infra_id)

        if exit_code == 2:
            logging.error('[%s] Error destroying infrastructure due to time out', job_id)

        if exit_code != 0:
            logging.error('[%s] Error destroying infrastructure', job_id)

        logging.info('[%s] Infrastructure successfully destroyed', job_id)

    # Handle case if routed job no longer exists, e.g. deleted by user
    if cluster_id is None:
        exit_code = 0

    # Handle case of job with no infrastructure id, i.e. deletion before any infrastructure was created
    if infra_id is None:
        logging.info('[%s] No infrastructure id', job_id)
        exit_code = 0

    # Write status file
    if job_status == 4:
        status = 'completed'
    else:
        status = 'failed'

    with open('%s/status' % iwd, 'w') as status_file:
        status_file.write(status)

    logging.info('[%s] Exiting with code %d', job_id, exit_code)
    sys.exit(exit_code)

if __name__ == "__main__":
    # Read config file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Delete infrastructure if necessary
    cleanup_infrastructure()

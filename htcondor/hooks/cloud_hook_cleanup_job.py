#!/usr/bin/python3
import configparser
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
import classad

def get_from_classad(name, class_ad, default=None):
    """
    Get the value of the specified item from a job ClassAd
    """
    value = default
    if name in class_ad:
        value = class_ad[name]
    return value

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
                                   #cert=(CONFIG.get('imc', 'ssl-cert'),
                                   #      CONFIG.get('imc', 'ssl-key')),
                                   #verify=CONFIG.get('imc', 'ssl-cert'),
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
    # Read ClassAd
    job_ad = classad.parseOne(sys.stdin, parser=classad.Parser.Old)

    iwd = get_from_classad('Iwd', job_ad)
    cluster_id = int(get_from_classad('ClusterId', job_ad, -1))
    proc_id = int(get_from_classad('ProcId', job_ad, 0))
    cluster_id_user = int(float(get_from_classad('RoutedFromJobId', job_ad, -1)))
    job_status = int(get_from_classad('JobStatus', job_ad, -1))
    infra_id = get_from_classad('ProminenceInfrastructureId', job_ad)
    infra_state = get_from_classad('ProminenceInfrastructureState', job_ad)
    infra_site = get_from_classad('ProminenceInfrastructureSite', job_ad)
    infra_type = get_from_classad('ProminenceInfrastructureType', job_ad)
    email = get_from_classad('ProminenceEmail', job_ad)
    identity = get_from_classad('ProminenceIdentity', job_ad)

    job_id = '%s.%s' % (cluster_id, proc_id)
    exit_code = -1

    logger.info('[%s] Started working on infrastructure with id %s of type %s on site %s with state %s', job_id, infra_id, infra_type, infra_site, infra_state)

    if str(infra_type) == 'batch':
        logger.info('[%s] Batch infrastructure, so no need to do anything', job_id)
        exit_code = 0
    elif infra_id is not None:
        logger.info('[%s] Will destroy infrastructure with id %s on site %s which has state %s', job_id, infra_id, infra_site, infra_state)

        # Delete the infrastructure
        exit_code = delete_infrastructure_with_retries(infra_id)

        if exit_code == 2:
            logger.error('[%s] Error destroying infrastructure due to time out', job_id)

        if exit_code != 0:
            logger.error('[%s] Error destroying infrastructure', job_id)

        logger.info('[%s] Infrastructure successfully destroyed', job_id)

    # Handle case if routed job no longer exists, e.g. deleted by user
    if cluster_id is None:
        exit_code = 0

    # Handle case of job with no infrastructure id, i.e. deletion before any infrastructure was created
    if infra_id is None:
        logger.info('[%s] No infrastructure id', job_id)
        exit_code = 0

    # Write status file
    if job_status == 4:
        status = 'completed'
    else:
        status = 'failed'

    with open('%s/status' % iwd, 'w') as status_file:
        status_file.write(status)

    logger.info('[%s] Exiting with code %d', job_id, exit_code)
    sys.exit(exit_code)

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    handler = RotatingFileHandler(CONFIG.get('logs', 'cleanup'),
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('cloud_hook_cleanup_job')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Delete infrastructure if necessary
    cleanup_infrastructure()

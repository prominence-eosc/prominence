#!/usr/bin/python3
from __future__ import print_function
import configparser
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import sys
import time
import uuid
import classad

import update_presigned_urls
from create_infrastructure import deploy

def get_from_classad(name, class_ad, default=None):
    """
    Get the value of the specified item from a job ClassAd
    """
    value = default
    if name in class_ad:
        value = class_ad[name]
    return value

def translate_classad():
    """
    Deploy infrastructure for a job
    """
    route = ''

    classad_in = sys.stdin.read().split('------')

    # Get route name
    match_obj = re.search(r'name = "([\w\-]+)"', classad_in[0])
    if match_obj:
        route = match_obj.group(1)

    job_ad = classad.parseOne(classad_in[1], parser=classad.Parser.Old)
    classad_new = job_ad

    iwd = get_from_classad('Iwd', job_ad)
    dag_node_name = get_from_classad('DAGNodeName', job_ad)
    cluster_id = int(get_from_classad('ClusterId', job_ad, -1))
    proc_id = int(get_from_classad('ProcId', job_ad, 0))
    job_status = int(get_from_classad('JobStatus', job_ad, 0))
    identity = get_from_classad('ProminenceIdentity', job_ad)
    uid = get_from_classad('ProminenceJobUniqueIdentifier', job_ad)
    my_groups = get_from_classad('ProminenceGroup', job_ad).split(',')
    factory_id = int(get_from_classad('ProminenceFactoryId', job_ad, 0))
    want_mpi = get_from_classad('ProminenceWantMPI', job_ad)
    existing_route_name = get_from_classad('RouteName', job_ad)
    args = get_from_classad('Args', job_ad)
    job_type = get_from_classad('ProminenceType', job_ad)

    if want_mpi:
        want_mpi = True
    else:
        want_mpi = False

    job_id = '%s.%s' % (cluster_id, proc_id)
    uid_raw = uid
    uid = "%s-%d" % (uid, factory_id)

    logger.info('[%s] Starting cloud_hook_translate_job', job_id)

    if 'batch' in route:
        # Write out updated ClassAd to stdout
        classad_new['InfrastructureSite'] = route
        print(classad_new.printOld())
        logger.info('[%s] Exiting cloud_hook_translate_job in batch mode for route %s', job_id, route)
        sys.exit(0)
    elif job_status == 1:
        logger.info('[%s] Attempting to create cloud infrastructure', job_id)

        # Create infrastructure ID
        uid_infra = str(uuid.uuid4())

        # Handle jobs submitted directly to HTCondor
        if existing_route_name:
            classad_new['TransferOutput'] = "promlet.0.log,promlet.0.json"

        # Current time
        epoch = int(time.time())
        classad_new['ProminenceLastRouted'] = epoch
        classad_new['ProminenceInfrastructureEnteredCurrentStatus'] = epoch

        # Create infrastructure
        logger.info('[%s] About to create infrastructure', job_id)
        try:
            (use_uid, infra_id) = deploy(identity, my_groups, iwd, cluster_id, None, 0, uid, factory_id, None)
        except Exception as exc:
            logger.critical('[%s] Exception deploying infrastructure: %s', job_id, exc)
            sys.exit(1)

        if infra_id is None:
            classad_new['ProminenceInfrastructureState'] = 'failed'
            logger.info('[%s] Deployment onto cloud failed', job_id)
        else:
            classad_new['ProminenceInfrastructureId'] = str('%s' % infra_id)
            classad_new['ProminenceInfrastructureState'] = 'deployment-init'
            classad_new['ProminenceWantCluster'] = use_uid
            classad_new['Requirements'] = classad.ExprTree('MY.ProminenceInfrastructureState =?= "configured"')
            classad_new['ProminenceProcId'] = str('%d' % proc_id)

            logger.info('[%s] Initiated infrastructure deployment with id "%s"', job_id, infra_id)

            new_args = update_presigned_urls.update_presigned_urls(args, '%s/job.mapped.json' % iwd)
            if new_args:
                classad_new['Args'] = str('%s' % new_args)

    # Write out updated ClassAd to stdout
    print(classad_new.printOld())

    # Write status file
    filename = '%s/status' % iwd
    try:
        with open(filename, 'w') as status_file:
            status_file.write('deploying')
    except Exception:
        logger.critical('[%s] Unable to write status file', job_id)

    logger.info('[%s] Exiting cloud_hook_translate_job', job_id)

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    handler = RotatingFileHandler(CONFIG.get('logs', 'translate'),
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('cloud_hook_translate_job')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Create infrastructure
    translate_classad()

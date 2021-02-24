#!/usr/bin/python3
import configparser
import glob
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import os
import sys
import socket
import time
import classad

from update_db import check_db, update_job_in_db, update_workflow_db

CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

# TODO: delete old processed completed jobs
# TODO: https://stackoverflow.com/questions/788411/check-to-see-if-python-script-is-running

def get_site(ad):
    if 'MachineAttrProminenceCloud0' in ad:
        return ad['MachineAttrProminenceCloud0']

    if 'ProminenceInfrastructureSite' in ad:
        return ad['ProminenceInfrastructureSite']

    return None

def get_creation_date(ad):
    """
    Determine the completion date of a job. This is unfortunately more complex than it 
    should be because any some circumstances CompletionDate is not in the job ClassAd
    """
    completion_date = 0

    if 'CompletionDate' in ad:
        completion_date = int(ad['CompletionDate'])

    if completion_date == 0:
        # Note that if a job was removed CompletionDate is 0 so we use EnteredCurrentStatus instead
        if 'EnteredCurrentStatus' in ad and int(ad['EnteredCurrentStatus']) > 0:
            completion_date = int(ad['EnteredCurrentStatus'])

        # Under some situations a completed job will have a CompletionDate of 0 and EnteredCurrentStatus will be the
        # time the job started running, so check if we can use LastVacateTime
        if 'LastVacateTime' in ad:
            if int(ad['LastVacateTime']) > completion_date:
                completion_date = int(ad['LastVacateTime'])

        # Also try JobFinishedHookDone
        if 'JobFinishedHookDone' in ad:
            if int(ad['JobFinishedHookDone']) > completion_date:
                completion_date = int(ad['JobFinishedHookDone'])

    return completion_date

def get_status_and_reason(ad, promlet_json):
    """
    Set the job status and reason
    """
    reason = 0

    if 'ProminenceInfrastructureStateReason' in ad:
        reason_txt = ad['ProminenceInfrastructureStateReason']
        if reason_txt == 'Creating infrastructure to run job':
            reason = 1
        elif reason_txt == 'NoMatchingResources':
            reason = 2
        elif reason_txt == 'NoMatchingResourcesAvailable':
            reason = 3
        elif reason_txt == 'DeploymentFailed_QuotaExceeded':
            reason = 17
        elif reason_txt == 'DeploymentFailed_ImageNotFound':
            reason = 18

    if 'JobStatus' in ad:
        job_status = int(ad['JobStatus'])

        # Set job status for completed and deleted jobs
        if job_status == 4:
            status = 3
        elif job_status == 3:
            status = 4

        if job_status == 3 and 'ProminenceInfrastructureState' in ad:
            if ad['ProminenceInfrastructureState'] == "failed":
                reason = 4
                status = 5
            if ad['ProminenceInfrastructureState'] == "unable":
                status = 5

        if job_status == 3 and 'RemoveReason' in ad:
            if 'Python-initiated action' in ad['RemoveReason']:
                reason = 16
                status = 4
            if 'Infrastructure took too long to be deployed' in ad['RemoveReason']:
                reason = 4
                status = 5
            if 'OtherJobRemoveRequirements = DAGManJobId' in ad['RemoveReason'] and 'was removed' in ad['RemoveReason']:
                reason = 11
                status = 4
            if ad['RemoveReason'] == 'NoMatchingResourcesAvailable':
                reason = 3
                status = 5
            if ad['RemoveReason'] == 'NoMatchingResources':
                reason = 4
                status = 5
            if 'Job was evicted' in ad['RemoveReason']:
                reason = 19
                status = 5

        if job_status == 3 and 'HoldReason' in ad:
            if 'Infrastructure took too long to be deployed' in ad['HoldReason']:
                if reason == 0:
                    reason = 15
                status = 5
            if 'Job took too long to start running' in ad['HoldReason']:
                if reason == 0:
                    reason = 4
                status = 5
            if 'Job was evicted' in ad['HoldReason']:
                reason = 19
                status = 5
            if ad['HoldReason'] == 'NoMatchingResourcesAvailable':
                if reason == 0:
                    reason = 3
                status = 5
            if ad['HoldReason'] == 'NoMatchingResources':
                if reason == 0:
                    reason = 2
                status = 5
            if ad['HoldReason'] == 'Job was queued for too long':
                if reason == 0:
                    reason = 13
                status = 5

    # Return status as failed if any fuse mounts failed
    if promlet_json and 'mounts' in promlet_json:
        for mount in promlet_json['mounts']:
            if 'status' in mount:
                if mount['status'] == 'failed':
                    status = 5
                    reason = 5

    # Return status as failed if artifact download failed
    if promlet_json and 'stagein' in promlet_json:
        for item in promlet_json['stagein']:
            if 'status' in item:
                if item['status'] == 'failedDownload':
                    status = 5
                    reason = 6
                if item['status'] == 'failedUncompress':
                    status = 5
                    reason = 7

    # Return status as failed if stageout failed
    if promlet_json and 'stageout' in promlet_json:
        if 'files' in promlet_json['stageout']:
            for item in promlet_json['stageout']['files']:
                if 'status' in item:
                    if item['status'] == 'failedNoSuchFile':
                        status = 5
                        reason = 8
                    if item['status'] == 'failedUpload':
                        status = 5
                        reason = 9
        if 'directories' in promlet_json['stageout']:
            for item in promlet_json['stageout']['directories']:
                if 'status' in item:
                    if item['status'] == 'failedNoSuchFile':
                        status = 5
                        reason = 8
                    if item['status'] == 'failedUpload':
                        status = 5
                        reason = 9

    # Return status as failed if container image pull failed
    if promlet_json and 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'imagePullStatus' in task:
                if task['imagePullStatus'] == 'failed':
                    status = 5
                    reason = 10

    # Return status as killed if job was killed due to walltime limit
    if promlet_json and 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'error' in task:
                status = 6
                reason = 12

    # Return status as failed if task had non-zero exit code
    if promlet_json and 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'exitCode' in task:
                if task['exitCode'] != 0:
                    status = 5
                    reason = 14

    # Make sure no errors are given if job completed successfully
    if status == 3:
        reason = 0

    return (status, reason)

def handle_workflow(job_id, ad):
    """
    """
    job_status = 3
    if 'JobStatus' in ad:
        job_status = int(ad['JobStatus'])

    completion_date = None
    if 'CompletionDate' in ad:
        completion_date = int(ad['CompletionDate'])

    if completion_date:
        logger.info('Updating workflow %d in DB', job_id)
        update_workflow_db(job_id, job_status, time_end=completion_date)

    return True

def move(filename):
    """
    """
    try:
        os.remove(filename)
    except Exception as err:
        logger.critical('Unable to remove file %s due to: %s', filename, err)

def send_to_socket(job_id, identity, group, promlet_json):
    """
    Sends InfluxDB formatted accounting record to Telegraf socket
    """
    telegraf_socket = "/tmp/telegraf.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.connect(telegraf_socket)

    cputime = 0
    walltime = 0

    site = None
    if promlet_json and 'site' in promlet_json:
        site = promlet_json['site']

    if promlet_json and 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'cpuTimeUsage' in task:
                cputime += task['cpuTimeUsage']
            if 'wallTimeUsage' in task:
                walltime += task['wallTimeUsage']

    message = "accounting,identity=%s,group=%s,infra_site=%s job_id=%d,walltime=%d,cputime=%d\n" % (identity, group, site, job_id, walltime, cputime)

    try:
        sock.send(message.encode('utf8'))
    except Exception as err:
        logger.error('Unable to write accounting record for job %d to Telegraf socket due to: %s', job_id, err)
        return None

    return True

def main():
    start_time = time.time()

    for filename in glob.glob('/var/spool/prominence/completed_jobs/history.*'):
        logger.info('Working on file %s', filename)
        # TODO: handle exception when opening file
        with open(filename, 'r') as fd:
            ad = classad.parseOne(fd, parser=classad.Parser.Old)

            if 'ProminenceType' not in ad:
                # This job is not one of ours!
                move(filename)
                continue

            # We don't need to do anything to handle routed jobs
            if 'RouteName' in ad:
                if ad['RouteName'] == 'cloud':
                    logger.info('Job is a routed job, no need to process it')
                    move(filename)
                    continue

            identity = None
            group = None
            cluster_id = None
            if 'ProminenceIdentity' in ad:
                identity = ad['ProminenceIdentity']
            if 'ProminenceGroup' in ad:
                group = ad['ProminenceGroup']
            if 'ClusterId' in ad:
                cluster_id = int(ad['ClusterId'])

            # Process workflow
            if 'Cmd' in ad:
                if ad['Cmd'] == '/usr/bin/condor_dagman':
                    if handle_workflow(cluster_id, ad):
                        move(filename)
                    continue

            iwd = None
            if 'Iwd' in ad:
                iwd = ad['Iwd']

            # Get promlet json
            promlet_json_filename = None
            if 'TransferOutput' in ad:
                output_files = ad['TransferOutput']
                match = re.search(r'\,(promlet.[\d]+.json)', output_files)
                if match:
                    promlet_json_filename = '%s/%s' % (iwd, match.group(1))

            if not promlet_json_filename:
                logger.error('Unable to get name of promlet json file for job %d', int(cluster_id))
                continue

            try:
                with open(promlet_json_filename) as promlet_json_file:
                    promlet_json = json.load(promlet_json_file)
            except Exception:
                logger.error('Unable to open promlet json file for job %d', int(cluster_id))
                promlet_json = None

            # Get the completion date
            completion_date = get_creation_date(ad)

            # Set the status and reason
            (status, reason) = get_status_and_reason(ad, promlet_json)

            # Get the site
            site = get_site(ad)

            # Get the start date
            start_date = None
            if 'JobStartDate' in ad:
                start_date = ad['JobStartDate']
                if start_date:
                    start_date = int(start_date)

            # Update the job in the DB
            logger.info('Setting status of job %d to %d with completion date %d and reason %d and site %s', int(cluster_id), status, completion_date, reason, site)
            if start_date:
                if site:
                    success_db = update_job_in_db(int(cluster_id), status, end_date=completion_date, start_date=start_date, reason=reason, site=site)
                else:
                    success_db = update_job_in_db(int(cluster_id), status, end_date=completion_date, start_date=start_date, reason=reason)
            else:
                if site:
                    success_db = update_job_in_db(int(cluster_id), status, end_date=completion_date, reason=reason, site=site)
                else:
                    success_db = update_job_in_db(int(cluster_id), status, end_date=completion_date, reason=reason)

            if success_db is None:
                logger.error('Unable to update status of job %d in the DB', int(cluster_id))
                continue

            # Send job details to InfluxDB
            if success_db:
                success = True
                if not send_to_socket(cluster_id, identity, group, promlet_json):
                    logger.error('Got error sending details to InfluxDB')
                    success = False

            # Move file to processed jobs directory
            move(filename)

    #logger.info('Time taken to process completed jobs: %d secs', int(time.time() - start_time))

if __name__ == "__main__":
    # Setup logging
    handler = RotatingFileHandler('/var/log/prominence/completed_jobs.log',
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('workflows')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    logger.info('Starting polling loop...')
    while True:
        if not check_db():
            sys.exit(1)

        try:
            main()
        except Exception as err:
            logger.error('Got exception processing completed jobs: %s', err)
        
        time.sleep(2)

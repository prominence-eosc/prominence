#!/usr/bin/python3
import configparser
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import socket
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
import classad

import send_email

def get_job_json_output(iwd, job_id):
    try:
        filename = '%s/promlet.0.json' % iwd
        with open(filename, 'r') as json_file:
            return json.load(json_file)
    except Exception as err:
        logger.error('[%d] Unable to open JSON promlet due to: %s', job_id, err)

    return {}

def send_to_socket(job_id, identity, infra_site, promlet_json):
    """
    Sends InfluxDB formatted accounting record to Telegraf socket
    """
    telegraf_socket = "/tmp/telegraf.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.connect(telegraf_socket)

    cputime = 0
    walltime = 0
    if 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'cpuTimeUsage' in task:
                cputime += task['cpuTimeUsage']
            if 'wallTimeUsage' in task:
                walltime += task['wallTimeUsage']

    message = "accounting,identity=%s,infra_site=%s job_id=%d,walltime=%d,cputime=%d\n" % (identity, infra_site, job_id, walltime, cputime)

    try:
        sock.send(message.encode('utf8'))
    except Exception as exc:
        logger.error('[%d] Unable to write accounting record to Telegraf socket due to: %s', job_id, exc)

def format_duration(tis):
    """
    Format a duration nicely
    """
    days = int(tis/86400)
    time_fmt = '%H:%M:%S'
    return '%d+%s' % (days, time.strftime(time_fmt, time.gmtime(tis)))

def send_completed_email(event, job_id_original, job_id_routed, identity, email, job_name, site, promlet_json):
    """
    Send a notification email
    """
    logger.info('[%d] Sending notification email to %s for event %s for job %d', job_id_routed, email, event, job_id_original)

    name = ''
    if job_name:
        name = ' (%s)' % job_name
    subject = 'Your PROMINENCE job with id %d%s has finished' % (job_id_original, name)

    memory_usage = 0
    cpu_usage = 0
    wall_usage = 0
    if 'tasks' in promlet_json:
        if 'maxMemoryUsageKB' in promlet_json['tasks']:
            memory_usage = int(promlet_json['tasks']['maxMemoryUsageKB'])/1000
        for task in promlet_json['tasks']:
            if 'cpuTimeUsage' in task:
                cpu_usage += task['cpuTimeUsage']
            if 'wallTimeUsage' in task:
                wall_usage += task['wallTimeUsage']

    cpu_usage = format_duration(cpu_usage)
    wall_usage = format_duration(wall_usage)

    content = ("CPU time usage   %s\n"
               "Wall time usage  %s\n"
               "Memory usage     %d MB\n"
               "\n"
               "Site             %s\n"
               "\n"
               "\n"
               "--\n"
               "Please do not reply to this email") % (cpu_usage, wall_usage, memory_usage, site)

    send_email.send_email(email, identity, subject, content)

def handle_notifications(iwd, job_id_original, job_id_routed, identity, email, site):
    """
    Check if any notifications are required
    """
    # Check if we need to run, as it is possible the cleanup hook may be run multiple times
    # and we only want to send a single notification
    lock_file = os.path.join(iwd, '.lock-cleanup-%d' % job_id_original)
    if os.path.isfile(lock_file):
        return

    # Create a lock file
    try:
        open(lock_file, 'a').close()
    except Exception:
        logger.critical('[%d] Unable to write lock file, will ignore notifications...', job_id_routed)
        return

    # Open JSON job description
    try:
        filename = '%s/job.json' % iwd
        with open(filename, 'r') as json_file:
            job_json = json.load(json_file)
    except Exception as err:
        logger.error('[%d] Unable to open JSON job description due to: %s', job_id_routed, err)
        return

    job_name = None
    if 'name' in job_json:
        job_name = job_json['name']

    # Open promlet json file
    try:
        filename = '%s/promlet.0.json' % iwd
        with open(filename, 'r') as json_file:
            promlet_json = json.load(json_file)
    except Exception as err:
        logger.error('[%d] Unable to open JSON promlet due to: %s', job_id_routed, err)

    # Check if we need to generate any notifications, and do so if necessary
    if 'notifications' in job_json:
        for notification in job_json['notifications']:
            if notification['event'] == 'jobFinished':
                if notification['type'] == 'email':
                    send_completed_email('jobFinished', job_id_original, job_id_routed, identity, email, job_name, site, promlet_json)

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
                                   cert=(CONFIG.get('imc', 'ssl-cert'),
                                         CONFIG.get('imc', 'ssl-key')),
                                   verify=CONFIG.get('imc', 'ssl-cert'),
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

    promlet_json = get_job_json_output(iwd, cluster_id)

    # Send accounting record to Telegraf
    send_to_socket(cluster_id_user, identity, infra_site, promlet_json)

    # Send any notifications if necessary, currently we only consider completed jobs
    if job_status == 4:
        handle_notifications(iwd, cluster_id_user, cluster_id, identity, email, infra_site)

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

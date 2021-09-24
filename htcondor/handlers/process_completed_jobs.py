#!/usr/bin/python3
import configparser
import glob
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import shutil
import time

import classad

import send_email_smtp as send_email
import accounting_es
import completed_jobs_db

logger = logging.getLogger('process_completed_jobs.process_completed_jobs')

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
    logger.info('Sending notification email to %s for event %s for job %d', email, event, job_id_original)

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

def handle_notifications(ad, iwd, job_id_original, job_id_routed, identity, email, site):
    """
    Check if any notifications are required
    """
    # Open JSON job description
    try:
        filename = '%s/.job.json' % iwd
        with open(filename, 'r') as json_file:
            job_json = json.load(json_file)
    except Exception as err:
        logger.error('[%d] Unable to open JSON job description due to: %s', job_id_routed, err)
        return

    # Get promlet json
    promlet_json_filename = None
    if 'TransferOutput' in ad:
        output_files = ad['TransferOutput']
        match = re.search(r'\,(promlet.[\d]+.json)', output_files)
        if match:
            promlet_json_filename = '%s/%s' % (iwd, match.group(1))

            if not promlet_json_filename:
                promlet_json = None

            try:
                with open(promlet_json_filename) as promlet_json_file:
                    promlet_json = json.load(promlet_json_file)
            except Exception:
                promlet_json = None

    if 'name' in job_json:
        job_name = job_json['name']
    else:
        job_name = None

    if 'notifications' in job_json:
        for notification in job_json['notifications']:
            if notification['event'] == 'jobFinished':
                if notification['type'] == 'email':
                    send_completed_email('jobFinished', job_id_original, job_id_routed, identity, email, job_name, site, promlet_json)


def move(filename):
    """
    Move history file to the processed directory
    """
    try:
        shutil.move(filename, filename.replace('/var/spool/prominence/completed_jobs', '/var/spool/prominence/completed_jobs_processed'))
    except Exception as err:
        logger.error('Unable to move file %s due to: %s', filename, err)

def process(ad, filename=None):
    """
    Handle completed jobs
    """
    processed = completed_jobs_db.is_done(int(ad['ClusterId']))
    if processed:
        logger.info('Job has been processing, no more to do')
        if filename:
            move(filename)
        return None

    # The job is not a PROMINENCE job
    if 'ProminenceType' not in ad:
        logger.info('Job is not from PROMINENCE')
        if filename:
            move(filename)
        return None

    # We don't need to do anything to handle routed jobs
    if 'RouteName' in ad:
        if ad['RouteName'] == 'cloud':
            logger.info('Job is a routed job, no need to process it')
            if filename:
                move(filename)
            return None

    iwd = ad['Iwd']

    job_id_routed = None
    if 'RoutedToJobId' in ad:
        job_id_routed = ad['RoutedToJobId']

    job_id_original = ad['ClusterId']
    identity = ad['ProminenceIdentity']

    email = None
    if 'ProminenceEmail' in ad:
        email = ad['ProminenceEmail']

    if 'ProminenceName' in ad:
        job_name = ad['ProminenceName']
    else:
        job_name = 'unnamed'

    site = None
    if 'ProminenceInfrastructureSite' in ad:
        site = ad['ProminenceInfrastructureSite']
    elif 'MachineAttrProminenceCloud0' in ad:
        site = ad['MachineAttrProminenceCloud0']

    status = None
    if 'JobStatus' in ad:
        status = int(ad['JobStatus'])

    # Send record to ElasticSearch
    accounting_es.accounting(ad)

    # Handle notifications if necessary
    if status == 4 and email:
        handle_notifications(ad, iwd, job_id_original, job_id_routed, identity, email, site)

    completed_jobs_db.add_job(int(ad['ClusterId']))
    if filename:
        move(filename)

    return True

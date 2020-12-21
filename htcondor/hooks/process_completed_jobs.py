#!/usr/bin/python3
import configparser
import glob
import json
import logging
from logging.handlers import RotatingFileHandler
import re
import shutil
import socket
import classad

def move(filename):
    """
    """
    filename_only = filename.replace('/var/spool/prominence/completed_jobs/', '')
    try:
        shutil.move(filename, '/var/spool/prominence/completed_jobs_processed/%s' % filename_only)
    except Exception as err:
        logger.critical('Unable to move file %s due to: %s', filename_only, err)

def send_to_socket(job_id, identity, promlet_json):
    """
    Sends InfluxDB formatted accounting record to Telegraf socket
    """
    telegraf_socket = "/tmp/telegraf.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.connect(telegraf_socket)

    cputime = 0
    walltime = 0

    site = None
    if 'site' in promlet_json:
        site = promlet_json['site']

    if 'tasks' in promlet_json:
        for task in promlet_json['tasks']:
            if 'cpuTimeUsage' in task:
                cputime += task['cpuTimeUsage']
            if 'wallTimeUsage' in task:
                walltime += task['wallTimeUsage']

    message = "accounting,identity=%s,infra_site=%s job_id=%d,walltime=%d,cputime=%d\n" % (identity, site, job_id, walltime, cputime)

    try:
        sock.send(message.encode('utf8'))
    except Exception as err:
        logger.error('Unable to write accounting record for job %d to Telegraf socket due to: %s', job_id, err)
        return None

    return True

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Setup logging
    handler = RotatingFileHandler('/var/log/prominence/completed_jobs.log',
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('workflows')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    for filename in glob.glob('/var/spool/prominence/completed_jobs/history.*'):
        logger.info('Working on file %s', filename)
        with open(filename, 'r') as fd:
            ad = classad.parseOne(fd, parser=classad.Parser.Old)

            if 'ProminenceType' in ad:
                if ad['ProminenceType'] != 'job':
                    move(filename)
                    continue
            else:
                continue

            if 'Cmd' in ad:
                if ad['Cmd'] == '/usr/bin/dagman':
                    move(filename)
                    continue

            iwd = None
            if 'Iwd' in ad:
                iwd = ad['Iwd']

            json_file = None
            if 'TransferOutput' in ad:
                output_files = ad['TransferOutput']
                match = re.search(r'\,(promlet.[\d]+.json)', output_files)
                if match:
                    json_file = '%s/%s' % (iwd, match.group(1))

            if not json_file:
                logger.info('Unable to get name of promlet json file')
                continue

            identity = None
            cluster_id = None
            if 'ProminenceIdentity' in ad:
                identity = ad['ProminenceIdentity']
            if 'ClusterId' in ad:
                cluster_id = ad['ClusterId']

            # Send job details to InfluxDB
            success = True
            if not send_to_socket(cluster_id, identity, json_file):
                success = False

            # Move file to processed jobs directory
            if success:
                move(filename)


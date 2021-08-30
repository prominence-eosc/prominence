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
import process_completed_jobs

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    handler = RotatingFileHandler(CONFIG.get('logs', 'completed'),
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('process_completed_jobs')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    completed_jobs_db.init_db()

    for filename in glob.glob('/var/spool/prominence/completed_jobs/history.*'):
        logger.info('Working on file %s', filename)

        with open(filename, 'r') as fd:
            ad = classad.parseOne(fd, parser=classad.Parser.Old)
            process_completed_jobs.process(ad, filename)

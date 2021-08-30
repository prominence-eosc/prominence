#!/usr/bin/python3
import configparser
import htcondor
import classad
import time
import logging
from logging.handlers import RotatingFileHandler

import completed_jobs_db
import process_completed_jobs

coll = htcondor.Collector()

# Read config file
CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

# Logging
handler = RotatingFileHandler(CONFIG.get('logs', 'completed_in_queue'),
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('process_completed_jobs')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def find_completed_jobs():
    results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
    for result in results:
        schedd = htcondor.Schedd(coll.locate(htcondor.DaemonTypes.Schedd, result["Name"]))
        jobs = schedd.query('JobStatus == 4 && CurrentTime - EnteredCurrentStatus < 120 && isUndefined(RoutedBy)')

        for job in jobs:
            logger.info('Working on completed job in queue %d', int(job['ClusterId']))
            job_id = int(job['ClusterId'])
            processed = completed_jobs_db.is_done(job_id)
            if processed == False:
                logger.info('Job needs to be processed')
                processed = process_completed_jobs.process(job)
                if processed:
                    logger.info('Successfully processed job')
                else:
                    logger.info('Unable to successfully process job')

if __name__ == "__main__":
    completed_jobs_db.init_db()
    find_completed_jobs()

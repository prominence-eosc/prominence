import json
import logging
import os
import re
import signal
import sys
import time
import django

import htcondor

from django.db.models import Q

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "prominence.settings")
django.setup()

# Logging
logger = logging.getLogger('update_job_info')

from frontend.models import Job, Workflow
import server.settings
from server.backend import ProminenceBackend
from server.set_groups import set_groups_user

from update_db import check_db

EXIT_NOW = False

def handle_signal(signum, frame):
    """
    Handle signals
    """
    global EXIT_NOW
    EXIT_NOW = True
    logger.info('Received signal %d, shutting down...', signum)

def update_jobs():
    # Get pending jobs from the DB
    jobs_db = Job.objects.filter(Q(status=1))

    # Get running jobs from HTCondor
    coll = htcondor.Collector()
    results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
    for result in results:
        host = result["Name"]
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, host)
        schedd = htcondor.Schedd(schedd_ad)
        jobs = schedd.query('JobStatus == 2 && isUndefined(RoutedToJobId) && isUndefined(RoutedFromJobId) && Cmd != "/usr/bin/condor_dagman"',
                            ["ClusterId", "MachineAttrProminenceCloud0"])

        for job in jobs:
            for job_db in jobs_db:
                if job_db.backend_id == int(job['ClusterId']):
                    logger.info('Setting status of job %d with HTCondor id %d to running', job_db.id, int(job['ClusterId']))
                    job_db.status = 2
                    fields = ['status']
                    if 'MachineAttrProminenceCloud0' in job:
                        job_db.site = job['MachineAttrProminenceCloud0']
                        fields.append('site')
                    job_db.save(update_fields=fields)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info('Entering main polling loop')

    while True:
        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)

        if not check_db():
            sys.exit(1)

        try:
            update_jobs()
        except Exception as err:
            logger.error('Got exception updating jobs: %s', err)

        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)

        time.sleep(10)

import json
import logging
import os
import re
import signal
import sys
import time
import django

import htcondor

from django.core.management.base import BaseCommand
from django.db.models import Q

from frontend.models import Job, Workflow
import server.settings
from server.backend import ProminenceBackend
from server.set_groups import set_groups_user

from update_db import check_db

# Logging
logger = logging.getLogger('update_job_info')

class Command(BaseCommand):

    def update_jobs(self):
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

    def handle(self, **options):

        while True:
            if not check_db():
                sys.exit(1)

            try:
                self.update_jobs()
            except Exception as err:
                logger.error('Got exception updating jobs: %s', err)

            time.sleep(10)

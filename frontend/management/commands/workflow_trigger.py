#!/usr/bin/python3
import configparser
import logging
import time
import htcondor

from django.core.management.base import BaseCommand

from workflow_handler import update_workflows, add_workflow

# Logging
logger = logging.getLogger('update_job_info')

def trigger():
    coll = htcondor.Collector()

    results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
    for result in results:
        host = result["Name"]
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, host)
        schedd = htcondor.Schedd(schedd_ad)

        jobs = schedd.query('Cmd =?= "/usr/bin/condor_dagman" && JobStatus == 2',
                            ['ProminenceIdentity',
                             'ProminenceGroup',
                             'iwd',
                             'ClusterId',
                             'ProminenceJobUniqueIdentifier'])

        for job in jobs:
            logger.info('Running add_workflow for workflow with id %d', int(job['ClusterId']))
            add_workflow(int(job['ClusterId']),
                         job['iwd'],
                         job['ProminenceIdentity'],
                         job['ProminenceGroup'],
                         job['ProminenceJobUniqueIdentifier'])

class Command(BaseCommand):

    def handle(self, **options):
        # Read config file
        CONFIG = configparser.ConfigParser()
        CONFIG.read('/etc/prominence/prominence.ini')

        while True:

            # Add workflow if necessary
            trigger()

            # Update workflows
            start_time = time.time()
            try:
                update_workflows()
            except Exception as err:
                logger.critical('Got exception running update_workflows: %s', err)
            end_time = time.time()

            if int(end_time - start_time) > 0:
                logger.info('Time to update workflows: %d secs', int(end_time - start_time))

            time.sleep(int(CONFIG.get('polling', 'workflow-handler')))

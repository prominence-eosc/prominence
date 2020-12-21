#!/usr/bin/python3
import configparser
import logging
from logging.handlers import RotatingFileHandler
import htcondor

from workflow_handler import update_workflows, add_workflow, acquire_lock, release_lock

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

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Setup logging
    handler = RotatingFileHandler('/var/log/prominence/workflow_trigger.log',
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('workflows')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Add workflow if necessary
    trigger()

    # Update workflows
    if acquire_lock():
        try:
            update_workflows()
        except Exception as err:
            logger.critical('Got exception running update_workflows: %s', err)
        release_lock()
    # TODO: handle stuck lock due to process dying for some reason

#!/usr/bin/python3
import configparser
import logging
from logging.handlers import RotatingFileHandler
import time
import htcondor
import fasteners

from workflow_handler import update_workflows, add_workflow

CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

handler = RotatingFileHandler('/var/log/prominence/workflow_trigger.log',
                              maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                              backupCount=int(CONFIG.get('logs', 'num')))
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger('workflows')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

coll = htcondor.Collector()

results = coll.query(htcondor.AdTypes.Schedd, "true", ["Name"])
for result in results:
    host = result["Name"]
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, host)
    schedd = htcondor.Schedd(schedd_ad)

    jobs = schedd.query('Cmd =?= "/usr/bin/condor_dagman" && JobStatus == 2',
                        ['ProminenceIdentity', 'ProminenceGroup', 'iwd', 'ClusterId', 'ProminenceJobUniqueIdentifier'])
    for job in jobs:
        logger.info('Running add_workflow for workflow with id %d', int(job['ClusterId']))
        add_workflow(int(job['ClusterId']),
                     job['iwd'],
                     job['ProminenceIdentity'],
                     job['ProminenceGroup'],
                     job['ProminenceJobUniqueIdentifier'])

lock = fasteners.InterProcessLock('/var/spool/prominence/wftrigger.lock')
with lock:
    update_workflows()

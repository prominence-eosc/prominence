#!/usr/bin/python3
import configparser
import logging
from logging.handlers import RotatingFileHandler
import time
import htcondor

from workflow_handler import get_infrastructure_in_status, delete_infrastructure_with_retries

def get_startds():
    """
    Get current list of startds
    """
    coll = htcondor.Collector()

    results = coll.query(htcondor.AdTypes.Startd, 'PartitionableSlot=?=True',
                         ['Machine',
                          'ProminenceCloud',
                          'ProminenceInfrastructureId',
                          'TotalCpus',
                          'TotalSlotCpus',
                          'Cpus'])

    return results
        
def check_configured_infrastructure(infras, startds):
    """
    Find any configured infrastructures with no associated startd
    """
    for infra in infras:
        found = False
        for startd in startds:
            if infra['id'] == startd['ProminenceInfrastructureId']:
                found = True

        if not found and time.time() - infra['updated'] > 30*60:
            logger.error('Found infrastructure with id %s in configured state with no startd', infra['id'])

def find_idle_workers(infras, startds):
    """
    Find any idle startds
    """
    a = 1
    #for startd in startds:
    # 

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Setup logging
    handler = RotatingFileHandler('/var/log/prominence/check_infrastructure.log',
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('infrastructure')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    infras = get_infrastructure_in_status('configured')
    startds = get_startds()

    # Compare configured infrastructure with startds known to the collector
    check_configured_infrastructure(infras, startds)

    # Find idle worker nodes
    find_idle_workers(infras, startds)


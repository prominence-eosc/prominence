import json
import logging
import os
import re
import signal
import sys
import time
import django

from django.db.models import Q

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "prominence.settings")
django.setup()

# Logging
logger = logging.getLogger('update_htcondor')

from frontend.models import Job, Workflow
import server.settings
from server.backend import ProminenceBackend
from server.set_groups import set_groups_user

EXIT_NOW = False

def handle_signal(signum, frame):
    """
    Handle signals
    """
    global EXIT_NOW
    EXIT_NOW = True
    logger.info('Received signal %d, shutting down...', signum)

def delete_jobs():
    """
    Delete jobs
    """
    backend = ProminenceBackend(server.settings.CONFIG)

    # Find newly deleted jobs
    jobs = Job.objects.filter(Q(status=4) & Q(updated=True))

    for job in jobs:
        logger.info('Deleting job %d with HTCondor id %d', job.id, job.backend_id)
        (return_code, msg) = backend.delete_job(job.user.username, [job.backend_id])
        if return_code == 0:
            job.updated = False
            job.save(update_fields=['updated'])
        else:
            logger.error('Unable to delete job')

def submit_new_jobs():
    """
    Submit new jobs to HTCondor
    """
    backend = ProminenceBackend(server.settings.CONFIG)

    # Find newly submitted jobs
    jobs = Job.objects.filter(Q(status=0))

    for job in jobs:
        # Get job JSON description
        try:
            with open(os.path.join(job.sandbox, 'job.json'), 'r') as json_file:
                job_desc = json.load(json_file)
        except Exception as err:
            logger.error('Unable to read job info due to: %s', err)
            continue

        # Set groups
        groups = set_groups_user(job.user)

        # Submit job
        (return_code, data) = backend.create_job(job.user.username,
                                                 ','.join(groups),
                                                 job.user.email,
                                                 job.uuid,
                                                 job_desc)

        # Handle job submission
        if 'id' in data:
            job.status = 1
            job.backend_id = data['id']
            fields = ['backend_id', 'status']

            if 'policies' in job_desc:
                if 'leaveInQueue' in job_desc['policies']:
                    if job_desc['policies']['leaveInQueue']:
                        job.in_queue = True
                        fields.append('in_queue')
            
            job.save(update_fields=fields)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info('Entering main polling loop')

    while True:
        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)

        submit_new_jobs()
        delete_jobs()

        if EXIT_NOW:
            logger.info('Exiting')
            sys.exit(0)

        time.sleep(10)

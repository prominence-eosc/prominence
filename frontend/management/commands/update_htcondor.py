import json
import logging
import os
import re
import sys
import time
import django

from django.core.management.base import BaseCommand
from django.db.models import Q

from frontend.models import Job, Workflow
import server.settings
from server.backend import ProminenceBackend
from server.set_groups import set_groups_user

# Logging
logger = logging.getLogger('update_htcondor')

class Command(BaseCommand):

    def delete_jobs(self):
        """
        Delete jobs
        """
        backend = ProminenceBackend(server.settings.CONFIG)

        # Find newly deleted jobs
        jobs = Job.objects.filter(Q(status=4) & Q(updated=True))

        for job in jobs:
            if job.backend_id:
                logger.info('Deleting job %d with HTCondor id %d', job.id, job.backend_id)
                (return_code, _) = backend.delete_job(job.user.username, [job.backend_id])
            else:
                logger.info('Deleting job %d which has no HTCondor id', job.id)
                return_code = 0

            if return_code == 0 or not job.backend_id:
                job.updated = False
                job.save(update_fields=['updated'])
            else:
                logger.error('Unable to delete job')

    def delete_workflows(self):
        """
        Delete workflows
        """
        backend = ProminenceBackend(server.settings.CONFIG)

        # Find newly deleted workflows
        workflows = Workflow.objects.filter(Q(status=4) & Q(updated=True))

        for workflow in workflows:
            if workflow.backend_id:
                logger.info('Deleting workflow %d with HTCondor id %d', workflow.id, workflow.backend_id)
                (return_code, _) = backend.delete_workflow(workflow.user.username, [workflow.backend_id])
            else:
                logger.info('Deleting workflow %d which has no HTCondor id', workflow.id)
                return_code = 0

            if return_code == 0 or not workflow.backend_id:
                workflow.updated = False
                workflow.save(update_fields=['updated'])
            else:
                logger.error('Unable to delete workflow')

    def submit_new_jobs(self):
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
                logger.info('Submitted job %d with HTCondor id %d', job.id, job.backend_id)

    def rerun_workflows(self):
        """
        Rerun workflows with failed jobs if necessary
        """
        backend = ProminenceBackend(server.settings.CONFIG)

        # Find any workflows to re-run
        workflows = Workflow.objects.filter((Q(status=3) | Q(status=4) | Q(status=5) | Q(status=6)) & Q(updated=True))
        for workflow in workflows:
            if workflow.backend_id:
                logger.info('Re-running workflow with id %d and HTCondor id %d', workflow.id, workflow.backend_id)

                # Set groups
                groups = set_groups_user(workflow.user)

                (return_code, data) = backend.rerun_workflow(workflow.user.username,
                                                             ','.join(groups),
                                                             workflow.user.email,
                                                             workflow.backend_id)
            else:
                logger.info('User request re-running workflow with id %d which has no HTCondor id', workflow.id)
                return_code = 0

            workflow.updated = False
            workflow.save(update_fields=['updated'])

            if return_code != 0:
                if 'error' in data:
                    logger.error('Unable to re-run workflow due to: %s', data['error'])
                else:
                    logger.error('Unable to re-run workflow')

    def submit_new_workflows(self):
        """
        Submit new workflows to HTCondor
        """
        backend = ProminenceBackend(server.settings.CONFIG)

        # Find newly submitted workflows
        workflows = Workflow.objects.filter(Q(status=0))

        for workflow in workflows:
        # Get workflow JSON description
            try:
                with open(os.path.join(workflow.sandbox, 'workflow.json'), 'r') as json_file:
                    workflow_desc = json.load(json_file)
            except Exception as err:
                logger.error('Unable to read workflow info due to: %s', err)
                continue

            # Set groups
            groups = set_groups_user(workflow.user)

            # Submit workflow
            (return_code, data) = backend.create_workflow(workflow.user.username,
                                                          ','.join(groups),
                                                          workflow.user.email,
                                                          workflow.uuid,
                                                          workflow_desc)

            # Handle workflow submission
            if 'id' in data:
                workflow.status = 1
                workflow.backend_id = data['id']
                fields = ['backend_id', 'status']

                if 'policies' in workflow_desc:
                    if 'leaveInQueue' in workflow_desc['policies']:
                        if workflow_desc['policies']['leaveInQueue']:
                            workflow.in_queue = True
                            fields.append('in_queue')

                workflow.save(update_fields=fields)
                logger.info('Submitted workflow %d with HTCondor id %d', workflow.id, workflow.backend_id)

    def handle(self, **options):

        while True:
            self.submit_new_jobs()
            self.delete_jobs()
            self.submit_new_workflows()
            self.delete_workflows()
            self.rerun_workflows()

            time.sleep(10)

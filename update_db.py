import json
import logging
import os
import re
import time
import django

from django.db.models import Q

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "prominence.settings")
django.setup()

# Logging
logger = logging.getLogger('workflows.update_db')

from frontend.models import Job, Workflow

def check_db():
    try:
        django.db.connection.ensure_connection()
    except Exception as err:
        logger.critical('Problem accessing database: %s', err)
        return False
    else:
        if not django.db.connection.is_usable():
            logger.critical('Database connection is not useable')
            return False
    return True

def get_workflow_from_db(workflow_condor_id):
    try:
        workflow = Workflow.objects.get(backend_id=workflow_condor_id)
    except Exception as err:
        logger.error('Unable to find workflow in DB with HTCondor id %d', workflow_condor_id)
        return None

    return workflow

def update_workflow_db(condor_id, status=None, jobs_total=None, jobs_done=None, jobs_failed=None, time_start=None, time_end=None):
    # Get workflow object
    workflow = get_workflow_from_db(condor_id)
    if not workflow:
        return

    # Workflow status
    if status == 3: # STATUS_SUBMITTED
        workflow_status = 2
    elif status == 5:
        workflow_status = 3
    else:
        workflow_status = 3

    if status is not None:
        workflow.status = workflow_status

    if jobs_done is not None and jobs_failed is not None and jobs_total is not None:
        workflow.jobs_total = jobs_total
        workflow.jobs_done = jobs_done
        workflow.jobs_failed = jobs_failed

    if time_end is not None:
        workflow.time_end = time_end

    if time_start is not None:
        workflow.time_start = time_start

    workflow.save()

def update_all_jobs_in_db(workflow_condor_id, status, epoch):
    try:
        workflow = Workflow.objects.get(backend_id=workflow_condor_id)
    except Exception as err:
        logger.error('Unable to find workflow in DB with HTCondor id %d', workflow_condor_id)
        return

    jobs = workflow.jobs
    for job in jobs:
        if job.status != status:
            job.status = status
            job.save()

def find_incomplete_jobs(workflow_condor_id):
    try:
        workflow = Workflow.objects.get(backend_id=workflow_condor_id)
    except Exception as err:
        logger.error('Unable to find workflow in DB with HTCondor id %d', workflow_condor_id)
        return

    jobs = Job.objects.filter(Q(workflow=workflow) & (Q(status=0) | Q(status=1) | Q(status=2)))

    # TODO: do this properly, for now just set status to unknown
    for job in jobs:
        job.status = 7
        job.save()

def update_job_in_db(job_condor_id, status, start_date=None, end_date=None, reason=None, site=None):
    rows = 0

    try:
        if status == 2 and start_date:
            # If job was previously not yet running we need to set the running status and start time
            if not site or site == '':
                rows = Job.objects.filter(backend_id=job_condor_id, status__lt=2).update(status=status, time_start=start_date)
            else:
                rows = Job.objects.filter(backend_id=job_condor_id, status__lt=2).update(status=status, time_start=start_date, site=site)
            # If job was had a terminal status we just set the start time, as the processing of the
            # completed job must have already taken place
            if rows == 0:
                if not site or site == '':
                    rows = Job.objects.filter(backend_id=job_condor_id, status__gt=2).update(time_start=start_date)
                else:
                    rows = Job.objects.filter(backend_id=job_condor_id, status__gt=2).update(time_start=start_date, site=site)
        elif end_date and start_date and reason is not None and site is not None and site is not '':
            rows = Job.objects.filter(backend_id=job_condor_id).update(status=status, status_reason=reason, time_start=start_date, time_end=end_date, site=site)
        elif end_date and start_date and reason is not None:
            rows = Job.objects.filter(backend_id=job_condor_id).update(status=status, status_reason=reason, time_start=start_date, time_end=end_date)
        elif end_date and reason is not None and site is not None and site is not '':
            rows = Job.objects.filter(backend_id=job_condor_id).update(status=status, status_reason=reason, time_end=end_date, site=site)
        elif end_date and reason is not None:
            rows = Job.objects.filter(backend_id=job_condor_id).update(status=status, status_reason=reason, time_end=end_date)
        elif status == 1 and site is not None and site is not '' and reason is not None:
            rows = Job.objects.filter(backend_id=job_condor_id).update(site=site, status_reason=reason)
        elif status == 1 and site is not None and site is not '':
            rows = Job.objects.filter(backend_id=job_condor_id).update(site=site)
        else:
            logger.error('Got unexpected arguments for job %d in update_job_in_db: start_date=%d,end_date=%d,site=%s,status=%d', job_condor_id, start_date, end_date, site, status)
    except Exception as err:
        logger.error('Unable execute job update in DB with HTCondor id %d due to: %s', job_condor_id, err)
        return None

    # If no rows were updated it probably means we tried to update a job which hasn't been added to the DB yet, so
    # we make sure we don't return success
    if rows > 0:
        return True
    return False

def add_job_to_workflow_db(workflow, workflow_condor_id, job_condor_id, job_name, directory, sandbox, epoch, workflow_name, job_json):
    logger.debug('Adding job to DB with workflow id=%d, job id=%d, job name=%s, directory=%s', workflow_condor_id, job_condor_id, job_name, directory)
    # Set job name visible to the user
    # TODO: what happens if the user had specified an underscore in the job name?
    user_job_name = job_name
    if '_' in job_name:
        job_index = int(job_name.split('_')[1])
        job_name_actual = job_name.split('_')[0]
        user_job_name = '%s/%s/%d' % (workflow_name, job_name_actual, job_index)
    else:
        user_job_name = '%s/%s' % (workflow_name, job_name)

    job = Job(user=workflow.user,
              sandbox=sandbox,
              workflow=workflow,
              status=1,
              created=epoch,
              name=user_job_name,
              image=job_json['tasks'][0]['image'],
              backend_id=job_condor_id)
    if 'cmd' in job_json['tasks'][0]:
        job.command = job_json['tasks'][0]['cmd']

    job.save()

def get_job(workflow, workflow_condor_id, job_condor_id, job_name, directory, sandbox, epoch, workflow_name, job_json):
    logger.debug('Adding job to DB with workflow id=%d, job id=%d, job name=%s, directory=%s', workflow_condor_id, job_condor_id, job_name, directory)
    # Set job name visible to the user
    # TODO: what happens if the user had specified an underscore in the job name?
    user_job_name = job_name
    if '_' in job_name:
        job_index = int(job_name.split('_')[1])
        job_name_actual = job_name.split('_')[0]
        user_job_name = '%s/%s/%d' % (workflow_name, job_name_actual, job_index)
    else:
        user_job_name = '%s/%s' % (workflow_name, job_name)

    job = Job(user=workflow.user,
              sandbox='%s/%s' % (sandbox, directory),
              workflow=workflow,
              status=1,
              created=epoch,
              name=user_job_name,
              image=job_json['tasks'][0]['image'],
              backend_id=job_condor_id)
    if 'cmd' in job_json['tasks'][0]:
        job.command = job_json['tasks'][0]['cmd']

    return job

def create_jobs(jobs):
    try:
        Job.objects.bulk_create(jobs, ignore_conflicts=True)
    except Exception as err:
        logger.error('Got exception running bulk create: %s', err)

def create_job(job):
    try:
        job.save()
    except Exception as err:
        logger.error('Got exception running job save: %s', err)

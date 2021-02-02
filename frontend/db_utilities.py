import time
from django.db.models import Q
from frontend.models import Job, JobLabel, Workflow
import server.settings

def get_condor_job_id(user, job_id):
    try:
        job = Job.objects.get(Q(user=user) & Q(id=job_id))
    except Exception:
        return None

    return job.backend_id

def get_condor_workflow_id(user, workflow_id):
    try:
        workflow = Workflow.objects.get(Q(user=user) & Q(id=workflow_id))
    except Exception:
        return None

    return workflow.backend_id

def db_create_job(user, data, uid):
    job = Job(user=user,
              created=time.time(),
              sandbox='%s/%s' % (server.settings.CONFIG['SANDBOX_PATH'], uid))
    if 'name' in data:
        job.name = data['name']

    if 'tasks' in data:
        if data['tasks']:
            if 'image' in data['tasks'][0]:
                job.image = data['tasks'][0]['image']
            if 'cmd' in data['tasks'][0]:
                job.command = data['tasks'][0]['cmd']

    job.save()

    # Add any labels if necessary
    if 'labels' in data:
        for key in data['labels']:
            label = JobLabel(job=job, key=key, value=data['labels'][key])
            label.save()

    return job

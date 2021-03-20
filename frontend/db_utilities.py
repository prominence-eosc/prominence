import time
from django.db.models import Q
from frontend.models import Job, JobLabel, Workflow
import server.settings

def get_job(user, job_id):
    try:
        job = Job.objects.get(Q(user=user) & Q(id=job_id))
    except Exception:
        return None

    return job

def get_workflow(user, workflow_id):
    try:
        workflow = Workflow.objects.get(Q(user=user) & Q(id=workflow_id))
    except Exception:
        return None

    return workflow

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
              uuid=uid,
              sandbox='%s/%s' % (server.settings.CONFIG['SANDBOX_PATH'], uid))
    if 'name' in data:
        job.name = data['name']

    if 'tasks' in data:
        if data['tasks']:
            if 'image' in data['tasks'][0]:
                job.image = data['tasks'][0]['image']
            if 'cmd' in data['tasks'][0]:
                job.command = data['tasks'][0]['cmd']

    if 'resources' in data:
        if 'cpus' in data['resources']:
            job.request_cpus = int(data['resources']['cpus'])
        if 'memory' in data['resources']:
            job.request_memory = int(data['resources']['memory'])
        if 'disk' in data['resources']:
            job.request_disk = int(data['resources']['disk'])
        if 'nodes' in data['resources']:
            job.request_nodes = int(data['resources']['nodes'])

    job.save()

    # Add any labels if necessary
    if 'labels' in data:
        for key in data['labels']:
            label = JobLabel(job=job, key=key, value=data['labels'][key])
            label.save()

    return job

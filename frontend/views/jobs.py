import os
import logging
import uuid

from django.shortcuts import render, HttpResponse, redirect
from django.http import JsonResponse
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required

from django.db.models import Q

from frontend.models import Job, Workflow
from frontend.serializers import JobDisplaySerializer, JobDetailsDisplaySerializer
from frontend.forms import JobForm, LabelsFormSet, ArtifactsFormSet, EnvVarsFormSet, InputFilesFormSet, OutputFileFormSet, OutputDirectoryFormSet
from server.backend import ProminenceBackend
from server.validate import validate_job
import server.settings
from frontend.utilities import create_job, get_details_from_name
from server.sandbox import create_sandbox, write_json
from frontend.db_utilities import get_job, db_create_job

# Get an instance of a logger
logger = logging.getLogger(__name__)

@login_required
def jobs(request):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)

    workflow = None
    jobs = []

    limit = -1
    if 'limit' in request.GET:
        limit = int(request.GET['limit'])

    workflow_id = -1
    if 'workflow_id' in request.GET:
        if int(request.GET.get('workflow_id')) > -1:
            workflow_id = int(request.GET.get('workflow_id'))

    active = False
    completed = False

    if 'state' in request.GET:
        if request.GET['state'] == 'active':
            active = True
            completed = False
        if request.GET['state'] == 'completed':
            completed = True
            active = False
        if request.GET['state'] == 'all':
            active = True
            completed = True
    else:
        active = True

    state_selectors = {}
    state_selectors['active'] = ''
    state_selectors['completed'] = ''
    state_selectors['all'] = ''
    state = 'active'
    if active and not completed:
        state_selectors['active'] = 'checked'
        limit = -1
    if active and completed:
        state_selectors['all'] = 'checked'
        state = 'all'
    if not active and completed:
        state_selectors['completed'] = 'checked'
        state = 'completed'

    constraints = {}
    name_constraint = None
    search = ''
    if 'fq' in request.GET:
        fq = request.GET['fq']
        search = fq
        if fq != '':
            if '=' in fq:
                pieces = fq.split(',')
                for constraint in pieces:
                    if '=' in constraint:
                        bits = constraint.split('=')
                        if len(bits) == 2:
                            constraints[bits[0]] = bits[1]
            else:
                name_constraint = fq

    if 'json' in request.GET:
        # Define query
        if active and completed:
            query = Q(user=request.user)
        elif active and not completed:
            query = Q(user=request.user) & (Q(status=0) | Q(status=1) | Q(status=2) | Q(in_queue=True))
        else:
            query = Q(user=request.user) & (Q(status=3) | Q(status=4) | Q(status=5) | Q(status=6) | Q(in_queue=True))

        if workflow_id != -1:
            workflow = Workflow.objects.get(id=workflow_id)
            if workflow:
                query = Q(user=request.user) & Q(workflow=workflow)
                limit = -1

        if constraints:
            for constraint in constraints:
                query = query & Q(labels__key=constraint) & Q(labels__value=constraints[constraint])

        if name_constraint:
            query = query & Q(name__contains=name_constraint)

        if limit > 0:
            if completed:
                jobs = Job.objects.filter(query).order_by('-id')[0:limit]
            else:
                jobs = Job.objects.filter(query).order_by('id')[0:limit]
        else:
            if completed:
                jobs = Job.objects.filter(query).order_by('-id')
            else:
                jobs = Job.objects.filter(query).order_by('id')

        serializer = JobDisplaySerializer(jobs, many=True)
        jobs_list = serializer.data

        jobs_table = []
        for job in jobs_list:
            new_job = {}
            new_job['id'] = job['id']
            new_job['name'] = job['name']
            new_job['status'] = job['status']
            new_job['createTime'] = job['events']['createTime']
            new_job['elapsedTime'] = job['elapsedTime']
            new_job['image'] = job['tasks'][0]['image']
            if 'cmd' in job['tasks'][0]:
                new_job['cmd'] = job['tasks'][0]['cmd']
            else:
                new_job['cmd'] = ''
            jobs_table.append(new_job)
        return JsonResponse({'data': jobs_table})
    else:
        return render(request,
                      'jobs.html',
                      {'search': search,
                       'state_selectors': state_selectors,
                       'state': state, 
                       'workflow_id': workflow_id})

@login_required
def job_create(request):
    if request.method == 'POST':
        form = JobForm(request.user, request.POST)
        labels_formset = LabelsFormSet(request.POST, prefix='fs1')
        envvars_formset = EnvVarsFormSet(request.POST, prefix='fs2')
        inputs_formset = InputFilesFormSet(request.POST, request.FILES, prefix='fs4')
        artifacts_formset = ArtifactsFormSet(request.POST, prefix='fs3')
        output_file_formset = OutputFileFormSet(request.POST, prefix='fs5')
        output_dir_formset = OutputDirectoryFormSet(request.POST, prefix='fs6')

        if form.is_valid() and labels_formset.is_valid() and artifacts_formset.is_valid() and envvars_formset.is_valid() and inputs_formset.is_valid() and output_file_formset.is_valid() and output_dir_formset.is_valid():
            job_uuid = str(uuid.uuid4())
            storage_list = request.user.storage_systems.all()
            job_desc = create_job(form.cleaned_data, envvars_formset, labels_formset, request.FILES, artifacts_formset, output_file_formset, output_dir_formset, storage_list, job_uuid)
            user_name = request.user.username
            backend = ProminenceBackend(server.settings.CONFIG)

            # Validate job
            (job_status, msg) = validate_job(job_desc)
            #if not job_status:
            # TODO: message that job is invalid

            # Create sandbox & write JSON job description
            status = create_sandbox(job_uuid, server.settings.CONFIG['SANDBOX_PATH'])
            status = write_json(job_desc, os.path.join(server.settings.CONFIG['SANDBOX_PATH'], job_uuid), 'job.json')

            # Add job to DB
            job = db_create_job(request.user, job_desc, job_uuid)

            return redirect('/jobs')
    else:
        form = JobForm(request.user)
        labels_formset = LabelsFormSet(prefix='fs1')
        envvars_formset = EnvVarsFormSet(prefix='fs2')
        inputs_formset = InputFilesFormSet(prefix='fs4')
        artifacts_formset = ArtifactsFormSet(prefix='fs3')
        output_file_formset = OutputFileFormSet(prefix='fs5')
        output_dir_formset = OutputDirectoryFormSet(prefix='fs6')
        storage_list = request.user.storage_systems.all()

    return render(request, 'job-create.html', {'form': form,
                                               'storage': storage_list,
                                               'envvars_formset': envvars_formset,
                                               'labels_formset': labels_formset,
                                               'artifacts_formset': artifacts_formset,
                                               'inputs_formset': inputs_formset,
                                               'output_file_formset': output_file_formset,
                                               'output_dir_formset': output_dir_formset})

@login_required
def jobs_delete(request, pk=None):
    data = dict()
    if request.method == 'POST':
        data['form_is_valid'] = True
        user_name = request.user.username
        backend = ProminenceBackend(server.settings.CONFIG)

        if 'ids[]' in request.POST:
            jobs = request.POST.getlist('ids[]')
            for pk in jobs:
                logger.info('Deleting job: %d', pk)
                try:
                    job = Job.objects.get(Q(user=request.user) & Q(id=pk))
                except Exception:
                    # TODO: message if unsuccessful
                    pass
                else:
                    if job:
                        job.status = 4
                        job.status_reason = 16
                        job.updated = True
                        job.save(update_fields=['status', 'status_reason', 'updated'])
    else:
        context = {}
        if pk:
            context['job'] = pk
        else:
            jobs = request.GET.getlist('ids[]')
            context['jobs'] = jobs
            context['jobs_display'] = ', '.join(jobs)

        data['html_form'] = render_to_string('job-delete.html', context, request=request)
    return JsonResponse(data)

@login_required
def job_actions(request):
    data = dict()
    if request.method == 'POST':
        if 'job_remove' in request.POST:
            job_id = int(request.POST['job_remove'])
            rows = 0
            try:
                rows = Job.objects.filter(id=job_id, user=request.user).update(in_queue=False)
            except Exception:
                pass
        if 'id' in request.POST:
            for item in request.POST.getlist('id'):
                logger.info('SELDATA=%s', item)

    return redirect('/jobs')

@login_required
def job_describe(request, pk):
    query = Q(user=request.user, id=pk)
    jobs = Job.objects.filter(query)
    serializer = JobDetailsDisplaySerializer(jobs, many=True)
    jobs_list = serializer.data

    if len(jobs_list) == 1:
        return render(request, 'job-info.html', {'job': jobs_list[0]})
    return JsonResponse({})

@login_required
def job_json(request, pk):
    query = Q(user=request.user, id=pk)
    jobs = Job.objects.filter(query)
    serializer = JobDetailsSerializer(jobs, many=True)
    jobs_list = serializer.data

    if len(jobs_list) == 1:
        return JsonResponse(jobs_list[0])
    return JsonResponse({})

@login_required
def job_logs(request, pk):
    job = get_job(request.user, pk)
    if not job:
        return HttpResponse('No such job or not authorised to access job')

    name = None
    instance = 0
    if job.workflow:
        (name, instance) = get_details_from_name(job.name)

    backend = ProminenceBackend(server.settings.CONFIG)
    stdout = backend.get_stdout(job.sandbox, name, instance)
    stderr = backend.get_stderr(job.sandbox, name, instance)

    if not stdout:
        stdout = ''
    if not stderr:
        stderr = ''

    return render(request, 'job-logs.html', {'job_id': pk, 'stdout': stdout, 'stderr': stderr})

from datetime import datetime, timedelta
import os
import logging
import uuid
import requests
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session

from django.shortcuts import render, get_object_or_404, HttpResponse, redirect
from django.http import JsonResponse, HttpResponseRedirect
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.authtoken.models import Token

from django.db.models import Q

from frontend.models import Job, JobLabel, Workflow, WorkflowLabel
from frontend.serializers import JobSerializer, JobDisplaySerializer, JobDetailsSerializer, WorkflowDetailsSerializer, WorkflowDisplaySerializer

from frontend.forms import ComputeForm, StorageForm, JobForm, LabelsFormSet, ArtifactsFormSet, EnvVarsFormSet, InputFilesFormSet, OutputFileFormSet, OutputDirectoryFormSet
from frontend.models import Compute, Storage
from server.backend import ProminenceBackend
from server.validate import validate_job
from server.set_groups import set_groups
import server.settings
from frontend.utilities import create_job, get_details_from_name
from server.sandbox import create_sandbox, write_json
from frontend.metrics import JobMetrics, JobMetricsByCloud, JobResourceUsageMetrics
from frontend.db_utilities import get_condor_job_id, get_job, db_create_job

# Get an instance of a logger
logger = logging.getLogger(__name__)

def index(request):
    if request.user.is_authenticated:
        xaxis_min = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        xaxis_max = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        return render(request, 'home.html', {'xaxis_min': xaxis_min, 'xaxis_max': xaxis_max})
    else:
        return render(request, 'index.html')

def terms_of_use(request):
    return render(request, 'terms-of-use.html')

def privacy_policy(request):
    return render(request, 'privacy-policy.html')

@login_required
def storage(request):
    storage_list = request.user.storage_systems.all()
    return render(request, 'storage.html', {'storage': storage_list})

def save_storage_form(request, form, template_name):
    data = dict()
    if request.method == 'POST':
        if form.is_valid():
            storage = form.save(commit=False)
            storage.user = request.user
            storage.save()
            data['form_is_valid'] = True
            storage_list = request.user.storage_systems.all()
            data['html_storage_list'] = render_to_string('storage-list.html', {'storage': storage_list})
        else:
            data['form_is_valid'] = False
    context = {'form': form}
    data['html_form'] = render_to_string(template_name, context, request=request)
    return JsonResponse(data)

@login_required
def storage_add(request):
    if request.method == 'POST':
        form = StorageForm(request.POST)
        if form.is_valid():
            storage = form.save(commit=False)
            storage.user = request.user
            storage.save()
        return redirect('/storage')
    else:
        form = StorageForm()

    return render(request, 'storage-add.html', {'form': form})

@login_required
def storage_update(request, pk):
    storage = get_object_or_404(Storage, user=request.user, pk=pk)
    if request.method == 'POST':
        form = StorageForm(request.POST, instance=storage)
        if form.is_valid():
            storage = form.save(commit=False)
            storage.user = request.user
            storage.save()
        return redirect('/storage')
    else:
        form = StorageForm(instance=storage)
    return render(request, 'storage-update.html', {'form': form, 'id': pk})

@login_required
def storage_delete(request, pk):
    storage = get_object_or_404(Storage, user=request.user, pk=pk)
    data = dict()
    if request.method == 'POST':
        storage.delete()
        data['form_is_valid'] = True
        storage_list = request.user.storage_systems.all()
        data['html_storage_list'] = render_to_string('storage-list.html', {'storage': storage_list})
    else:
        context = {'storage': storage}
        data['html_form'] = render_to_string('storage-delete.html', context, request=request)
    return JsonResponse(data)

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
def workflows(request):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)

    limit = -1
    if 'limit' in request.GET:
        limit = int(request.GET['limit'])

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
        if active and completed:
            query = Q(user=request.user)
        elif active and not completed:
            query = Q(user=request.user) & (Q(status=0) | Q(status=1) | Q(status=2))
        else:
            query = Q(user=request.user) & (Q(status=3) | Q(status=4) | Q(status=5) | Q(status=6))

        if constraints:
            for constraint in constraints:
                query = query & Q(labels__key=constraint) & Q(labels__value=constraints[constraint])

        if name_constraint:
            query = query & Q(name__contains=name_constraint)

        if limit > 0:
            if completed:
                workflows = Workflow.objects.filter(query).order_by('-id')[0:limit]
            else:
                workflows = Workflow.objects.filter(query).order_by('id')[0:limit]
        else:
            if completed:
                workflows = Workflow.objects.filter(query).order_by('-id')
            else:
                workflows = Workflow.objects.filter(query).order_by('id')

        serializer = WorkflowDisplaySerializer(workflows, many=True)
        workflows_list = serializer.data

        workflows_table = []
        for workflow in workflows_list:
            new_workflow = {}
            new_workflow['id'] = workflow['id']
            new_workflow['name'] = workflow['name']
            new_workflow['status'] = workflow['status']
            new_workflow['createTime'] = workflow['events']['createTime']
            new_workflow['elapsedTime'] = workflow['elapsedTime']
            new_workflow['progress'] = {}
            new_workflow['progress']['done'] = workflow['progress']['done']
            new_workflow['progress']['failed'] = workflow['progress']['failed']
            new_workflow['progress']['total'] = workflow['progress']['total']
            new_workflow['progress']['donePercentage'] = workflow['progress']['donePercentage']
            new_workflow['progress']['failedPercentage'] = workflow['progress']['failedPercentage']
            workflows_table.append(new_workflow)
        return JsonResponse({'data': workflows_table})
    else:
        return render(request,
                      'workflows.html',
                      {'search': search,
                       'state_selectors': state_selectors,
                       'state': state})

@login_required
def create_token(request):
    """
    Generate a new API access token for the User.
    """
    user_model = get_user_model()
    user_name = request.user.username
    user = user_model.objects.get_by_natural_key(user_name)
    Token.objects.filter(user=user).delete()
    token = Token.objects.get_or_create(user=user)
    context = {
        'token': token[0]
    }
    return HttpResponse('Your token is: %s' % token[0])

@login_required
def revoke_token(request):
    """
    Revoke an existing API access token for the User.
    """
    user_model = get_user_model()
    user_name = request.user.username
    user = user_model.objects.get_by_natural_key(user_name)
    Token.objects.filter(user=user).delete()
    return HttpResponse('Your token has been revoked')

@login_required
def compute(request):
    compute_list = request.user.resources.all()
    return render(request, 'clouds.html', {'resources': compute_list})

@login_required
def compute_add(request):
    if request.method == 'POST':
        form = ComputeForm(request.POST)
        if form.is_valid():
            compute = form.save(commit=False)
            compute.user = request.user
            compute.save()
        return redirect('/compute')
    else:
        form = ComputeForm()

    return render(request, 'compute-add.html', {'form': form})

@login_required
def compute_update(request, pk):
    compute = get_object_or_404(Compute, user=request.user, pk=pk)
    if request.method == 'POST':
        form = ComputeForm(request.POST, instance=compute)
        if form.is_valid():
            compute = form.save(commit=False)
            compute.user = request.user
            compute.save()
        return redirect('/compute')
    else:
        form = ComputeForm(instance=compute)
    return render(request, 'compute-update.html', {'form': form, 'id': pk})

@login_required
def compute_delete(request, pk):
    compute = get_object_or_404(Compute, user=request.user, pk=pk)
    data = dict()
    if request.method == 'POST':
        compute.delete()
        data['form_is_valid'] = True
        resources = request.user.resources.all()
        data['html_resources_list'] = render_to_string('clouds-list.html', {'resources': resources})
    else:
        context = {'resource': compute}
        data['html_form'] = render_to_string('compute-delete.html', context, request=request)
    return JsonResponse(data)

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
            storage = request.user.storage_systems.all()
            job_desc = create_job(form.cleaned_data, envvars_formset, labels_formset, request.FILES, artifacts_formset, output_file_formset, output_dir_formset, storage, job_uuid)
            user_name = request.user.username
            backend = ProminenceBackend(server.settings.CONFIG)

            # Set groups
            groups = set_groups(request)

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
    serializer = JobDetailsSerializer(jobs, many=True)
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
def job_usage(request, pk):
    try:
        job = Job.objects.get(id=pk, user=request.user)
    except:
        pass

    if job:
        # TODO: need to specify the range in a better way than this
        metrics = JobResourceUsageMetrics(server.settings.CONFIG)
        return JsonResponse(metrics.get_job(job.backend_id, 20160))
    return JsonResponse({})

@login_required
def user_usage(request):
    user_name = request.user.username
    if 'by-resource' in request.GET:
        metrics = JobMetricsByCloud(server.settings.CONFIG)       
    else:
        metrics = JobMetrics(server.settings.CONFIG)
    return JsonResponse(metrics.get_jobs(user_name, 1440))

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

@login_required
def workflow(request, pk):
    backend = ProminenceBackend(server.settings.CONFIG)

    if request.method == 'GET':
        query = Q(user=request.user, id=pk)
        workflows = Workflow.objects.filter(query)
        serializer = WorkflowDetailsSerializer(workflows, many=True)
        workflows_list = serializer.data

        if len(workflows_list) == 1:
            return render(request, 'workflow-info.html', {'workflow': workflows_list[0]})
        return JsonResponse({})

@login_required
def workflow_actions(request):
    data = dict()
    if request.method == 'POST':
        if 'rerun' in request.POST:
            workflow_id = int(request.POST['rerun'])
            try:
                workflow = Workflow.objects.get(Q(user=request.user) & Q(id=workflow_id))
            except Exception:
                return redirect('/workflows')
            if workflow:
                groups = set_groups(request)
                backend = ProminenceBackend(server.settings.CONFIG)
                (return_code, data) = backend.rerun_workflow(request.user.username,
                                                             ','.join(groups),
                                                             request.user.email,
                                                             workflow.backend_id)
                if 'id' in data:
                    workflow.status = 2
                    workflow.backend_id = int(data['id'])
                    workflow.save()

    return redirect('/workflows')

@login_required
def workflows_delete(request, pk=None):
    data = dict()
    if request.method == 'POST':
        data['form_is_valid'] = True
        user_name = request.user.username
        backend = ProminenceBackend(server.settings.CONFIG)

        if 'ids[]' in request.POST:
            workflows = request.POST.getlist('ids[]')
            for pk in workflows:
                logger.info('Deleting workflow: %d', pk)
                try:
                    workflow = Workflow.objects.get(Q(user=request.user) & Q(id=pk))
                except Exception:
                    # TODO: message if unsuccessful
                    pass
                else:
                    if workflow:
                        workflow.status = 4
                        workflow.updated = True
                        workflow.save(update_fields=['status', 'updated'])
    else:
        context = {}
        if pk:
            context['workflow'] = pk
        else:
            workflows = request.GET.getlist('ids[]')
            context['workflows'] = workflows
            context['workflows_display'] = ', '.join(workflows)

        data['html_form'] = render_to_string('workflow-delete.html', context, request=request)
    return JsonResponse(data)

@login_required
def refresh_authorise(request):
    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'],
                             scope=server.settings.CONFIG['SCOPES'],
                             redirect_uri=request.build_absolute_uri(reverse('refresh_callback')))
    authorization_url, state = identity.authorization_url(server.settings.CONFIG['AUTHORISATION_BASE_URL'],
                                                          access_type="offline",
                                                          prompt="select_account")
    request.session['refresh_oauth_state'] = state
    return redirect(authorization_url)

@login_required
def refresh_callback(request):
    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'],
                             redirect_uri=request.build_absolute_uri(reverse('refresh_callback')),
                             state=request.session['refresh_oauth_state'])
    token = identity.fetch_token(server.settings.CONFIG['TOKEN_URL'],
                                 client_secret=server.settings.CONFIG['CLIENT_SECRET'],
                                 authorization_response=request.build_absolute_uri())

    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'], token=token)
    #userinfo = identity.get(server.settings.CONFIG['OIDC_BASE_URL'] + 'userinfo').json()

    data = {}
    data['username'] = request.user.username
    data['refresh_token'] = token['refresh_token']
        
    try:
        response = requests.post(server.settings.CONFIG['IMC_URL'],
                                 timeout=5,
                                 json=data,
                                 auth=HTTPBasicAuth(server.settings.CONFIG['IMC_USERNAME'],
                                 server.settings.CONFIG['IMC_PASSWORD']),
                                 cert=(server.settings.CONFIG['IMC_SSL_CERT'],
                                       server.settings.CONFIG['IMC_SSL_KEY']),
                                 verify=server.settings.CONFIG['IMC_SSL_CERT'])
    except (requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
        logger.critical('Unable to update refresh token due to: %s', err)
        pass

    return redirect('/')

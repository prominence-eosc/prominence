from datetime import datetime, timedelta
import uuid
import requests
from requests.auth import HTTPBasicAuth

from django.shortcuts import render, get_object_or_404, HttpResponse, redirect
from django.http import JsonResponse, HttpResponseRedirect
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token

from .forms import ComputeForm, StorageForm, JobForm, LabelsFormSet, ArtifactsFormSet, EnvVarsFormSet, InputFilesFormSet
from .models import Compute, Storage
from server.backend import ProminenceBackend
from server.validate import validate_job
import server.settings
from .utilities import create_job
from .metrics import JobMetrics, JobMetricsByCloud, JobResourceUsageMetrics

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
    if 'workflow_id' in request.GET:
        jobs = [int(request.GET.get('workflow_id'))]
        workflow = True

    limit = -1
    if 'limit' in request.GET:
        limit = int(request.GET['limit'])

    active = False
    if 'active' in request.GET:
        if request.GET['active'] == 'true':
            active = True

    completed = False
    if 'completed' in request.GET:
        if request.GET['completed'] == 'true':
            completed = True
            if limit == -1:
                limit = 1

    if not active and not completed:
        active = True

    state_selectors = {}
    state_selectors['active'] = ''
    state_selectors['completed'] = ''
    state_selectors['all'] = ''
    if (active and not completed) or (not active and not completed):
        state_selectors['active'] = 'checked'
    elif active and completed:
        state_selectors['all'] = 'checked'
    elif not active and completed:
        state_selectors['completed'] = 'checked'

    constraint = ()
    name_constraint = None
    search = ''
    if 'fq' in request.GET:
        fq = request.GET['fq']
        search = fq
        if ':' in fq:
            pieces = fq.split(':')
            if len(pieces) == 2:
                constraint = (pieces[0], pieces[1])
        else:
            name_constraint = fq

    jobs_list = backend.list_jobs(jobs, user_name, active, completed, workflow, limit, False, constraint, name_constraint, True)
    return render(request, 'jobs.html', {'job_list': jobs_list, 'search': search, 'state_selectors': state_selectors})

@login_required
def workflows(request):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)

    limit = -1
    if 'limit' in request.GET:
        limit = int(request.GET['limit'])

    active = False
    if 'active' in request.GET:
        if request.GET['active'] == 'true':
            active = True

    completed = False
    if 'completed' in request.GET:
        if request.GET['completed'] == 'true':
            completed = True
            if limit == -1:
                limit = 1

    if not active and not completed:
        active = True

    state_selectors = {}
    state_selectors['active'] = ''
    state_selectors['completed'] = ''
    state_selectors['all'] = ''
    if (active and not completed) or (not active and not completed):
        state_selectors['active'] = 'checked'
    elif active and completed:
        state_selectors['all'] = 'checked'
    elif not active and completed:
        state_selectors['completed'] = 'checked'

    constraint = ()
    name_constraint = None
    search = ''
    if 'fq' in request.GET:
        fq = request.GET['fq']
        search = fq
        if ':' in fq:
            pieces = fq.split(':')
            if len(pieces) == 2:
                constraint = (pieces[0], pieces[1])
        else:
            name_constraint = fq

    workflows_list = backend.list_workflows([], user_name, active, completed, limit, False, constraint, name_constraint, True)
    return render(request, 'workflows.html', {'workflow_list': workflows_list, 'search': search, 'state_selectors': state_selectors})

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
def register_token(request):
    """
    Register a refresh token to use with EGI FedCloud sites
    """
    user = request.user
    account = user.socialaccount_set.get(provider="egicheckin")
    refresh_token = account.socialtoken_set.first().token_secret

    data = {}
    data['username'] = request.user.username
    data['refresh_token'] = refresh_token

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
        return HttpResponse('Error: %s', err)

    return HttpResponse('')

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
        form = JobForm(request.POST)
        labels_formset = LabelsFormSet(request.POST, prefix='fs1')
        envvars_formset = EnvVarsFormSet(request.POST, prefix='fs2')
        inputs_formset = InputFilesFormSet(request.POST, request.FILES, prefix='fs4')
        artifacts_formset = ArtifactsFormSet(request.POST, prefix='fs3')

        if form.is_valid() and labels_formset.is_valid() and artifacts_formset.is_valid() and envvars_formset.is_valid() and inputs_formset.is_valid():
            job_uuid = str(uuid.uuid4())
            storage = request.user.storage_systems.all()
            job_desc = create_job(form.cleaned_data, envvars_formset, labels_formset, request.FILES, artifacts_formset, storage, job_uuid)
            user_name = request.user.username
            backend = ProminenceBackend(server.settings.CONFIG)

            # Validate job
            (job_status, msg) = validate_job(job_desc)
            #if not job_status:
            # TODO: message that job is invalid

            # Submit job
            (return_code, msg) = backend.create_job(user_name, 'group', 'email', job_uuid, job_desc)
            # TODO: if return code not zero, return message to user

            return redirect('/jobs')
    else:
        form = JobForm()
        labels_formset = LabelsFormSet(prefix='fs1')
        envvars_formset = EnvVarsFormSet(prefix='fs2')
        inputs_formset = InputFilesFormSet(prefix='fs4')
        artifacts_formset = ArtifactsFormSet(prefix='fs3')

    return render(request, 'job-create.html', {'form': form,
                                               'envvars_formset': envvars_formset,
                                               'labels_formset': labels_formset,
                                               'artifacts_formset': artifacts_formset,
                                               'inputs_formset': inputs_formset})

@login_required
def job_delete(request, pk):
    data = dict()
    if request.method == 'POST':
        data['form_is_valid'] = True
        user_name = request.user.username
        backend = ProminenceBackend(server.settings.CONFIG)
        (return_code, msg) = backend.delete_job(request.user.username, [pk])
        # TODO: message if unsuccessful deletion?
        jobs_list = backend.list_jobs([], user_name, True, False, None, -1, False, [], None, True)
        data['html_jobs_list'] = render_to_string('jobs-list.html', {'job_list': jobs_list})
    else:
        job = {}
        job['id'] = pk
        context = {'job': job}
        data['html_form'] = render_to_string('job-delete.html', context, request=request)
    return JsonResponse(data)

@login_required
def job_describe(request, pk):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)
    jobs_list = backend.list_jobs([pk], user_name, True, True, None, -1, True, [], None, True)
    if len(jobs_list) == 1:
        return render(request, 'job-info.html', {'job': jobs_list[0]})
    return JsonResponse({})

@login_required
def job_json(request, pk):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)
    jobs_list = backend.list_jobs([pk], user_name, True, True, None, -1, True, [], None, True)
    if len(jobs_list) == 1:
        return JsonResponse(jobs_list[0])
    return JsonResponse({})

@login_required
def job_usage(request, pk):
    metrics = JobResourceUsageMetrics(server.settings.CONFIG)
    # TODO: need to specify the range in a better way than this
    return JsonResponse(metrics.get_job(pk, 20160))

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
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(pk)

    if not identity:
        return HttpResponse('No such job')

    if user_name != identity:
        return HttpResponse('Not authorised for this job')

    stdout = backend.get_stdout(uid, iwd, out, err, pk, name)
    stderr = backend.get_stderr(uid, iwd, out, err, pk, name)

    if not stdout:
        stdout = ''
    if not stderr:
        stderr = ''

    return render(request, 'job-logs.html', {'job_id': pk, 'stdout': stdout, 'stderr': stderr})

@login_required
def workflow_describe(request, pk):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)
    workflows_list = backend.list_workflows([pk], user_name, True, True, -1, True, [], None, True)
    if len(workflows_list) == 1:
        return render(request, 'workflow-info.html', {'workflow': workflows_list[0]})
    return JsonResponse({})

@login_required
def workflow_delete(request, pk):
    data = dict()
    if request.method == 'POST':
        data['form_is_valid'] = True
        user_name = request.user.username
        backend = ProminenceBackend(server.settings.CONFIG)
        (return_code, msg) = backend.delete_workflow(request.user.username, [pk])
        # TODO: message if unsuccessful deletion?
        workflows_list = backend.list_workflows([], user_name, True, False, -1, False, [], None, True)
        data['html_workflows_list'] = render_to_string('workflow-list.html', {'workflow_list': workflows_list})
    else:
        workflow = {}
        workflow['id'] = pk
        context = {'workflow': workflow}
        data['html_form'] = render_to_string('workflow-delete.html', context, request=request)
    return JsonResponse(data)

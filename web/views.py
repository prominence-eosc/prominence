import uuid
import requests
from requests.auth import HTTPBasicAuth

from django.shortcuts import render, get_object_or_404, HttpResponse
from django.http import JsonResponse, HttpResponseRedirect
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token

from .forms import StorageForm, JobForm
from .models import Storage
from server.backend import ProminenceBackend
from server.validate import validate_job
import server.settings
from .utilities import create_job

def index(request):
    if request.user.is_authenticated:
        return render(request, 'home.html')
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
    else:
        form = StorageForm()
    return save_storage_form(request, form, 'storage-add.html')

@login_required
def storage_update(request, pk):
    storage = get_object_or_404(Storage, user=request.user, pk=pk)
    if request.method == 'POST':
        form = StorageForm(request.POST, instance=storage)
    else:
        form = StorageForm(instance=storage)
    return save_storage_form(request, form, 'storage-update.html')

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
    jobs_list = backend.list_jobs([], user_name, True, False, None, -1, False, [], None, True)
    return render(request, 'jobs.html', {'job_list': jobs_list})

@login_required
def workflows(request):
    return render(request, 'workflows.html')

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
def clouds(request):
    return render(request, 'clouds.html')

@login_required
def job_add(request):
    if request.method == 'POST':
        form = JobForm(request.POST)
    else:
        form = JobForm()
    return save_job_form(request, form, 'job-add.html')

def save_job_form(request, form, template_name):
    data = dict()
    if request.method == 'POST':
        if form.is_valid():
            data['form_is_valid'] = True # TODO: do we need this?
            job_desc = create_job(form.cleaned_data)
            user_name = request.user.username
            backend = ProminenceBackend(server.settings.CONFIG)

            # Validate job
            (job_status, msg) = validate_job(job_desc)
            #if not job_status:
            # TODO: message that job is invalid

            # Submit job
            (return_code, msg) = backend.create_job(user_name, 'group', 'email', str(uuid.uuid4()), job_desc)
            # TODO: if return code not zero, return message to user

            # Update jobs list
            jobs_list = backend.list_jobs([], user_name, True, False, None, -1, False, [], None, True)
            data['html_jobs_list'] = render_to_string('jobs-list.html', {'job_list': jobs_list})
        else:
            data['form_is_valid'] = False
    context = {'form': form}
    data['html_form'] = render_to_string(template_name, context, request=request)
    return JsonResponse(data)

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
def job_std_streams(request, pk):
    user_name = request.user.username
    backend = ProminenceBackend(server.settings.CONFIG)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(pk)
    stdout = backend.get_stdout(uid, iwd, out, err, pk, name)
    stderr = backend.get_stderr(uid, iwd, out, err, pk, name)

    return render(request, 'job-std-streams.html', {'stdout': stdout, 'stderr': stderr})

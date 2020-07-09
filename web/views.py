from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse, HttpResponseRedirect
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required

from .forms import StorageForm
from .models import Storage

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
            data['html_storage_list'] = render_to_string('storage_list.html', {
                'storage': storage_list
            })
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
    return save_storage_form(request, form, 'storage_add.html')

@login_required
def storage_update(request, pk):
    storage = get_object_or_404(Storage, user=request.user, pk=pk)
    if request.method == 'POST':
        form = StorageForm(request.POST, instance=storage)
    else:
        form = StorageForm(instance=storage)
    return save_storage_form(request, form, 'storage_update.html')

@login_required
def storage_delete(request, pk):
    storage = get_object_or_404(Storage, user=request.user, pk=pk)
    data = dict()
    if request.method == 'POST':
        storage.delete()
        data['form_is_valid'] = True
        storage_list = request.user.storage_systems.all()
        data['html_storage_list'] = render_to_string('storage_list.html', {
            'storage': storage_list
        })
    else:
        context = {'storage': storage}
        data['html_form'] = render_to_string('storage_delete.html', context, request=request)
    return JsonResponse(data)

@login_required
def jobs(request):
    return render(request, 'jobs.html')

@login_required
def workflows(request):
    return render(request, 'workflows.html')

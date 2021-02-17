import logging

from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required

from frontend.forms import StorageForm
from frontend.models import Storage

# Get an instance of a logger
logger = logging.getLogger(__name__)

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

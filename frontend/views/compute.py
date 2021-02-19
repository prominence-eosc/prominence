import logging

from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required

from frontend.models import Compute
from frontend.forms import ComputeForm

# Get an instance of a logger
logger = logging.getLogger(__name__)

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

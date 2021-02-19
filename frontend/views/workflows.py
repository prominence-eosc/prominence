import logging

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.template.loader import render_to_string
from django.contrib.auth.decorators import login_required

from django.db.models import Q

from frontend.models import Workflow
from frontend.serializers import WorkflowDetailsSerializer, WorkflowDisplaySerializer
from server.backend import ProminenceBackend
import server.settings

# Get an instance of a logger
logger = logging.getLogger(__name__)

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
                workflow.updated = True
                workflow.save(update_fields=['updated'])

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

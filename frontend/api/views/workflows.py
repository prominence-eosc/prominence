"""
API views for managing workflows
"""
import re
import time
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response


from django.db.models import Q

from frontend.models import Workflow, WorkflowLabel
from frontend.serializers import WorkflowSerializer, WorkflowDetailsSerializer

from server.backend import ProminenceBackend
from server.validate import validate_workflow
from server.set_groups import set_groups
import server.settings
from frontend.db_utilities import get_condor_workflow_id

def db_create_workflow(user, data, uid):
    workflow = Workflow(user=user,
                        created=time.time(),
                        sandbox='%s/%s' % (server.settings.CONFIG['SANDBOX_PATH'], uid))
    if 'name' in data:
        workflow.name = data['name']

    workflow.save()

    # Add any labels if necessary
    if 'labels' in data:
        for key in data['labels']:
            label = WorkflowLabel(workflow=workflow, key=key, value=data['labels'][key])
            label.save()

    return workflow

def get_workflow_ids(workflow_id, request):
    """
    Get list of workflow ids supplied by user
    """
    workflow_ids = []

    if workflow_id:
        workflow_ids = [workflow_id]

    if 'id' in request.query_params:
        workflow_ids.extend(request.query_params.get('id').split(','))

    return workflow_ids

class WorkflowsView(views.APIView):
    """
    API views for creating, listing and deleting workflows
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [JSONRenderer]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def post(self, request):
        """
        Create a workflow
        """
        # Set job unique identifier
        uid = str(uuid.uuid4())

        # Validate the input JSON
        (workflow_status, msg) = validate_workflow(request.data)
        if not workflow_status:
            return Response({'error': msg}, status=status.HTTP_400_BAD_REQUEST)

        # Set groups
        groups = set_groups(request)

        workflow = db_create_workflow(request.user, request.data, uid)

        # Create workflow
        (return_code, data) = self._backend.create_workflow(request.user.username,
                                                            ','.join(groups),
                                                            request.user.email,
                                                            uid,
                                                            request.data)

        # Return status as appropriate; TODO: handle server error differently to user error?
        http_status = status.HTTP_201_CREATED
        if return_code == 1:
            http_status = status.HTTP_400_BAD_REQUEST
        else:
            if 'id' in data:
                workflow.backend_id = data['id']
                workflow.save()

                # Return id from Django, not HTCondor, to user
                data['id'] = workflow.id

        return Response(data, status=http_status)

    def get(self, request, workflow_id=None):
        """
        List workflows
        """
        # Constraints
        constraint = (None, None)
        if 'constraint' in request.query_params:
            if ':' in request.query_params.get('constraint'):
                if len(request.query_params.get('constraint').split(':')) == 2:
                    constraint = (request.query_params.get('constraint').split(':')[0],
                                  request.query_params.get('constraint').split(':')[1])
                else:
                    return Response({'error': 'invalid constraint'},
                                    status=status.HTTP_400_BAD_REQUEST)
            else:
                return Response({'error': 'invalid constraint'},
                                status=status.HTTP_400_BAD_REQUEST)

        # Constraint on name
        name_constraint = None
        if 'name' in request.query_params:
            name_constraint = request.query_params.get('name')

        # Active and/or completed workflows
        active = True
        completed = False
        limit = -1

        if 'completed' in request.query_params:
            if request.query_params.get('completed') == 'true':
                completed = True
                active = False
                if 'limit' in request.query_params:
                    limit = int(request.query_params.get('limit'))
                else:
                    limit = 1

        if 'all' in request.query_params:
            completed = True
            active = True
            limit = -1

        # Define query
        if active and completed:
            query = Q(user=request.user)
        elif active and not completed:
            query = Q(user=request.user) & (Q(status=0) | Q(status=1) | Q(status=2))
        else:
            query = Q(user=request.user) & (Q(status=3) | Q(status=4) | Q(status=5) | Q(status=6))

        # Get workflow ids
        workflow_ids = get_workflow_ids(workflow_id, request)
        if workflow_ids:
            query_ids = Q(id=workflow_ids[0])
            if len(workflow_ids) > 1:
                for index in range(1, len(workflow_ids)):
                    query_ids = query_ids | Q(id=workflow_ids[index])
            query = query & query_ids

        # Provide detailed information about each workflow if necessary
        detail = False
        if 'detail' in request.query_params:
            detail = True
        elif workflow_ids:
            # If user has specified workflow(s), assume they want details
            detail = True

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

        if detail:
            serializer = WorkflowDetailsSerializer(workflows, many=True)
        else:
            serializer = WorkflowSerializer(workflows, many=True)
        data = serializer.data

        return Response(data)

    def delete(self, request, workflow_id=None):
        """
        Delete a workflow
        """
        workflow_ids = get_workflow_ids(workflow_id, request)

        if not workflow_ids:
            return Response({'error': 'a workflow id or list of workflow ids must be provided'},
                            status=status.HTTP_400_BAD_REQUEST)

        # Create the query
        workflow_query = Q(id=workflow_ids[0])
        if len(workflow_ids) > 1:
            for count in range(1, len(workflow_ids)):
                workflow_query = workflow_query | Q(id=workflow_ids[count])
        query = Q(user=request.user) & workflow_query

        workflows = Workflow.objects.filter(query)

        condor_ids = []
        for workflow in workflows:
            condor_ids.append(workflow.backend_id)
            workflow.status = 4
            workflow.save(update_fields=['status'])

        (return_code, data) = self._backend.delete_workflow(request.user.username, condor_ids)

        if return_code != 0:
            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        return Response(data, status=status.HTTP_200_OK)

    def put(self, request, workflow_id=None):
        """
        Re-run any failed jobs from a workflow
        """
        # Get the workflow
        try:
            workflow = Workflow.objects.get(Q(user=request.user) & Q(id=workflow_id))
        except Exception:
            return Response({}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not workflow:
            return Response({}, status=status.HTTP_400_BAD_REQUEST)

        # Failed jobs can only be re-run from completed workflows
        if workflow.status < 3:
            Response({'error': 'Workflow is not yet complete'}, status=status.HTTP_400_BAD_REQUEST)

        # Re-run the workflow
        (return_code, data) = self._backend.rerun_workflow(request.user.username,
                                                           ','.join(set_groups(request)),
                                                           request.user.email,
                                                           workflow.backend_id)

        if return_code == 1:
            Response(data, status=status.HTTP_400_BAD_REQUEST)

        if 'id' in data:
            workflow.status = 2
            workflow.backend_id = int(data['id'])
            workflow.save(update_fields=['status', 'backend_id'])
            Response({}, status=status.HTTP_200_OK)

        return Response({}, status=status.HTTP_400_BAD_REQUEST)

class WorkflowStdOutView(views.APIView):
    """
    API view for getting job standard output stream from a workflow
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, workflow_id=None, job=None, instance_id=-1):
        """
        Get standard output
        """
        condor_workflow_id = get_condor_workflow_id(request.user, workflow_id)

        if not condor_workflow_id:
            return Response({'error': 'Workflow does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, identity, iwd, _, _, _, _) = self._backend.get_job_unique_id(condor_workflow_id)

        if not job:
            job = 0

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stdout = self._backend.get_stdout(uid, iwd, None, None, -1, job, instance_id)
        if stdout is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return stdout

class WorkflowStdErrView(views.APIView):
    """
    API view for getting job standard error stream from a workflow
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, workflow_id=None, job=None, instance_id=-1):
        """
        Get standard error
        """
        condor_workflow_id = get_condor_workflow_id(request.user, workflow_id)

        if not condor_workflow_id:
            return Response({'error': 'Workflow does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, identity, iwd, _, _, _, _) = self._backend.get_job_unique_id(condor_workflow_id)

        if not job:
            job = 0

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stderr = self._backend.get_stderr(uid, iwd, None, None, -1, job, instance_id)
        if stderr is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return stderr

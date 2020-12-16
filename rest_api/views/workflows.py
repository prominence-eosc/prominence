"""
API views for managing workflows
"""
import re
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from server.backend import ProminenceBackend
from server.validate import validate_workflow
from server.set_groups import set_groups
import server.settings

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
        limit = 1

        if 'completed' in request.query_params:
            if request.query_params.get('completed') == 'true':
                completed = True
                active = False
                if 'limit' in request.query_params:
                    limit = request.query_params.get('limit')

        if 'all' in request.query_params:
            completed = True
            active = True
            limit = -1

        # Get workflow ids
        workflow_ids = get_workflow_ids(workflow_id, request)

        # Provide detailed information about each workflow if necessary
        detail = False
        if 'detail' in request.query_params:
            detail = True

        data = self._backend.list_workflows(workflow_ids,
                                            request.user.username,
                                            active,
                                            completed,
                                            limit,
                                            detail,
                                            constraint,
                                            name_constraint)

        return Response(data)

    def delete(self, request, workflow_id=None):
        """
        Delete a workflow
        """
        workflow_ids = get_workflow_ids(workflow_id, request)

        if not workflow_ids:
            return Response({'error': 'a workflow id or list of workflow ids must be provided'},
                            status=status.HTTP_400_BAD_REQUEST)

        (return_code, data) = self._backend.delete_workflow(request.user.username, workflow_ids)

        if return_code != 0:
            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        return Response(data, status=status.HTTP_200_OK)

    def put(self, request, workflow_id=None):
        """
        Re-run any failed jobs from a workflow
        """
        (return_code, data) = self._backend.rerun_workflow(request.user.username,
                                                           request.user.email,
                                                           workflow_id)

        if return_code == 1:
            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        return Response(data, status=status.HTTP_200_OK)

class WorkflowStdOutView(views.APIView):
    """
    API view for getting job standard output stream from a workflow
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, workflow_id=None, job_name=None):
        """
        Get standard output
        """
        (uid, identity, iwd, _, _, _, _) = self._backend.get_job_unique_id(workflow_id)

        if not job_name:
            job_name = 0

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stdout = self._backend.get_stdout(uid, iwd, None, None, -1, job_name, -1)
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

    def get(self, request, workflow_id=None, job_name=None):
        """
        Get standard error
        """
        (uid, identity, iwd, _, _, _, _) = self._backend.get_job_unique_id(workflow_id)

        if not job_name:
            job_name = 0

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stderr = self._backend.get_stderr(uid, iwd, None, None, -1, job_name, -1)
        if stderr is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return stderr

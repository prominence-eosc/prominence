"""
API views for managing workflows
"""
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from server.backend import ProminenceBackend
from server.validate import validate_workflow
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

        # Set groups TODO: this is not quite right yet
        groups = []
        for entitlement in request.user.entitlements.split(','):
            if 'member' in entitlement:
                groups.append(entitlement)

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
            if '=' in request.query_params.get('constraint'):
                if len(request.query_params.get('constraint').split('=')) == 2:
                    constraint = (request.query_params.get('constraint').split('=')[0],
                                  request.query_params.get('constraint').split('=')[1])
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
        num = 1

        if 'completed' in request.query_params:
            if request.query_params.get('completed') == 'true':
                completed = True
                active = False
                if 'num' in request.query_params:
                    num = request.query_params.get('num')

        if 'all' in request.query_params:
            completed = True
            active = True
            num = -1

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
                                            num,
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


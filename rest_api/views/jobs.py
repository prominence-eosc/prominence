"""
API views for managing jobs
"""
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from server.backend import ProminenceBackend
from server.validate import validate_job
import server.settings

def get_job_ids(job_id, request):
    """
    Get list of job ids supplied by user
    """
    job_ids = []

    if job_id:
        job_ids = [job_id]

    if 'id' in request.query_params:
        job_ids.extend(request.query_params.get('id').split(','))

    return job_ids

class JobsView(views.APIView):
    """
    API views for creating, listing and deleting jobs
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [JSONRenderer]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def post(self, request):
        """
        Create a job
        """
        # Set job unique identifier
        uid = str(uuid.uuid4())

        # Validate the input JSON
        (job_status, msg) = validate_job(request.data)
        if not job_status:
            return Response({'error': msg}, status=status.HTTP_400_BAD_REQUEST)

        # Set groups TODO: this is not quite right yet
        groups = []
        for entitlement in request.user.entitlements.split(','):
            if 'member' in entitlement:
                groups.append(entitlement)

        # Create job
        (return_code, data) = self._backend.create_job(request.user.username,
                                                       ','.join(groups),
                                                       request.user.email,
                                                       uid,
                                                       request.data)

        # Return status as appropriate; TODO: handle server error differently to user error?
        http_status = status.HTTP_201_CREATED
        if return_code == 1:
            http_status = status.HTTP_400_BAD_REQUEST

        return Response(data, status=http_status)

    def get(self, request, job_id=None):
        """
        List jobs
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

        # Active and/or completed jobs
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

        # Select jobs from a workflow if necessary
        workflow = False
        if 'workflow' in request.query_params:
            if request.query_params.get('workflow') == 'true':
                workflow = True
                num = -1

        # Get job ids
        job_ids = get_job_ids(job_id, request)

        # Provide detailed information about each job if necessary
        detail = False
        if 'detail' in request.query_params:
            detail = True

        data = self._backend.list_jobs(job_ids,
                                       request.user.username,
                                       active,
                                       completed,
                                       workflow,
                                       num,
                                       detail,
                                       constraint,
                                       name_constraint)

        return Response(data)

    def delete(self, request, job_id=None):
        """
        Delete a job
        """
        job_ids = get_job_ids(job_id, request)

        if not job_ids:
            return Response({'error': 'a job id or list of job ids must be provided'},
                            status=status.HTTP_400_BAD_REQUEST)

        (return_code, data) = self._backend.delete_job(request.user.username, job_ids)

        if return_code != 0:
            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        return Response(data, status=status.HTTP_200_OK)

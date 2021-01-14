"""
API views for managing jobs
"""
import re
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from server.backend import ProminenceBackend
from server.validate import validate_job
from server.set_groups import set_groups
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

        # Set groups
        groups = set_groups(request)

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

        # Active and/or completed jobs
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

        # Select jobs from a workflow if necessary
        workflow = False
        if 'workflow' in request.query_params:
            if request.query_params.get('workflow') == 'true':
                workflow = True
                limit = -1

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
                                       limit,
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

        return Response(data, status=status.HTTP_204_NO_CONTENT)

class JobStdOutView(views.APIView):
    """
    API view for getting job standard output stream
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, job_id=None):
        """
        Get standard output
        """
        (uid, identity, iwd, out, err, name, _) = self._backend.get_job_unique_id(job_id)

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stdout = self._backend.get_stdout(uid, iwd, out, err, job_id, name)
        if stdout is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return stdout

class JobStdErrView(views.APIView):
    """
    API view for getting job standard error stream
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, job_id=None):
        """
        Get standard error
        """
        (uid, identity, iwd, out, err, name, _) = self._backend.get_job_unique_id(job_id)

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        stderr = self._backend.get_stderr(uid, iwd, out, err, job_id, name)
        if stderr is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return stderr


class JobRemoveFromQueue(views.APIView):
    """
    API view for removing a job from the queue
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def put(self, request, job_id=None):
        """
        Remove job from the queue
        """
        (_, identity, _, _, _, _, _) = self._backend.get_job_unique_id(job_id)

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        if not self._backend.remove_job(job_id):
            return Response({'error': 'Job is no longer in the queue'},
                            status=status.HTTP_400_BAD_REQUEST)

        return Response({}, status=status.HTTP_200_OK)

class JobSnapshot(views.APIView):
    """
    API view for creating and getting snapshots
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def put(self, request, job_id=None):
        """
        Create a snapshot
        """
        if not server.settings.CONFIG['ENABLE_SNAPSHOTS']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, identity, iwd, _, _, _, status) = self._backend.get_job_unique_id(job_id)

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        if status != 2:
            return Response({'error': 'Job is not running'},
                            status=status.HTTP_400_BAD_REQUEST)

        if 'path' in request.query_params:
            path = request.query_params.get('path')
        else:
            return Response({'error': 'Path for snapshot not specified'},
                            status=status.HTTP_400_BAD_REQUEST)

        path = self._backend.validate_snapshot_path(iwd, path)
        if not path:
            return Response({'error': 'Invalid path for shapshot'},
                            status=status.HTTP_400_BAD_REQUEST)

        self._backend.create_snapshot(uid, job_id, path)
        return Response({}, status=status.HTTP_200_OK)
        
    def get(self, request, job_id=None):
        """
        Get a snapshot
        """
        if not server.settings.CONFIG['ENABLE_SNAPSHOTS']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, identity, _, _, _, _, status) = self._backend.get_job_unique_id(job_id)

        if not identity:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        if request.user.username != identity:
            return Response({'error': 'Not authorized to access this job'},
                            status=status.HTTP_403_FORBIDDEN)

        if status != 2:
            return Response({'error': 'Job is not running'},
                            status=status.HTTP_400_BAD_REQUEST)

        url = self._backend.get_snapshot_url(uid)
        return Response({'url': url}, status=status.HTTP_200_OK)

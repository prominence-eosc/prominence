"""
API views for managing jobs
"""
import re
import time
import uuid

from rest_framework.authentication import TokenAuthentication
from rest_framework import status, permissions, views
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework import renderers

from django.db.models import Q

from frontend.models import Job, JobLabel, Workflow
from frontend.serializers import JobSerializer, JobDetailsSerializer

from server.backend import ProminenceBackend
from server.validate import validate_job
from server.set_groups import set_groups
import server.settings
from frontend.db_utilities import get_condor_job_id, db_create_job

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

class PlainTextRenderer(renderers.BaseRenderer):
    media_type = 'text/plain'
    format = 'text'

    def render(self, data, media_type=None, renderer_context=None):
        return data.encode(self.charset)

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
        job = db_create_job(request.user, request.data, uid)

        # TODO: ideally this should be elsewhere
        (return_code, data) = self._backend.create_job(request.user.username,
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
                job.backend_id = data['id']
                if 'policies' in request.data:
                    if 'leaveInQueue' in request.data['policies']:
                        if request.data['policies']['leaveInQueue']:
                            job.in_queue = True
                job.save()

                # Return id from Django, not HTCondor, to user
                data['id'] = job.id

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
            query = Q(user=request.user) & (Q(status=0) | Q(status=1) | Q(status=2) | Q(in_queue=True))
        else:
            query = Q(user=request.user) & (Q(status=3) | Q(status=4) | Q(status=5) | Q(status=6) | Q(in_queue=True))

        # Get job ids
        job_ids = get_job_ids(job_id, request)

        # Select jobs from a workflow if necessary
        workflow = False
        if 'workflow' in request.query_params:
            if request.query_params.get('workflow') == 'true':
                workflow = True
                if 'limit' not in request.query_params:
                    limit = -1
                wf = Workflow.objects.get(id=job_ids[0])
                query = query & Q(workflow=wf)

        if job_ids and not workflow:
            query = query & Q(id=job_ids[0])

        # Constraint on name
        if 'name' in request.query_params:
            query = query & Q(name=request.query_params.get('name'))

        # Constraint on labels
        if constraint:
            query = query & Q(labels__key=constraint[0]) & Q(labels__value=constraint[1])

        # Provide detailed information about each job if necessary
        detail = False
        if 'detail' in request.query_params:
            detail = True
        elif job_ids:
            # If user has specified job(s), assume they want details
            detail = True

        if limit > 0:
            if completed:
                jobs = Job.objects.filter(query).order_by('-id')[0:limit]
            else:
                jobs = Job.objects.filter(query).order_by('id')[0:limit]
        else:
            if completed:
                jobs = Job.objects.filter(query).order_by('-id')
            else:
                jobs = Job.objects.filter(query).order_by('id')

        if detail:
            serializer = JobDetailsSerializer(jobs, many=True)
        else:
            serializer = JobSerializer(jobs, many=True)
        data = serializer.data      

        return Response(data)

    def delete(self, request, job_id=None):
        """
        Delete a job
        """
        job_ids = get_job_ids(job_id, request)

        # At least one job id must be specified
        if not job_ids:
            return Response({'error': 'a job id or list of job ids must be provided'},
                            status=status.HTTP_400_BAD_REQUEST)

        # Create the query
        job_query = Q(id=job_ids[0])
        if len(job_ids) > 1:
            for count in range(1, len(job_ids)):
                job_query = job_query | Q(id=job_ids[count])
        query = Q(user=request.user) & job_query

        jobs = Job.objects.filter(query)

        condor_job_ids = []
        for job in jobs:
            condor_job_ids.append(job.backend_id)
            job.status = 4
            job.status_reason = 16
            job.save(update_fields=['status', 'status_reason'])

        # Delete the jobs from condor
        # TODO: this could be done elsewhere perhaps
        (return_code, data) = self._backend.delete_job(request.user.username, condor_job_ids)

        if return_code != 0:
            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        return Response(data, status=status.HTTP_204_NO_CONTENT)

class JobStdOutView(views.APIView):
    """
    API view for getting job standard output stream
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [PlainTextRenderer]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, job_id=None):
        """
        Get standard output
        """
        condor_job_id = get_condor_job_id(request.user, job_id)

        if not condor_job_id:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, _, iwd, out, err, name, _) = self._backend.get_job_unique_id(condor_job_id)

        stdout = self._backend.get_stdout(uid, iwd, out, err, condor_job_id, name)
        if stdout is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return Response(stdout)

class JobStdErrView(views.APIView):
    """
    API view for getting job standard error stream
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [PlainTextRenderer]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, job_id=None):
        """
        Get standard error
        """
        condor_job_id = get_condor_job_id(request.user, job_id)

        if not condor_job_id:
            return Response({'error': 'Job does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, _, iwd, out, err, name, _) = self._backend.get_job_unique_id(condor_job_id)

        stderr = self._backend.get_stderr(uid, iwd, out, err, condor_job_id, name)
        if stderr is None:
            return Response({'error': 'Standard output does not exist'},
                            status=status.HTTP_400_BAD_REQUEST)

        return Response(stderr)

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
        rows = 0
        try:
            rows = Job.objects.filter(id=job_id, user=request.user).update(in_queue=False)
        except Exception:
            return Response({}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
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

        condor_job_id = get_condor_job_id(request.user, job_id)

        if not condor_job_id:
            return Response({'error': 'Job does not exist or user not authorized to access this job'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, _, iwd, _, _, _, status) = self._backend.get_job_unique_id(condor_job_id)

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

        self._backend.create_snapshot(uid, condor_job_id, path)
        return Response({}, status=status.HTTP_200_OK)
        
    def get(self, request, job_id=None):
        """
        Get a snapshot
        """
        if not server.settings.CONFIG['ENABLE_SNAPSHOTS']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        condor_job_id = get_condor_job_id(request.user, job_id)

        if not condor_job_id:
            return Response({'error': 'Job does not exist or user not authorized to access this job'},
                            status=status.HTTP_400_BAD_REQUEST)

        (uid, _, _, _, _, _, status) = self._backend.get_job_unique_id(condor_job_id)

        if status != 2:
            return Response({'error': 'Job is not running'},
                            status=status.HTTP_400_BAD_REQUEST)

        url = self._backend.get_snapshot_url(uid)
        return Response({'url': url}, status=status.HTTP_200_OK)

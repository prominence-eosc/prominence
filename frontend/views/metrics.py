import logging

from django.http import JsonResponse
from django.contrib.auth.decorators import login_required

from frontend.models import Job
import server.settings
from frontend.metrics import JobMetrics, JobMetricsByCloud, JobResourceUsageMetrics

# Get an instance of a logger
logger = logging.getLogger(__name__)

@login_required
def user_usage(request):
    user_name = request.user.username
    if 'by-resource' in request.GET:
        metrics = JobMetricsByCloud(server.settings.CONFIG)       
    else:
        metrics = JobMetrics(server.settings.CONFIG)
    return JsonResponse(metrics.get_jobs(user_name, 1440))

@login_required
def job_usage(request, pk):
    try:
        job = Job.objects.get(id=pk, user=request.user)
    except:
        pass

    if job:
        # TODO: need to specify the range in a better way than this
        metrics = JobResourceUsageMetrics(server.settings.CONFIG)
        return JsonResponse(metrics.get_job(job.backend_id, 20160))
    return JsonResponse({})

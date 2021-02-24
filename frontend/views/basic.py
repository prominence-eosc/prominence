from datetime import datetime, timedelta
import logging

from django.shortcuts import render

from django.db.models import Q
  
from frontend.models import Job

# Get an instance of a logger
logger = logging.getLogger(__name__)

def get_jobs_overview(user):
    jobs_pending = len(Job.objects.filter(Q(user=user) & (Q(status=0) | Q(status=1))))
    jobs_running = len(Job.objects.filter(Q(user=user) & Q(status=2)))
    return (jobs_pending, jobs_running)

def index(request):
    if request.user.is_authenticated:
        xaxis_min = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        xaxis_max = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        (jobs_pending, jobs_running) = get_jobs_overview(request.user)
        return render(request, 'home.html', {'xaxis_min': xaxis_min,
                                             'xaxis_max': xaxis_max,
                                             'jobs_pending': jobs_pending,
                                             'jobs_running': jobs_running})
    else:
        return render(request, 'index.html')

def terms_of_use(request):
    return render(request, 'terms-of-use.html')

def privacy_policy(request):
    return render(request, 'privacy-policy.html')

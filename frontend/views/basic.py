from datetime import datetime, timedelta
import logging

from django.shortcuts import render

# Get an instance of a logger
logger = logging.getLogger(__name__)

def index(request):
    if request.user.is_authenticated:
        xaxis_min = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        xaxis_max = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        return render(request, 'home.html', {'xaxis_min': xaxis_min, 'xaxis_max': xaxis_max})
    else:
        return render(request, 'index.html')

def terms_of_use(request):
    return render(request, 'terms-of-use.html')

def privacy_policy(request):
    return render(request, 'privacy-policy.html')

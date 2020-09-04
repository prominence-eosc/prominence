from django.urls import path
from . import views

urlpatterns = [
    path('v1/health', views.HealthView.as_view(), name='health'),
    path('v1/jobs', views.JobsView.as_view(), name='jobs'),
    path('v1/jobs/<int:job_id>', views.JobsView.as_view(), name='job'),
    path('v1/jobs/<int:job_id>/stdout', views.JobStdOutView.as_view(), name='job_get_stdout'),
    path('v1/jobs/<int:job_id>/stderr', views.JobStdErrView.as_view(), name='job_get_stderr'),
    path('v1/jobs/<int:job_id>/remove', views.JobRemoveFromQueue.as_view(), name='job_remove_from_queue'),
    path('v1/workflows', views.WorkflowsView.as_view(), name='workflows'),
]

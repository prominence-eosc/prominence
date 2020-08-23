from django.conf.urls import include  
from django.urls import path, re_path
from . import views

urlpatterns = [
    path('v1/jobs', views.JobsView.as_view(), name='jobs'),
    path('v1/jobs/<int:job_id>', views.JobsView.as_view(), name='job'),
#    path('v1/jobs/<int:id>/stdout', views.JobStdOut.as_view(), name='job_get_stdout'),
#    path('v1/jobs/<int:id>/stderr', views.JobStdErr.as_view(), name='job_get_stderr'),
#    path('v1/jobs/<int:id>/remove', views.job_remove_queue, name='job_remove_queue'),
]

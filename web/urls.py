from django.conf.urls import include  
from django.urls import path, re_path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('terms-of-use', views.terms_of_use, name='terms-of-use'),
    path('privacy-policy', views.privacy_policy, name='privacy-policy'),
    re_path(r'^storage/$', views.storage, name='storage'),
    re_path(r'^storage/add/$', views.storage_add, name='storage_add'),
    re_path(r'^storage/(?P<pk>\d+)/update/$', views.storage_update, name='storage_update'),
    re_path(r'^storage/(?P<pk>\d+)/delete/$', views.storage_delete, name='storage_delete'),
    re_path(r'^compute/$', views.compute, name='compute'),
    path(r'jobs/', views.jobs, name='jobs'),
    re_path(r'^jobs/create/$', views.job_create, name='job_create'),
    re_path(r'^jobs/(?P<pk>\d+)/delete/$', views.job_delete, name='job_delete'),
    re_path(r'^jobs/(?P<pk>\d+)$', views.job_describe, name='job_describe'),
    re_path(r'^jobs/(?P<pk>\d+)/logs/$', views.job_logs, name='job_logs'),
    re_path(r'^jobs/(?P<pk>\d+)/json/$', views.job_json, name='job_json'),
    re_path(r'^jobs/(?P<pk>\d+)/usage/$', views.job_usage, name='job_usage'),
    re_path(r'^workflows/$', views.workflows, name='workflows'),
    re_path(r'^workflows/(?P<pk>\d+)/delete/$', views.workflow_delete, name='workflow_delete'),
    re_path(r'^workflows/(?P<pk>\d+)$', views.workflow_describe, name='workflow_describe'),
    re_path(r'^user/usage/$', views.user_usage, name='user-usage'),
    re_path(r'^user/create-token/$', views.create_token, name='create-token'),
    re_path(r'^user/revoke-token/$', views.revoke_token, name='revoke-token'),
    re_path(r'^user/register-token/$', views.register_token, name='register-token'),
]

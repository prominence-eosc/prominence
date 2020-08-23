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
    re_path(r'^jobs/$', views.jobs, name='jobs'),
    re_path(r'^workflows/$', views.workflows, name='workflows'),
]
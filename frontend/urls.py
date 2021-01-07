from django.conf.urls import include  
from django.urls import path, re_path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('terms-of-use', views.terms_of_use, name='terms-of-use'),
    path('privacy-policy', views.privacy_policy, name='privacy-policy'),
    path('storage/', views.storage, name='storage'),
    path('storage/add/', views.storage_add, name='storage_add'),
    path('storage/<int:pk>/update/', views.storage_update, name='storage_update'),
    path('storage/<int:pk>/delete/', views.storage_delete, name='storage_delete'),
    path('compute/', views.compute, name='compute'),
    path('compute/add/', views.compute_add, name='compute_add'),
    path('compute/<int:pk>/update/', views.compute_update, name='compute_update'),
    path('compute/<int:pk>/delete/', views.compute_delete, name='compute_delete'),
    path('jobs/', views.jobs, name='jobs'),
    path('jobs/create/', views.job_create, name='job_create'),
    path('jobs/<int:pk>/delete/', views.job_delete, name='job_delete'),
    path('jobs/<int:pk>', views.job_describe, name='job_describe'),
    path('jobs/<int:pk>/logs/', views.job_logs, name='job_logs'),
    path('jobs/<int:pk>/json/', views.job_json, name='job_json'),
    path('jobs/<int:pk>/usage/', views.job_usage, name='job_usage'),
    path('workflows/', views.workflows, name='workflows'),
    path('workflows/<int:pk>/delete/', views.workflow_delete, name='workflow_delete'),
    path('workflows/<int:pk>', views.workflow, name='workflow'),
    path('user/usage/', views.user_usage, name='user-usage'),
    path('user/create-token/', views.create_token, name='create-token'),
    path('user/revoke-token/', views.revoke_token, name='revoke-token'),
    path('refresh/authorise', views.refresh_authorise, name='refresh_authorise'),
    path('refresh/callback', views.refresh_callback, name='refresh_callback'),
    path('api/', include('frontend.api_urls')),
]

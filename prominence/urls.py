from django.contrib import admin
from django.urls import re_path, path, include
from django.views.generic.base import RedirectView
from allauth.account.views import logout

urlpatterns = [
    re_path(r'^accounts/email/', RedirectView.as_view(url='/', permanent=False)),
    re_path(r'^accounts/password/', RedirectView.as_view(url='/', permanent=False)),
    re_path(r'^accounts/inactive/', RedirectView.as_view(url='/', permanent=False)),
    re_path(r'^accounts/confirm-email/', RedirectView.as_view(url='/', permanent=False)),
    re_path(r'^login/', RedirectView.as_view(url='/', permanent=False)),
    re_path(r'^signup/', RedirectView.as_view(url='/', permanent=False)),
    path('logout/', logout, name='account_logout'),
    path('accounts/egicheckin/', include('egicheckin.urls')),
    path('accounts/', include('allauth.urls')),
    path('admin/', admin.site.urls),
    path('', include('frontend.urls')),
]

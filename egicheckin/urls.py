from allauth.socialaccount.providers.oauth2.urls import default_urlpatterns
  
from .provider import EGICheckInProvider


urlpatterns = default_urlpatterns(EGICheckInProvider)

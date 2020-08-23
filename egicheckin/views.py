import requests

from allauth.socialaccount.providers.oauth2.views import (
    OAuth2Adapter,
    OAuth2CallbackView,
    OAuth2LoginView,
)

from .provider import EGICheckInProvider


class EGICheckInOAuth2Adapter(OAuth2Adapter):
    provider_id = EGICheckInProvider.id
    access_token_url = 'https://aai-dev.egi.eu/oidc/token'
    authorize_url = 'https://aai-dev.egi.eu/oidc/authorize'
    profile_url = 'https://aai-dev.egi.eu/oidc/userinfo'

    def complete_login(self, request, app, token, **kwargs):
        headers = {'Authorization': 'Bearer {0}'.format(token.token)}
        resp = requests.get(self.profile_url, headers=headers)
        extra_data = resp.json()
        return self.get_provider().sociallogin_from_response(request,
                                                             extra_data)


oauth2_login = OAuth2LoginView.adapter_view(EGICheckInOAuth2Adapter)
oauth2_callback = OAuth2CallbackView.adapter_view(EGICheckInOAuth2Adapter)

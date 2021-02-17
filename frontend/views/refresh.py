import logging
import requests
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session

from django.shortcuts import redirect
from django.contrib.auth.decorators import login_required
from django.urls import reverse

import server.settings

# Get an instance of a logger
logger = logging.getLogger(__name__)

@login_required
def refresh_authorise(request):
    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'],
                             scope=server.settings.CONFIG['SCOPES'],
                             redirect_uri=request.build_absolute_uri(reverse('refresh_callback')))
    authorization_url, state = identity.authorization_url(server.settings.CONFIG['AUTHORISATION_BASE_URL'],
                                                          access_type="offline",
                                                          prompt="select_account")
    request.session['refresh_oauth_state'] = state
    return redirect(authorization_url)

@login_required
def refresh_callback(request):
    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'],
                             redirect_uri=request.build_absolute_uri(reverse('refresh_callback')),
                             state=request.session['refresh_oauth_state'])
    token = identity.fetch_token(server.settings.CONFIG['TOKEN_URL'],
                                 client_secret=server.settings.CONFIG['CLIENT_SECRET'],
                                 authorization_response=request.build_absolute_uri())

    identity = OAuth2Session(server.settings.CONFIG['CLIENT_ID'], token=token)
    #userinfo = identity.get(server.settings.CONFIG['OIDC_BASE_URL'] + 'userinfo').json()

    data = {}
    data['username'] = request.user.username
    data['refresh_token'] = token['refresh_token']
        
    try:
        response = requests.post(server.settings.CONFIG['IMC_URL'],
                                 timeout=5,
                                 json=data,
                                 auth=HTTPBasicAuth(server.settings.CONFIG['IMC_USERNAME'],
                                 server.settings.CONFIG['IMC_PASSWORD']),
                                 cert=(server.settings.CONFIG['IMC_SSL_CERT'],
                                       server.settings.CONFIG['IMC_SSL_KEY']),
                                 verify=server.settings.CONFIG['IMC_SSL_CERT'])
    except (requests.exceptions.Timeout, requests.exceptions.RequestException) as err:
        logger.critical('Unable to update refresh token due to: %s', err)
        pass

    return redirect('/')

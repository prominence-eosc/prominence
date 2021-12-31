"""Authorisation functions"""
from __future__ import print_function
from functools import wraps
import time
import requests
import jwt
from flask import jsonify, request
from flask import current_app as app

from .errors import auth_failure, oidc_error
from .utilities import get_remote_addr

def validate_token(token):
    """
    Try to decode the token using the job token secret and return the username and groups if the token is valid
    """
    decoded = None
    try:
        decoded = jwt.decode(token, app.config['JOB_TOKEN_SECRET'], algorithms=["HS256"])
    except Exception as err:
        app.logger.warning('Got exception checking for job token: %s', err)

    if decoded:
        if 'username' in decoded and 'groups' in decoded and 'email' in decoded:
            return (str(decoded['username']), str(decoded['groups']), str(decoded['email']))

        if 'username' in decoded and 'groups' in decoded:
            return (str(decoded['username']), str(decoded['groups']), None)

    return (None, None, None)

def get_expiry(token):
    """
    Get expiry date from a JWT token
    - this is a just a first basic check of validity (and will be improved later), we still check with the OIDC server
    """
    expiry = 0
    try:
        expiry = jwt.decode(token, options={"verify_signature": False})['exp']
    except:
        pass
    return expiry

def get_user_details_with_retries(token):
    success = False
    count = 0
    while not success and count < 5:
        (success, a1, a2, a3, a4) = get_user_details(token)
        count = count + 1
        time.sleep(count*0.2)
    return (success, a1, a2, a3, a4)

def get_user_details(token):
    """
    Get the username and group from a token
    """
    headers = {'Authorization':'Bearer %s' % token}
    try:
        response = requests.get(app.config['OIDC_URL']+'/userinfo', timeout=app.config['OIDC_TIMEOUT'], headers=headers)
    except requests.exceptions.RequestException as err:
        app.logger.warning('%s AuthenticationFailure no response from identity provider: %s' % (get_remote_addr(request), err))
        return (False, None, None, False)

    username = None
    if 'USERNAME_FROM' in app.config:
        if app.config['USERNAME_FROM'] in response.json():
            username = str(response.json()[app.config['USERNAME_FROM']])
    elif 'sub' in response.json():
        username = str(response.json()['sub'])
    elif 'preferred_username' in response.json():
        username = str(response.json()['preferred_username'])

    email = None
    if 'email' in response.json():
        email = response.json()['email']

    groups = None
    if 'groups' in response.json():
        if len(response.json()['groups']) > 0:
            groups = ','.join(str(group) for group in response.json()['groups'])

    allowed = False
    if app.config['REQUIRED_ENTITLEMENTS'] != '':
        if 'edu_person_entitlements' in response.json():
            for vo in app.config['REQUIRED_ENTITLEMENTS']:
                num_required = len(app.config['REQUIRED_ENTITLEMENTS'][vo])
                num_have = 0
                for entitlement in app.config['REQUIRED_ENTITLEMENTS'][vo]:
                    if entitlement in response.json()['edu_person_entitlements']:
                        if 'role=member' in entitlement and not groups:
                            groups = vo
                        num_have += 1
                if num_required == num_have:
                    allowed = True
                    break
    else:
        allowed = True

    return (True, username, groups, email, allowed)

def authenticate():
    """
    Sends a 401 response
    """
    return auth_failure()

def requires_auth(function):
    """
    Check authentication
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        start_time = time.time()
        if 'Authorization' not in request.headers:
            app.logger.warning('%s AuthenticationFailure authorization not in headers' % get_remote_addr(request))
            return authenticate()
        auth = request.headers['Authorization']
        try:
            token = auth.split(' ')[1]
        except:
            app.logger.warning('%s AuthenticationFailure no token specified' % get_remote_addr(request))
            return authenticate()

        # Check token expiry
        if time.time() > get_expiry(token):
            app.logger.warning('%s AuthenticationFailure token has already expired' % get_remote_addr(request))
            return authenticate()

        # Firstly check if token is a job token
        success = False
        (username, group, email) = validate_token(token)
        if username and group:
            success = True
            allowed = True

        # Query OIDC server if necessary
        if not success:
            (success, username, group, email, allowed) = get_user_details(token)

        if not success:
            return oidc_error()

        if not username:
            app.logger.warning('%s AuthenticationFailure username not returned from identity provider' % get_remote_addr(request))
            return authenticate()

        if not allowed:
            app.logger.warning('%s AuthenticationFailure user does not have required entitlements' % get_remote_addr(request))
            return authenticate()

        app.logger.info('%s AuthenticationSuccess user:%s group:%s duration:%d' % (get_remote_addr(request), username, group, time.time() - start_time))

        return function(username, group, email, *args, **kwargs)
    return decorated

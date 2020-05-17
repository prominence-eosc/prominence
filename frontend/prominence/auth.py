"""Authorisation functions"""
from __future__ import print_function
from functools import wraps
import time
import requests
import jwt
from flask import jsonify, request
from flask import current_app as app

import errors
import validate
from utilities import get_remote_addr

def get_expiry(token):
    """
    Get expiry date from a JWT token
    - this is a just a first basic check of validity (and will be improved later), we still check with the OIDC server
    """
    expiry = 0
    try:
        expiry = jwt.decode(token, verify=False)['exp']
    except:
        pass
    return expiry

def get_user_details(token):
    """
    Get the username and group from a token
    """
    headers = {'Authorization':'Bearer %s' % token}
    try:
        response = requests.get(app.config['OIDC_URL']+'/userinfo', timeout=app.config['OIDC_TIMEOUT'], headers=headers)
    except requests.exceptions.RequestException:
        app.logger.warning('%s AuthenticationFailure no response from identity provider' % get_remote_addr(request))
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
        if response.json()['groups'] > 0:
            groups = ','.join(str(group) for group in response.json()['groups'])

    allowed = False
    if app.config['REQUIRED_ENTITLEMENTS'] != '':
        if 'edu_person_entitlements' in response.json():
            for entitlements in app.config['REQUIRED_ENTITLEMENTS']:
                num_required = len(entitlements)
                num_have = 0
                for entitlement in entitlements:
                    if entitlement in response.json()['edu_person_entitlements']:
                        if 'member@' in entitlement and not groups:
                            groups = entitlement.split('@')[1]
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
    return jsonify({'error':'Authentication failure'}), 401

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

        # Query OIDC server
        (success, username, group, email, allowed) = get_user_details(token)

        if not success:
            return jsonify({'error':'Unable to connect to OIDC server'}), 401

        if not username:
            app.logger.warning('%s AuthenticationFailure username not returned from identity provider' % get_remote_addr(request))
            return authenticate()

        if not allowed:
            app.logger.warning('%s AuthenticationFailure user does not have required entitlements' % get_remote_addr(request))
            return authenticate()

        app.logger.info('%s AuthenticationSuccess user:%s group:%s duration:%d' % (get_remote_addr(request), username, group, time.time() - start_time))

        return function(username, group, email, *args, **kwargs)
    return decorated

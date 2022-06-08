"""Routes for the token endpoint"""
import base64
import sys
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .utilities import get_remote_addr

from .auth import requires_auth

from .create_worker_token import create_condor_worker_token

token = Blueprint('token', __name__)

@token.route("/prominence/v1/token", methods=['POST'])
@requires_auth
def create_token(username, group, email):
    """
    Get worker token
    """
    app.logger.info('%s CreateToken user:%s group:%s' % (get_remote_addr(request), username, group))

    token = create_condor_worker_token(username)

    if token:
        return jsonify({'token': token}), 201

    return jsonify({}), 400

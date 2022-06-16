"""Route for getting resources information"""
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .auth import requires_auth
from .backend import ProminenceBackend
from .errors import resources_error
from .utilities import get_remote_addr

resources = Blueprint('resources', __name__)

@resources.route("/prominence/v1/resources", methods=['GET'])
@requires_auth
def get_resources(username, group, email):
    """
    Return resources
    """
    app.logger.info('%s GetResources user:%s group:%s' % (get_remote_addr(request), username, group))

    backend = ProminenceBackend(app.config)
    data = backend.get_existing_resources(group)
    if data:
        return jsonify(data), 200

    return resources_error()

"""Route for providing health status"""
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .backend import ProminenceBackend
from .utilities import get_remote_addr

health = Blueprint('health', __name__)

@health.route("/prominence/v1/health", methods=['GET'])
def get_health():
    """
    Return health status
    """
    app.logger.info('%s GetHealth' % get_remote_addr(request))

    backend = ProminenceBackend(app.config)
    (status, msg) = backend.get_health()
    if not status:
        return jsonify(msg), 409

    return jsonify(msg), 204

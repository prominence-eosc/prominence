"""Routes for the key-value store"""
import base64
import sys
import etcd
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .errors import func_disabled, kv_error, key_not_specified, no_value_provided, value_too_big
from .utilities import get_remote_addr

from .auth import requires_auth

kv = Blueprint('kv', __name__)

@kv.route("/prominence/v1/kv", methods=['GET'])
@kv.route("/prominence/v1/kv/<path:path>", methods=['GET'])
@requires_auth
def list_keys(username, group, email, path=None):
    """
    List keys
    """
    app.logger.info('%s ListKeys user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    prefix = ''
    if path:
        prefix = '/%s' % path

    keys = []
    try:
        etcd = etcd3.client()
        for item in etcd.get_prefix('/%s%s' % (username, prefix)):
            key = item[0].decode('utf-8')
            if '_internal_' not in key:
                keys.append(key)
        etcd.close()
    except:
        return kv_error()

    data = {}
    data['keys'] = keys
    return jsonify(data), 200

@kv.route("/prominence/v1/kv/<path:key>", methods=['GET'])
@requires_auth
def get_value(username, group, email, key=None):
    """
    Get value of the specified key
    """
    app.logger.info('%s GetValue user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    value = None
    try:
        etcd = etcd3.client()
        value = etcd.get('/%s/%s' % (username, key))
        etcd.close()
    except:
        return kv_error()

    if not value:
        return ''

    return base64.b64decode(value).decode('utf-8')

@kv.route("/prominence/v1/kv/<path:key>", methods=['POST'])
@requires_auth
def set_value(username, group, email, key=None):
    """
    Set value of the specified key
    """
    app.logger.info('%s SetValue user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    if not key:
        return key_not_specified()

    if not request.get_data():
        return no_value_provided()

    if sys.getsizeof(request.get_data()) > app.config['KV_MAX_BYTES']:
        return value_too_big()

    try:
        etcd = etcd3.client()
        value = etcd.set('/%s/%s' % (username, key, base64.b64encode(request.get_data()))
        etcd.close()
    except:
        return kv_error()

    return jsonify({}), 201

@kv.route("/prominence/v1/kv/<path:key>", methods=['DELETE'])
@requires_auth
def delete_key(username, group, email, key=None):
    """
    Delete a key
    """
    app.logger.info('%s DeleteKey user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    if not key:
        return key_not_specified()

    prefix = False
    if 'prefix' in request.args:
        prefix = True

    try:
        etcd = etcd3.client()
        if not prefix:
            etcd.delete('/%s/%s' % (username, key))
        else:
            etcd.delete_prefix('/%s/%s' % (username, key))
        etcd.close()
    except:
        return kv_error()

    return jsonify({}), 200

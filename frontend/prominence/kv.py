"""Routes for the key-value store"""
import etcd
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .errors import func_disabled, kv_error, key_not_specified, no_value_provided
from .utilities import get_remote_addr, object_access_allowed

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
            keys.append(item[0].decode('utf-8'))
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

    return value.decode('utf-8')

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

    data = list((request.form).keys())
    if data:
        data = data[0]
    else:
        return no_value_provided()

    try:
        etcd = etcd3.client()
        value = etcd.set('/%s/%s' % (username, key), data)
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

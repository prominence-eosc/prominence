"""Routes for the key-value store"""
import base64
import sys
import etcd3
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .errors import func_disabled, kv_error, key_not_specified, no_value_provided, value_too_big, ttl_not_specified, lease_not_found
from .utilities import get_remote_addr

from .auth import requires_auth

kv = Blueprint('kv', __name__)

@kv.route("/prominence/v1/kv", methods=['GET'])
@kv.route("/prominence/v1/kv/<path:path>", methods=['GET'])
@requires_auth
def get_keys(username, group, email, path=None):
    """
    List keys
    """
    app.logger.info('%s ListKeys user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    if 'list' in request.args:
        prefix = ''
        if path:
            prefix = '/%s' % path

        keys = []
        try:
            etcd = etcd3.client()
            for item in etcd.get_prefix('/%s%s' % (username, prefix)):
                key = item[1].key.decode('utf-8').replace('/%s' % username, '', 1)
                if '_internal_' not in key:
                    keys.append(key)
            etcd.close()
        except Exception as err:
            app.logger.error('Got exception listing kv: %s', err)
            return kv_error()

        data = {}
        data['keys'] = keys
        return jsonify(data), 200

    if not path:
        return key_not_specified()

    value = None
    try:
        etcd = etcd3.client()
        value = etcd.get('/%s/%s' % (username, path))
        etcd.close()
    except Exception as err:
        app.logger.error('Got exception getting kv: %s', err)
        return kv_error()

    if not value:
        return ''
    if not value[0]:
        return ''

    return base64.b64decode(value[0]).decode('utf-8')

@kv.route("/prominence/v1/kv/lease", methods=['POST'])
@requires_auth
def create_lease(username, group, email):
    """
    Create a lease with the specified TTL
    """
    app.logger.info('%s CreateLease user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_KV'] != 'True':
        return func_disabled()

    ttl = None
    lease_id = None
    try:
        if 'ttl' in request.get_json():
            ttl = int(request.get_json()['ttl'])
        if 'id' in request.get_json():
            lease_id = request.get_json()['id']
    except:
        pass

    if not ttl:
        return ttl_not_specified()

    lease = None
    try:
        etcd = etcd3.client()
        if lease_id:
            lease = etcd.lease(ttl, lease_id=lease_id)
        else:
            lease = etcd.lease(ttl)
        etcd.close()
    except Exception as err:
        app.logger.error('Got exception creating leave: %s', err)
        return kv_error()

    if lease:
        return jsonify({'id': lease.id}), 200

    return kv_error()

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

    value = None
    lease_id = None
    try:
        if 'value' in request.get_json():
            value = str(request.get_json()['value']).encode()
        if 'lease_id' in request.get_json():
            lease_id = request.get_json()['lease_id']
    except:
        pass

    if not value:
        value = request.get_data()

    try:
        etcd = etcd3.client()

        if not lease_id:
            etcd.put('/%s/%s' % (username, key), base64.b64encode(value))
        else:
            etcd.put('/%s/%s' % (username, key), base64.b64encode(value), lease=lease_id)
        etcd.close()
    except Exception as err:
        app.logger.error('Got exception setting kv: %s', err)
        if 'requested lease not found' in err:
            return lease_not_found()

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
    except Exception as err:
        app.logger.error('Got exception deleting kv: %s', err)
        return kv_error()

    return jsonify({}), 200

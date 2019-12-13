"""Routes for managing data"""
from flask import Blueprint, jsonify, request
from flask import current_app as app

from backend import ProminenceBackend
from utilities import get_remote_addr, object_access_allowed

from auth import requires_auth

data = Blueprint('data', __name__)

@data.route("/prominence/v1/data", methods=['GET'])
@data.route("/prominence/v1/data/<path:path>", methods=['GET'])
@requires_auth
def list_objects(username, group, email, path=None):
    """
    List objects in cloud storage
    """
    app.logger.info('%s ListData user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_DATA'] != 'True':
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)

    if not path:
        objects = backend.list_objects(username, group)
        return jsonify(objects)
    else:
        path = str(path)

    if not object_access_allowed(group, path):
        return jsonify({'error':'Not authorized to access this path'}), 403

    objects = backend.list_objects(username, group, path)
    return jsonify(objects)

@data.route("/prominence/v1/data/<path:obj>", methods=['DELETE'])
@requires_auth
def delete_object(username, group, email, obj):
    """
    Delete object in cloud storage
    """
    app.logger.info('%s DeleteData user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_DATA'] != 'True':
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)

    obj = str(obj)
    if '/' in obj:
        if not object_access_allowed(group, obj):
            return jsonify({'error':'Not authorized to access this object'}), 403

    success = backend.delete_object(username, group, obj)
    return jsonify({}), 204

@data.route("/prominence/v1/data", methods=['POST'])
@data.route("/prominence/v1/data/upload", methods=['POST'])
@requires_auth
def upload_file(username, group, email):
    """
    Return Swift URL to allow users to upload data to Swift
    """
    app.logger.info('%s UploadData user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_DATA'] != 'True':
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)

    if 'name' in request.args:
        object_name = request.args.get('name')
    elif 'filename' in request.get_json():
        object_name = request.get_json()['filename']
    else:
        return jsonify({'error':'An object name must be specified'}), 400

    if '/' in object_name:
        pieces = object_name.split('/')
        object_name_only = pieces[len(pieces) - 1]
        pieces.remove(object_name_only)
        file_group = '/'.join(pieces)
        if file_group not in group:
            return jsonify({'error':'Not authorized to access upload with this path'}), 403
        url = backend.create_presigned_url('put', app.config['S3_BUCKET'], 'uploads/%s' % object_name)
    else:
        url = backend.create_presigned_url('put', app.config['S3_BUCKET'], 'uploads/%s/%s' % (username, object_name))
    return jsonify({'url':url}), 201

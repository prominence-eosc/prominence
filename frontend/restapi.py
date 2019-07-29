#!/usr/bin/python
from __future__ import print_function
from functools import wraps
import logging
import os
import sys
import uuid
import requests
from flask import Flask, jsonify, request

from backend import ProminenceBackend

import validate

app = Flask(__name__)

# Configuration
if 'PROMINENCE_RESTAPI_CONFIG_FILE' in os.environ:
    app.config.from_pyfile(os.environ['PROMINENCE_RESTAPI_CONFIG_FILE'])
else:
    print('ERROR: Environment variable PROMINENCE_RESTAPI_CONFIG_FILE has not been defined')
    exit(1)

# Create backend
backend = ProminenceBackend(app.config)

# Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def get_user_details(token):
    """
    Get the username and group from a token
    """
    headers = {'Authorization':'Bearer %s' % token}
    try:
        response = requests.get(app.config['OIDC_URL']+'/userinfo', timeout=app.config['OIDC_TIMEOUT'], headers=headers)
    except requests.exceptions.RequestException:
        app.logger.warning('%s AuthenticationFailure no response from identity provider' % get_remote_addr(request))
        return None

    username = None
    if 'preferred_username' in response.json():
        username = str(response.json()['preferred_username'])
    elif 'sub' in response.json():
        username = str(response.json()['sub'])

    groups = None
    if 'groups' in response.json():
        if len(response.json()['groups']) > 0:
            groups = ','.join(str(group) for group in response.json()['groups'])

    return (username, groups)

def get_remote_addr(req):
    """
    Returns the remote IP address of a user
    """
    return req.environ.get('HTTP_X_REAL_IP', req.remote_addr)

def object_access_allowed(groups, path):
    """
    Decide if a user is allowed to access a path
    """
    for group in groups.split(','):
        if path.startswith(group):
            return True
    return False

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
        if 'Authorization' not in request.headers:
            app.logger.warning('%s AuthenticationFailure authorization not in headers' % get_remote_addr(request))
            return authenticate()
        auth = request.headers['Authorization']
        try:
            token = auth.split(' ')[1]
        except:
            app.logger.warning('%s AuthenticationFailure no token specified' % get_remote_addr(request))
            return authenticate()
        (username, group) = get_user_details(token)
        if not username:
            app.logger.warning('%s AuthenticationFailure username not returned from identity provider' % get_remote_addr(request))
            return authenticate()
        app.logger.info('%s AuthenticationSuccess user:%s group:%s' % (get_remote_addr(request), username, group))
        return function(username, group, *args, **kwargs)
    return decorated

@app.route("/prominence/v1/data", methods=['GET'])
@app.route("/prominence/v1/data/<path:path>", methods=['GET'])
@requires_auth
def list_objects(username, group, path=None):
    """
    List objects in cloud storage
    """
    if path is None:
        objects = backend.list_objects(username, group)
        return jsonify(objects)
    else:
        path = str(path)

    if not object_access_allowed(group, path):
        return jsonify({'error':'Not authorized to access this path'}), 403

    objects = backend.list_objects(username, group, path)
    return jsonify(objects)

@app.route("/prominence/v1/data/<path:obj>", methods=['DELETE'])
@requires_auth
def delete_object(username, group, obj):
    """
    Delete object in cloud storage
    """
    obj = str(obj)
    if '/' in obj:
        if not object_access_allowed(group, obj):
            return jsonify({'error':'Not authorized to access this object'}), 403

    success = backend.delete_object(username, group, obj)
    return jsonify({}), 204

@app.route("/prominence/v1/data/upload", methods=['POST'])
@requires_auth
def upload_file(username, group):
    """
    Return Swift URL to allow users to upload data to Swift
    """
    app.logger.info('%s UploadData user:%s group:%s' % (get_remote_addr(request), username, group))

    if 'filename' in request.get_json():
        filename = request.get_json()['filename']
        if '/' in filename:
            pieces = filename.split('/')
            filename_only = pieces[len(pieces) - 1]
            pieces.remove(filename_only)
            file_group = '/'.join(pieces)
            if file_group not in group:
                return jsonify({'error':'Not authorized to access upload with this path'}), 403
            url = backend.create_presigned_url('put', app.config['S3_BUCKET'], 'uploads/%s' % request.get_json()['filename'])
        else:
            url = backend.create_presigned_url('put', app.config['S3_BUCKET'], 'uploads/%s/%s' % (username, request.get_json()['filename']))
        return jsonify({'url':url}), 201
    return jsonify({'error':'invalid JSON content supplied'}), 400

@app.route("/prominence/v1/workflows", methods=['GET'])
@requires_auth
def workflows(username, group):
    """
    List workflows
    """
    app.logger.info('%s ListWorkflows user:%s group:%s' % (get_remote_addr(request), username, group))

    active = True
    completed = False
    num = 1
    if 'completed' in request.args:
        if request.args.get('completed') == 'true':
            completed = True
            active = False
        if 'num' in request.args:
            num = request.args.get('num')
    constraint = (None, None)
    if 'constraint' in request.args:
        cons = request.args.get('constraint')
        key = cons.split('=')[0]
        value = cons.split('=')[1]
        constraint = (key, value)

    if 'all' in request.args:
        completed = True
        active = True
        num = -1

    detail = 0
    if 'detail' in request.args:
        detail = 1

    workflow_ids = []
    if 'id' in request.args:
        workflow_ids = request.args.get('id').split(',')
        # Assume both active workflows and completed workflows
        completed = True
        active = True

    data = backend.list_workflows(workflow_ids, username, active, completed, num, detail, constraint)

    return jsonify(data)

@app.route("/prominence/v1/workflows/<int:workflow_id>", methods=['GET'])
@requires_auth
def get_workflow(username, group, workflow_id):
    """
    Describe a workflow
    """
    app.logger.info('%s DescribeWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    data = backend.list_workflows([workflow_id], username, True, True, 1, 1, (None, None))
    return jsonify(data)

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stdout", methods=['GET'])
@requires_auth
def get_stdout_wf(username, group, workflow_id, job):
    """
    Return the standard output from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity) = backend.get_job_unique_id(workflow_id)
    if job is None:
        job = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stdout = backend.get_stdout(uid, job_id, job)
    if stdout is None:
        return jsonify({'error':'stdout does not exist'}), 400
    else:
        return stdout

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stderr", methods=['GET'])
@requires_auth
def get_stderr_wf(username, group, workflow_id, job):
    """
    Return the standard error from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity) = backend.get_job_unique_id(workflow_id)
    if job is None:
        job = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stderr = backend.get_stderr(uid, job_id, job)
    if stderr is None:
        return jsonify({'error':'stderr does not exist'}), 400
    else:
        return stderr

@app.route("/prominence/v1/workflows/<int:workflow_id>", methods=['DELETE'])
@requires_auth
def delete_workflow(username, group, workflow_id):
    """
    Delete a workflow
    """
    app.logger.info('%s DeleteWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (return_code, data) = backend.delete_workflows(username, [workflow_id])

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/workflows", methods=['DELETE'])
@requires_auth
def delete_workflows(username, group):
    """
    Delete the specified workflow(s)
    """
    if 'id' not in request.args:
        return jsonify({'error':'a workflow id or list of workflow ids must be provided'}), 400

    app.logger.info('%s DeleteWorkflows user:%s group:%s id:%s' % (get_remote_addr(request), username, group, request.args.get('id')))

    (return_code, data) = backend.delete_workflows(username, request.args.get('id').split(','))

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/workflows", methods=['POST'])
@requires_auth
def submit_job_new(username, group):
    """
    Create a new workflow
    """
    # Create job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s WorkflowSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Create workflow
    (return_code, data) = backend.create_workflow(username, group, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@app.route("/prominence/v1/jobs", methods=['POST'])
@requires_auth
def submit_job(username, group):
    """
    Create a new job
    """
    # Job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s JobSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Validate the input JSON
    (status, msg) = validate.validate_job(request.get_json())
    if not status:
        return jsonify({'error': msg}), 400

    # Create job
    (return_code, data) = backend.create_job(username, group, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@app.route("/prominence/v1/jobs", methods=['GET'])
@requires_auth
def jobs(username, group):
    """
    List jobs
    """
    app.logger.info('%s ListJobs user:%s group:%s' % (get_remote_addr(request), username, group))

    job_id = -1
    active = True
    completed = False
    num = 1
    if 'completed' in request.args:
        if request.args.get('completed') == 'true':
            completed = True
            active = False
        if 'num' in request.args:
            num = request.args.get('num')
    constraint = (None, None)
    if 'constraint' in request.args:
        cons = request.args.get('constraint')
        key = cons.split('=')[0]
        value = cons.split('=')[1]
        constraint = (key, value)

    if 'all' in request.args:
        completed = True
        active = True
        num = -1

    detail = 0
    if 'detail' in request.args:
        detail = 1

    job_ids = []
    if 'id' in request.args:
        job_ids = request.args.get('id').split(',')
        # Assume both active jobs and completed jobs
        completed = True
        active = True

    data = backend.list_jobs(job_ids, username, active, completed, num, detail, constraint)

    return jsonify(data)

@app.route("/prominence/v1/jobs/<int:job_id>", methods=['GET'])
@requires_auth
def get_job(username, group, job_id):
    """
    Describe the specified job
    """
    app.logger.info('%s DescribeJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    data = backend.list_jobs([job_id], username, True, True, 1, 1, (None, None))
    return jsonify(data)

@app.route("/prominence/v1/jobs", methods=['DELETE'])
@requires_auth
def delete_jobs(username, group):
    """
    Delete the specified job(s)
    """
    if 'id' not in request.args:
        return jsonify({'error':'a job id or list of job ids must be provided'}), 400

    app.logger.info('%s DeleteJobs user:%s group:%s id:%s' % (get_remote_addr(request), username, group, request.args.get('id')))

    (return_code, data) = backend.delete_job(username, request.args.get('id').split(','))

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/jobs/<int:job_id>", methods=['DELETE'])
@requires_auth
def delete_job(username, group, job_id):
    """
    Delete the specified job
    """
    app.logger.info('%s DeleteJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (return_code, data) = backend.delete_job(username, [job_id])

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/jobs/<int:job_id>/stdout", methods=['GET'])
@requires_auth
def get_stdout(username, group, job_id):
    """
    Return the standard output from the specified job
    """
    app.logger.info('%s GetStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    
    stdout = backend.get_stdout(uid, job_id)
    if stdout is None:
        return jsonify({'error':'stdout does not exist'}), 400
    else:
        return stdout
    
@app.route("/prominence/v1/jobs/<int:job_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr(username, group, job_id):
    """
    Return the standard error from the specified job
    """
    app.logger.info('%s GetStdErr user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stderr = backend.get_stderr(uid, job_id)
    if stderr is None:
        return jsonify({'error':'stderr does not exist'}), 400
    else:
        return stderr

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)

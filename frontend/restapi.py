#!/usr/bin/python
from functools import wraps
import logging
import uuid
import os
import requests
from flask import Flask, jsonify, request

import backend
from CreateSwiftURL import create_swift_url

app = Flask(__name__)

# Configuration
app.config.from_pyfile('/etc/prominence/prominence-rest.cfg')

# Logging
logging.basicConfig(filename=app.config['LOG_FILE'], level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

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

    group = None
    if 'groups' in response.json():
        if len(response.json()['groups']) > 0:
            group = str(response.json()['groups'][0])

    return (username, group)

def get_remote_addr(req):
    """
    Returns the remote IP address of a user
    """
    return req.environ.get('HTTP_X_REAL_IP', req.remote_addr)

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

@app.route("/prominence/v1/data/upload", methods=['POST'])
@requires_auth
def upload_file(username, group):
    """
    Return Swift URL to allow users to upload data to Swift
    """
    url = create_swift_url('PUT', '/v1/prominence-jobs/%s/%s' % (username, request.get_json()['filename']))
    return jsonify({'url':url}), 201

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

    data = backend.list_workflows(-1, username, active, completed, num, detail, constraint)

    return jsonify(data)

@app.route("/prominence/v1/workflows/<int:workflow_id>", methods=['GET'])
@requires_auth
def get_workflow(username, group, workflow_id):
    """
    Describe a workflow
    """
    app.logger.info('%s DescribeWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    completed = False
    active = True
    num = 1
    if 'completed' in request.args:
        if request.args.get('completed') == 'true':
            completed = True
            active = False
        if 'num' in request.args and isinstance(request.args.get('num'), int):
            num = request.args.get('num')

    data = backend.list_workflows(workflow_id, username, active, completed, num, 1, (None, None))

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
    filename = app.config['SANDBOX_PATH'] + '/%s/%s/job.%s.out' % (uid, job, job)
    if os.path.isfile(filename):
        with open(filename) as fd:
            return fd.read()
    else:
        return jsonify({'error':'stdout does not exist'}), 404

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
    filename = app.config['SANDBOX_PATH'] + '/%s/%s/job.%s.err' % (uid, job, job)
    if os.path.isfile(filename):
        with open(filename) as fd:
            return fd.read()
    else:
        return jsonify({'error':'stdout does not exist'}), 404

@app.route("/prominence/v1/workflows/<int:workflow_id>", methods=['DELETE'])
@requires_auth
def delete_workflow(username, group, workflow_id):
    """
    Delete a workflow
    """
    app.logger.info('%s DeleteWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (return_code, data) = backend.delete_workflow(username, workflow_id)

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/workflows", methods=['POST'])
@requires_auth
def submit_job_new(username, group):
    """
    Create a new workflow
    """
    # Job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s WorkflowSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Create sandbox
    job_sandbox = app.config['SANDBOX_PATH'] + '/' + uid
    try:
        os.makedirs(job_sandbox)
    except:
        return jsonify({'error':'Unable to create job sandbox'}), 400

    # Create workflow
    (return_code, data) = backend.create_workflow(username, group, uid, job_sandbox, request.get_json())

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

    # Create sandbox
    job_sandbox = app.config['SANDBOX_PATH'] + '/' + uid
    try:
        os.makedirs(job_sandbox)
        os.makedirs(job_sandbox + '/input')
    except:
        return jsonify({"error":"Unable to create job sandbox"}), 400

    # Create job
    (return_code, data) = backend.create_job(username, group, uid, job_sandbox, request.get_json())

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

    data = backend.list_jobs(job_id, username, active, completed, num, detail, constraint)

    return jsonify(data)

@app.route("/prominence/v1/jobs/<int:job_id>", methods=['GET'])
@requires_auth
def get_job(username, group, job_id):
    """
    Describe the specified job
    """
    app.logger.info('%s DescribeJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    completed = False
    active = True
    num = 1
    if 'completed' in request.args:
        if request.args.get('completed') == 'true':
            completed = True
            active = False
        if 'num' in request.args and isinstance(request.args.get('num'), int):
            num = request.args.get('num')

    data = backend.list_jobs(job_id, username, active, completed, num, 1, (None, None))

    return jsonify(data)

@app.route("/prominence/v1/jobs/<int:job_id>", methods=['DELETE'])
@requires_auth
def delete_job(username, group, job_id):
    """
    Delete the specified job
    """
    app.logger.info('%s DeleteJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (return_code, data) = backend.delete_job(username, job_id)

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@app.route("/prominence/v1/jobs/<int:job_id>/<int:task>/stdout", methods=['GET'])
@requires_auth
def get_stdout(username, group, job_id, task):
    """
    Return the standard output from the specified job
    """
    app.logger.info('%s GetStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity) = backend.get_job_unique_id(job_id)
    if task is None:
        task = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    filename = app.config['SANDBOX_PATH'] + '/%s/job.%d.%d.out' % (uid, job_id, task)
    if os.path.isfile(filename):
        with open(filename) as fd:
            return fd.read()
    else:
        return jsonify({'error':'stdout does not exist'}), 404

@app.route("/prominence/v1/jobs/<int:job_id>/<int:task>/stderr", methods=['GET'])
@requires_auth
def get_stderr(username, group, job_id, task):
    """
    Return the standard error from the specified job
    """
    app.logger.info('%s GetStdErr user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity) = backend.get_job_unique_id(job_id)
    if task is None:
        task = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    filename = app.config['SANDBOX_PATH'] + '/%s/job.%d.%d.err' % (uid, job_id, task)
    if os.path.isfile(filename):
        with open(filename) as fd:
            return fd.read()
    else:
        return jsonify({'error':'stderr does not exist'}), 404

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)

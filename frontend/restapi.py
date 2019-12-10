#!/usr/bin/python
from __future__ import print_function
from functools import wraps
import logging
import os
import sys
import uuid
import time
import requests
import jwt
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
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
        if len(response.json()['groups']) > 0:
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

@app.route("/prominence/v1/data", methods=['GET'])
@app.route("/prominence/v1/data/<path:path>", methods=['GET'])
@requires_auth
def list_objects(username, group, email, path=None):
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
def delete_object(username, group, email, obj):
    """
    Delete object in cloud storage
    """
    obj = str(obj)
    if '/' in obj:
        if not object_access_allowed(group, obj):
            return jsonify({'error':'Not authorized to access this object'}), 403

    success = backend.delete_object(username, group, obj)
    return jsonify({}), 204

@app.route("/prominence/v1/data", methods=['POST'])
@app.route("/prominence/v1/data/upload", methods=['POST'])
@requires_auth
def upload_file(username, group, email):
    """
    Return Swift URL to allow users to upload data to Swift
    """
    app.logger.info('%s UploadData user:%s group:%s' % (get_remote_addr(request), username, group))

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

@app.route("/prominence/v1/workflows", methods=['GET'])
@requires_auth
def workflows(username, group, email):
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
        if '=' in request.args.get('constraint'):
            if len(request.args.get('constraint').split('=')) == 2:
                constraint = (request.args.get('constraint').split('=')[0],
                              request.args.get('constraint').split('=')[1])
            else:
                return jsonify({'error':'Invalid constraint'}), 400
        else:
            return jsonify({'error':'Invalid constraint'}), 400

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
def get_workflow(username, group, email, workflow_id):
    """
    Describe a workflow
    """
    app.logger.info('%s DescribeWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    data = backend.list_workflows([workflow_id], username, True, True, 1, 1, (None, None))
    return jsonify(data)

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stdout", methods=['GET'])
@requires_auth
def get_stdout_wf(username, group, email, workflow_id, job):
    """
    Return the standard output from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if job is None:
        job = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stdout = backend.get_stdout(uid, iwd, None, None, -1, job, -1)
    if stdout is None:
        return jsonify({'error':'stdout does not exist'}), 400
    else:
        return stdout

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stderr", methods=['GET'])
@requires_auth
def get_stderr_wf(username, group, email, workflow_id, job):
    """
    Return the standard error from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if job is None:
        job = 0
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stderr = backend.get_stderr(uid, iwd, None, None, -1, job, -1)
    if stderr is None:
        return jsonify({'error':'stderr does not exist'}), 400
    else:
        return stderr

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/<int:instance_id>/stdout", methods=['GET'])
@requires_auth
def get_stdout_wf_jf(username, group, email, workflow_id, job, instance_id):
    """
    Return the standard output from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stdout = backend.get_stdout(uid, iwd, None, None, workflow_id, None, instance_id)
    if stdout is None:
        return jsonify({'error':'stdout does not exist'}), 400
    else:
        return stdout

@app.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/<int:instance_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr_wf_jf(username, group, email, workflow_id, job, instance_id):
    """
    Return the standard error from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stderr = backend.get_stderr(uid, iwd, None, None, workflow_id, None, instance_id)
    if stderr is None:
        return jsonify({'error':'stderr does not exist'}), 400
    else:
        return stderr

@app.route("/prominence/v1/workflows/<int:workflow_id>", methods=['DELETE'])
@requires_auth
def delete_workflow(username, group, email, workflow_id):
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
def delete_workflows(username, group, email):
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
def submit_job_new(username, group, email):
    """
    Create a new workflow
    """
    # Create job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s WorkflowSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Validate the input JSON
    (status, msg) = validate.validate_workflow(request.get_json())
    if not status:
        return jsonify({'error': msg}), 400

    # Create workflow
    (return_code, data) = backend.create_workflow(username, group, email, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@app.route("/prominence/v1/jobs", methods=['POST'])
@requires_auth
def submit_job(username, group, email):
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
    (return_code, data) = backend.create_job(username, group, email, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@app.route("/prominence/v1/jobs", methods=['GET'])
@requires_auth
def jobs(username, group, email):
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
        if '=' in request.args.get('constraint'):
            if len(request.args.get('constraint').split('=')) == 2:
                constraint = (request.args.get('constraint').split('=')[0],
                              request.args.get('constraint').split('=')[1])
            else:
                return jsonify({'error':'Invalid constraint'}), 400
        else:
            return jsonify({'error':'Invalid constraint'}), 400

    if 'all' in request.args:
        completed = True
        active = True
        num = -1

    detail = 0
    if 'detail' in request.args:
        detail = 1

    workflow = False
    if 'workflow' in request.args:
        if request.args.get('workflow') == 'true':
            workflow = True
            num = -1

    job_ids = []
    if 'id' in request.args:
        job_ids = request.args.get('id').split(',')
        # Assume both active jobs and completed jobs
        if not workflow:
            completed = True
            active = True

    data = backend.list_jobs(job_ids, username, active, completed, workflow, num, detail, constraint)

    return jsonify(data)

@app.route("/prominence/v1/jobs/<int:job_id>", methods=['GET'])
@requires_auth
def get_job(username, group, email, job_id):
    """
    Describe the specified job
    """
    app.logger.info('%s DescribeJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    data = backend.list_jobs([job_id], username, True, True, False, 1, 1, (None, None))
    return jsonify(data)

@app.route("/prominence/v1/jobs/<int:job_id>/exec", methods=['POST'])
@requires_auth
def exec_in_job(username, group, email, job_id):
    """
    Execute a command in a job
    """
    app.logger.info('%s ExecJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_EXEC'] != 'True':
        return jsonify({'error':'Functionality disabled'}), 401

    (uid, identity, iwd, _, _, name, status) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    if status != 2:
        return jsonify({'error':'Job is not running'}), 400

    command = []
    if 'command' in request.args:
        command = str(request.args.get('command')).split(',')

    output = backend.execute_command(job_id, iwd, command)
    if output is not None:
        return output, 200
 
    return jsonify({'error':'Unable to execute command'}), 400

@app.route("/prominence/v1/jobs", methods=['DELETE'])
@requires_auth
def delete_jobs(username, group, email):
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
def delete_job(username, group, email, job_id):
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
def get_stdout(username, group, email, job_id):
    """
    Return the standard output from the specified job
    """
    app.logger.info('%s GetStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    
    stdout = backend.get_stdout(uid, iwd, out, err, job_id, name)
    if stdout is None:
        return jsonify({'error':'stdout does not exist'}), 400
    else:
        return stdout
    
@app.route("/prominence/v1/jobs/<int:job_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr(username, group, email, job_id):
    """
    Return the standard error from the specified job
    """
    app.logger.info('%s GetStdErr user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403

    stderr = backend.get_stderr(uid, iwd, out, err, job_id, name)
    if stderr is None:
        return jsonify({'error':'stderr does not exist'}), 400
    else:
        return stderr

@app.route("/prominence/v1/jobs/<int:job_id>/snapshot", methods=['GET'])
@requires_auth
def get_snapshot(username, group, email, job_id):
    """
    Download the current snapshot
    """
    app.logger.info('%s GetSnapshot user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_SNAPSHOTS'] != 'True':
        return jsonify({'error':'Functionality disabled'}), 401

    (uid, identity, _, _, _, name, status) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    if status != 2:
        return jsonify({'error':'Job is not running'}), 400

    url = backend.get_snapshot_url(uid)
    return jsonify({'url': url}), 200

@app.route("/prominence/v1/jobs/<int:job_id>/snapshot", methods=['PUT'])
@requires_auth
def create_snapshot(username, group, email, job_id):
    """
    Download the current snapshot
    """
    app.logger.info('%s CreateSnapshot user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_SNAPSHOTS'] != 'True':
        return jsonify({'error':'Functionality disabled'}), 401

    (uid, identity, iwd, _, _, _, status) = backend.get_job_unique_id(job_id)
    if identity is None:
        return jsonify({'error':'Job does not exist'}), 400
    if username != identity:
        return jsonify({'error':'Not authorized to access this job'}), 403
    if status != 2:
        return jsonify({'error':'Job is not running'}), 400

    if 'path' in request.args:
        path = request.args.get('path')
    else:
        return jsonify({'error':'A path to snapshot must be specified'}), 400

    path = backend.validate_snapshot_path(iwd, path)
    if not path:
         return jsonify({'error':'Invalid path'}), 400

    backend.create_snapshot(uid, job_id, path)
    return jsonify({}), 200

@app.route("/prominence/v1/accounting", methods=['GET'])
@requires_auth
def get_accounting(username, group, email):
    """
    Return usage data
    """
    app.logger.info('%s GetAccounting user:%s group:%s' % (get_remote_addr(request), username, group))
    
    username_use = username
    group_use = None
    if 'by_group' in request.args:
        if request.args.get('by_group') == 'true':
            group_use = group
            username_use = None

    if 'start' in request.args:
        start_date = request.args.get('start')
    else:
        return jsonify({'error':'Start date must be provided'}), 400

    if 'end' in request.args:
        end_date = request.args.get('end')
    else:
        return jsonify({'error':'End date must be provided'}), 400

    client = Elasticsearch([{'host':app.config['ELASTICSEARCH_HOST'],
                             'port':app.config['ELASTICSEARCH_PORT']}])
    
    if username_use and group_use:
        query = Q('match', group__keyword=group_use) & Q('match', username=username_use)
    elif username_use and not group_use:
        query = Q('match', username=username_use)
    elif not username_use and group_use:
        query = Q('match', group__keyword=group_use)

    search = Search(using=client, index=app.config['ELASTICSEARCH_INDEX']) \
             .filter('range', date={'gte':start_date, 'lte':end_date}) \
             .query(query) \
             .scan()

    wall_time = 0
    cpu_time = 0
    num_jobs = 0

    for hit in search:
        if hit.type == 'job':
            cpus = hit.resources['cpus']
            if 'tasks' in hit.execution:
                for task in hit.execution['tasks']:
                    if 'wallTimeUsage' in task:
                        wall_time += task['wallTimeUsage']*cpus
                        num_jobs += 1
                    if 'cpuTimeUsage' in task:
                        cpu_time += task['cpuTimeUsage']

    data = {}
    data['usage'] = {}
    data['usage']['groups'] = {}
    data['usage']['users'] = {}
    
    if group_use and not username_use:
        data['usage']['groups'][group_use] = {}
        data['usage']['groups'][group_use]['cpuTime'] = cpu_time/3600.0
        data['usage']['groups'][group_use]['wallTime'] = wall_time/3600.0
        data['usage']['groups'][group_use]['numberOfJobs'] = num_jobs
    elif not group_use and username_use:
        data['usage']['groups'][username_use] = {}
        data['usage']['groups'][username_use]['cpuTime'] = cpu_time/3600.0
        data['usage']['groups'][username_use]['wallTime'] = wall_time/3600.0
        data['usage']['groups'][username_use]['numberOfJobs'] = num_jobs

    return jsonify(data), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)


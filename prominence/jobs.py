"""Routes for managing jobs"""
import json
import time
import uuid

from flask import Blueprint, jsonify, request
from flask import current_app as app

from .auth import requires_auth
from .backend import ProminenceBackend
from .errors import invalid_constraint, func_disabled, no_such_job, not_auth_job, job_not_running, command_failed, job_clone_error
from .errors import job_id_required, no_stdout, no_stderr, snapshot_path_required, snapshot_invalid_path, job_removal_failed, invalid_status
from .validate import validate_job
from .utilities import get_remote_addr

jobs = Blueprint('jobs', __name__)

@jobs.route("/prominence/v1/jobs", methods=['POST'])
@requires_auth
def submit_job(username, group, email):
    """
    Create a new job
    """
    # Job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s JobSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Validate the input JSON
    (status, msg) = validate_job(request.get_json())
    if not status:
        return jsonify({'error': msg}), 400

    # Create job
    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.create_job(username, group, email, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@jobs.route("/prominence/v1/jobs", methods=['GET'])
@requires_auth
def list_jobs(username, group, email):
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
                return invalid_constraint()
        else:
            return invalid_constraint()

    name_constraint = None
    if 'name' in request.args:
        name_constraint = request.args.get('name')
       
    if 'all' in request.args:
        completed = True
        active = True
        num = -1

    status = None
    if 'status' in request.args:
        status = request.args.get('status')
        if status != 'idle' and status != 'running':
            return invalid_status()

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

    backend = ProminenceBackend(app.config)
    data = backend.list_jobs(job_ids, username, active, completed, status, workflow, num, detail, constraint, name_constraint)

    return jsonify(data)

@jobs.route("/prominence/v1/jobs/<int:job_id>", methods=['GET'])
@requires_auth
def get_job(username, group, email, job_id):
    """
    Describe the specified job
    """
    app.logger.info('%s DescribeJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    backend = ProminenceBackend(app.config)
    data = backend.list_jobs([job_id], username, True, True, False, None, 1, 1, (None, None), None)
    return jsonify(data)

@jobs.route("/prominence/v1/jobs/<int:job_id>/exec", methods=['POST'])
@requires_auth
def exec_in_job(username, group, email, job_id):
    """
    Execute a command in a job
    """
    app.logger.info('%s ExecJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_EXEC'] != 'True':
        return func_disabled()

    backend = ProminenceBackend(app.config)

    (_, identity, iwd, _, _, name, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()
    if status != 2:
        return job_not_running()

    command = []
    if 'command' in request.args:
        command_user = str(request.args.get('command'))
        command = 'cd userhome && %s' % str(request.args.get('command'))
        command = command.split(',')

    output = backend.execute_command(job_id, iwd, command)
    if output is not None:
        try:
            with open('%s/commands.log' % iwd, 'a') as fh:
                fh.write('%d %s\n' % (int(time.time()), command_user))
        except:
            pass
        return output, 200
 
    return command_failed()

@jobs.route("/prominence/v1/jobs", methods=['DELETE'])
@requires_auth
def delete_jobs(username, group, email):
    """
    Delete the specified job(s)
    """
    if 'id' not in request.args:
        return job_id_required()

    app.logger.info('%s DeleteJobs user:%s group:%s id:%s' % (get_remote_addr(request), username, group, request.args.get('id')))

    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.delete_job(username, request.args.get('id').split(','))

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@jobs.route("/prominence/v1/jobs/<int:job_id>", methods=['DELETE'])
@requires_auth
def delete_job(username, group, email, job_id):
    """
    Delete the specified job
    """
    app.logger.info('%s DeleteJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.delete_job(username, [job_id])

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@jobs.route("/prominence/v1/jobs/<int:job_id>/stdout", methods=['GET'])
@requires_auth
def get_stdout(username, group, email, job_id):
    """
    Return the standard output from the specified job
    """
    app.logger.info('%s GetStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    node = 0
    if 'node' in request.args:
        node = int(request.args.get('node'))

    offset = 0
    if 'offset' in request.args:
        offset = int(request.args.get('offset'))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()

    stdout = backend.get_stdout(uid, iwd, out, err, job_id, name, node=node, offset=offset)
    if stdout is None:
        return no_stdout()
    else:
        return stdout

@jobs.route("/prominence/v1/jobs/<int:job_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr(username, group, email, job_id):
    """
    Return the standard error from the specified job
    """
    app.logger.info('%s GetStdErr user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    node = 0
    if 'node' in request.args:
        node = int(request.args.get('node'))

    offset = 0
    if 'offset' in request.args:
        offset = int(request.args.get('offset'))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()

    stderr = backend.get_stderr(uid, iwd, out, err, job_id, name, node=node, offset=offset)
    if stderr is None:
        return no_stderr()
    else:
        return stderr

@jobs.route("/prominence/v1/jobs/<int:job_id>/snapshot", methods=['GET'])
@requires_auth
def get_snapshot(username, group, email, job_id):
    """
    Download the current snapshot
    """
    app.logger.info('%s GetSnapshot user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_SNAPSHOTS'] != 'True':
        return func_disabled()

    backend = ProminenceBackend(app.config)
    (uid, identity, _, _, _, name, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()
    if status != 2:
        return job_not_running()

    url = backend.get_snapshot_url(uid)
    return jsonify({'url': url}), 200

@jobs.route("/prominence/v1/jobs/<int:job_id>/snapshot", methods=['PUT'])
@requires_auth
def create_snapshot(username, group, email, job_id):
    """
    Download the current snapshot
    """
    app.logger.info('%s CreateSnapshot user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_SNAPSHOTS'] != 'True':
        return func_disabled()

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()
    if status != 2:
        return job_not_running()

    if 'path' in request.args:
        path = request.args.get('path')
    else:
        return snapshot_path_required()

    (path, userhome) = backend.validate_snapshot_path(iwd, path)
    if not path:
        return snapshot_invalid_path()

    backend.create_snapshot(uid, job_id, path, userhome)
    return jsonify({}), 200

@jobs.route("/prominence/v1/jobs/<int:job_id>/remove", methods=['PUT'])
@requires_auth
def remove_job(username, group, email, job_id):
    """
    Remove a completed job from the queue
    """
    app.logger.info('%s RemoveFromQueue user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()

    if not backend.remove_job(job_id):
        return job_removal_failed()

    return jsonify({}), 200

@jobs.route("/prominence/v1/jobs/<int:job_id>/clone", methods=['PUT'])
@requires_auth
def clone_job(username, group, email, job_id):
    """
    Clone the specified job
    """
    # Job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s JobClone user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    # Get previous job description
    backend = ProminenceBackend(app.config)
    (_, identity, iwd, _, _, _, status) = backend.get_job_unique_id(job_id)

    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()

    try:
        with open(iwd+  '/.job.json') as json_file:
            job_json = json.load(json_file)
    except:
        return job_clone_error()

    (return_code, data) = backend.create_job(username, group, email, uid, job_json)

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

"""Routes for managing jobs"""
import uuid

from flask import Blueprint, jsonify, request
from flask import current_app as app

from auth import requires_auth
from backend import ProminenceBackend
import errors
import validate
from utilities import get_remote_addr

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
    (status, msg) = validate.validate_job(request.get_json())
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
                return errors.invalid_constraint()
        else:
            return errors.invalid_constraint()

    name_constraint = None
    if 'name' in request.args:
        name_constraint = request.args.get('name')
       
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

    backend = ProminenceBackend(app.config)
    data = backend.list_jobs(job_ids, username, active, completed, workflow, num, detail, constraint, name_constraint)

    return jsonify(data)

@jobs.route("/prominence/v1/jobs/<int:job_id>", methods=['GET'])
@requires_auth
def get_job(username, group, email, job_id):
    """
    Describe the specified job
    """
    app.logger.info('%s DescribeJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    backend = ProminenceBackend(app.config)
    data = backend.list_jobs([job_id], username, True, True, False, 1, 1, (None, None), None)
    return jsonify(data)

@jobs.route("/prominence/v1/jobs/<int:job_id>/exec", methods=['POST'])
@requires_auth
def exec_in_job(username, group, email, job_id):
    """
    Execute a command in a job
    """
    app.logger.info('%s ExecJob user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    if app.config['ENABLE_EXEC'] != 'True':
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)

    (_, identity, iwd, _, _, name, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return errors.no_such_job()
    if username != identity:
        return errors.not_auth_job()
    if status != 2:
        return errors.job_not_running()

    command = []
    if 'command' in request.args:
        command = str(request.args.get('command')).split(',')

    output = backend.execute_command(job_id, iwd, command)
    if output is not None:
        return output, 200
 
    return jsonify({'error':'Unable to execute command'}), 400

@jobs.route("/prominence/v1/jobs", methods=['DELETE'])
@requires_auth
def delete_jobs(username, group, email):
    """
    Delete the specified job(s)
    """
    if 'id' not in request.args:
        return jsonify({'error':'a job id or list of job ids must be provided'}), 400

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

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if not identity:
        return errors.no_such_job()
    if username != identity:
        return errors.not_auth_job()

    stdout = backend.get_stdout(uid, iwd, out, err, job_id, name)
    if stdout is None:
        return errors.no_stdout()
    else:
        return stdout

@jobs.route("/prominence/v1/jobs/<int:job_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr(username, group, email, job_id):
    """
    Return the standard error from the specified job
    """
    app.logger.info('%s GetStdErr user:%s group:%s id:%d' % (get_remote_addr(request), username, group, job_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, out, err, name, _) = backend.get_job_unique_id(job_id)
    if not identity:
        return errors.no_such_job()
    if username != identity:
        return errors.not_auth_job()

    stderr = backend.get_stderr(uid, iwd, out, err, job_id, name)
    if stderr is None:
        return errors.no_stderr()
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
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)
    (uid, identity, _, _, _, name, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return errors.no_such_job()
    if username != identity:
        return errors.not_auth_job()
    if status != 2:
        return errors.job_not_running()

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
        return errors.func_disabled()

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, status) = backend.get_job_unique_id(job_id)
    if not identity:
        return errors.no_such_job()
    if username != identity:
        return errors.not_auth_job()
    if status != 2:
        return errors.job_not_running()

    if 'path' in request.args:
        path = request.args.get('path')
    else:
        return jsonify({'error':'A path to snapshot must be specified'}), 400

    path = backend.validate_snapshot_path(iwd, path)
    if not path:
        return jsonify({'error':'Invalid path'}), 400

    backend.create_snapshot(uid, job_id, path)
    return jsonify({}), 200

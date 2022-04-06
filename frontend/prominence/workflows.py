"""Routes for managing workflows"""
import uuid

from flask import Blueprint, jsonify, request
from flask import current_app as app

from .auth import requires_auth
from .backend import ProminenceBackend
from .errors import invalid_constraint, no_such_workflow, no_stdout, no_stderr, not_auth_workflow, workflow_id_required
from .validate import validate_workflow
from .utilities import get_remote_addr

workflows = Blueprint('workflows', __name__)

@workflows.route("/prominence/v1/workflows", methods=['GET'])
@requires_auth
def list_workflows(username, group, email):
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

    detail = 0
    if 'detail' in request.args:
        detail = 1

    workflow_ids = []
    if 'id' in request.args:
        workflow_ids = request.args.get('id').split(',')
        # Assume both active workflows and completed workflows
        completed = True
        active = True

    backend = ProminenceBackend(app.config)
    data = backend.list_workflows(workflow_ids, username, active, completed, num, detail, constraint, name_constraint)

    return jsonify(data)

@workflows.route("/prominence/v1/workflows/<int:workflow_id>", methods=['GET'])
@requires_auth
def get_workflow(username, group, email, workflow_id):
    """
    Describe a workflow
    """
    app.logger.info('%s DescribeWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    data = backend.list_workflows([workflow_id], username, True, True, 1, 1, (None, None), None)
    return jsonify(data)

@workflows.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stdout", methods=['GET'])
@requires_auth
def get_stdout_wf(username, group, email, workflow_id, job):
    """
    Return the standard output from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if not job:
        job = 0
    if not identity:
        return no_such_workflow()
    if username != identity:
        return not_auth_workflow()

    stdout = backend.get_stdout(uid, iwd, None, None, -1, job, -1)
    if stdout is None:
        return no_stdout()
    else:
        return stdout

@workflows.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/stderr", methods=['GET'])
@requires_auth
def get_stderr_wf(username, group, email, workflow_id, job):
    """
    Return the standard error from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if not job:
        job = 0
    if not identity:
        return no_such_workflow()
    if username != identity:
        return not_auth_workflow()

    stderr = backend.get_stderr(uid, iwd, None, None, -1, job, -1)
    if stderr is None:
        return no_stderr()
    else:
        return stderr

@workflows.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/<int:instance_id>/stdout", methods=['GET'])
@requires_auth
def get_stdout_wf_jf(username, group, email, workflow_id, job, instance_id):
    """
    Return the standard output from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if not identity:
        return no_such_workflow()
    if username != identity:
        return not_auth_workflow()

    stdout = backend.get_stdout(uid, iwd, None, None, workflow_id, None, instance_id)
    if stdout is None:
        return no_stdout()
    else:
        return stdout

@workflows.route("/prominence/v1/workflows/<int:workflow_id>/<string:job>/<int:instance_id>/stderr", methods=['GET'])
@requires_auth
def get_stderr_wf_jf(username, group, email, workflow_id, job, instance_id):
    """
    Return the standard error from the specified job from a workflow
    """
    app.logger.info('%s GetWorkflowStdOut user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, _) = backend.get_job_unique_id(workflow_id)
    if not identity:
        return no_such_workflow()
    if username != identity:
        return not_auth_workflow()

    stderr = backend.get_stderr(uid, iwd, None, None, workflow_id, None, instance_id)
    if stderr is None:
        return no_stderr()
    else:
        return stderr

@workflows.route("/prominence/v1/workflows/<int:workflow_id>", methods=['DELETE'])
@requires_auth
def delete_workflow(username, group, email, workflow_id):
    """
    Delete a workflow
    """
    app.logger.info('%s DeleteWorkflow user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.delete_workflow(username, [workflow_id])

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@workflows.route("/prominence/v1/workflows", methods=['DELETE'])
@requires_auth
def delete_workflows(username, group, email):
    """
    Delete the specified workflow(s)
    """
    if 'id' not in request.args:
        return workflow_id_required()

    app.logger.info('%s DeleteWorkflows user:%s group:%s id:%s' % (get_remote_addr(request), username, group, request.args.get('id')))

    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.delete_workflow(username, request.args.get('id').split(','))

    if return_code == 0:
        return jsonify(data), 200
    return jsonify(data), 400

@workflows.route("/prominence/v1/workflows", methods=['POST'])
@requires_auth
def submit_workflow(username, group, email):
    """
    Create a new workflow
    """
    # Create job unique identifier
    uid = str(uuid.uuid4())

    app.logger.info('%s WorkflowSubmission user:%s group:%s uid:%s' % (get_remote_addr(request), username, group, uid))

    # Validate the input JSON
    (status, msg) = validate_workflow(request.get_json())
    if not status:
        return jsonify({'error': msg}), 400

    # Create workflow
    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.create_workflow(username, group, email, uid, request.get_json())

    retval = 201
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@workflows.route("/prominence/v1/workflows/<int:workflow_id>", methods=['PUT'])
@requires_auth
def rerun_workflow(username, group, email, workflow_id):
    """
    Re-run any failed jobs from a completed workflow
    """

    app.logger.info('%s WorkflowReRun user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (return_code, data) = backend.rerun_workflow(username, group, email, workflow_id)

    retval = 200
    if return_code == 1:
        retval = 400

    return jsonify(data), retval

@workflows.route("/prominence/v1/workflows/<int:workflow_id>/remove", methods=['PUT'])
@requires_auth
def remove_workflow(username, group, email, workflow_id):
    """
    Remove a completed workflow from the queue
    """
    app.logger.info('%s RemoveFromQueue user:%s group:%s id:%d' % (get_remote_addr(request), username, group, workflow_id))

    backend = ProminenceBackend(app.config)
    (uid, identity, iwd, _, _, _, status) = backend.get_job_unique_id(workflow_id)
    if not identity:
        return no_such_workflow()
    if username != identity:
        return not_auth_workflow()

    if not backend.remove_workflow(workflow_id):
        return removal_failed()

    return jsonify({}), 200

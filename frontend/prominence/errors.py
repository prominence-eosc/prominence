"""User error messages"""
from flask import current_app as app
from flask import jsonify

def job_clone_error():
    """
    Unable to clone job
    """
    with app.app_context():
        return jsonify({'error':'Unable to get JSON description of the specified job'}), 400

def workflow_clone_error():
    """
    Unable to clone workflow
    """
    with app.app_context():
        return jsonify({'error':'Unable to get JSON description of the specified workflow'}), 400

def no_such_job():
    """
    User has specified an invalid job id
    """
    with app.app_context():
        return jsonify({'error':'Job does not exist'}), 400

def no_such_workflow():
    """
    User has specified an invalid workflow id
    """
    with app.app_context():
        return jsonify({'error':'Workflow does not exist'}), 400

def not_auth_job():
    """
    User is not authorised to access the specified job
    """
    with app.app_context():
        return jsonify({'error':'Not authorized to access this job'}), 403

def not_auth_workflow():
    """
    User is not authorised to access the specified workflow
    """
    with app.app_context():
        return jsonify({'error':'Not authorized to access this workflow'}), 403

def no_stdout():
    """
    There is no standard output
    """
    with app.app_context():
        return jsonify({'error':'stdout does not exist'}), 400

def no_stderr():
    """
    There is no standard error
    """
    with app.app_context():
        return jsonify({'error':'stderr does not exist'}), 400

def invalid_constraint():
    """
    User has specified an invalid constraint
    """
    with app.app_context():
        return jsonify({'error':'Invalid constraint'}), 400

def job_not_running():
    """
    User has tried to perform an operation which requires a running job, but the job is not running
    """
    with app.app_context():
        return jsonify({'error':'Job is not running'}), 400

def func_disabled():
    """
    Functionality disabled
    """
    with app.app_context():
        return jsonify({'error':'Functionality disabled'}), 401

def job_removal_failed():
    """
    User has tried to remove a job which does not exist or has already completed
    """
    with app.app_context():
        return jsonify({'error':'Job is no longer in the queue'}), 400

def workflow_removal_failed():
    """
    User has tried to remove a workflow which does not exist or has already completed
    """
    with app.app_context():
        return jsonify({'error':'Workflow is no longer in the queue'}), 400

def start_date_missing():
    """
    User has requested accounting data but the start date is missing
    """
    with app.app_context():
        return jsonify({'error':'Start date must be provided'}), 400

def end_date_missing():
    """
    User has requested accounting data but the end data is missing
    """
    with app.app_context():
        return jsonify({'error':'End date must be provided'}), 400

def usage_data_error():
    """
    There is a problem obtaining usage data
    """
    with app.app_context():
        return jsonify({'error':'Unable to retrieve usage data'}), 400

def kv_error():
    """
    There is a problem with the kv store
    """
    with app.app_context():
        return jsonify({'error':'Unable to access key-value store'}), 400

def ttl_not_specified():
    """
    TTL not specified when creating lease
    """
    with app.app_context():
        return jsonify({'error':'TTL not specified'}), 400

def lease_not_found():
    """
    Lease not found
    """
    with app.app_context():
        return jsonify({'error':'Lease not found'}), 400

def no_such_key():
    """
    No such key
    """
    with app.app_context():
        return jsonify({'error':'No such key'}), 404

def key_not_specified():
    """
    Key not specified
    """
    with app.app_context():
        return jsonify({'error':'Key not specified'}), 400

def no_value_provided():
    """
    No value provided
    """
    with app.app_context():
        return jsonify({'error':'No value provided'}), 400

def value_too_big():
    """
    Value too large
    """
    with app.app_context():
        return jsonify({'error':'Content of value too large'}), 400

def auth_failure():
    """
    Authentication failure
    """
    with app.app_context():
        return jsonify({'error':'Authentication failure'}), 401

def oidc_error():
    """
    Problem connecting to the OIDC server
    """
    with app.app_context():
        return jsonify({'error':'Unable to connect to OIDC server'}), 401

def command_failed():
    """
    Unable to execute command
    """
    with app.app_context():
        return jsonify({'error':'Unable to execute command'}), 400

def job_id_required():
    """
    Job id or list of ids missing
    """
    with app.app_context():
        return jsonify({'error':'a job id or list of job ids must be provided'}), 400

def workflow_id_required():
    """
    Workflow id or list of workflow ids missing
    """
    with app.app_context():
        return jsonify({'error':'a workflow id or list of workflow ids must be provided'}), 400

def snapshot_path_required():
    """
    Snapshot path required
    """
    with app.app_context():
        return jsonify({'error':'A path to snapshot must be specified'}), 400

def snapshot_invalid_path():
    """
    An invalid path was specified
    """
    with app.app_context():
        return jsonify({'error':'Invalid path'}), 400


"""User error messages"""
from flask import current_app as app
from flask import jsonify

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

def removal_failed():
    """
    User has tried to remove a job which does not exist or has already completed
    """
    with app.app_context():
        return jsonify({'error':'Job is no longer in the queue'}), 400

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

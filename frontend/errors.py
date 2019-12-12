from flask import jsonify

NO_SUCH_JOB = jsonify({'error':'Job does not exist'}), 400
NO_SUCH_WORKFLOW = jsonify({'error':'Workflow does not exist'}), 400
NOT_AUTH_JOB = jsonify({'error':'Not authorized to access this job'}), 403
NOT_AUTH_WORKFLOW = jsonify({'error':'Not authorized to access this workflow'}), 403
NO_STDOUT = jsonify({'error':'stdout does not exist'}), 400
NO_STDERR = jsonify({'error':'stderr does not exist'}), 400
INVALID_CONSTRAINT = jsonify({'error':'Invalid constraint'}), 400
JOB_NOT_RUNNING = jsonify({'error':'Job is not running'}), 400
FUNC_DISABLED = jsonify({'error':'Functionality disabled'}), 401

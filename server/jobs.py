import uuid

from backend import ProminenceBackend
import errors
import validate

def list_jobs(username, group, email):
    """
    List jobs
    """
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

    return data


import configparser
import json
import re
import time
import logging
import sqlite3
import uuid
import requests
from requests.auth import HTTPBasicAuth
import classad

from create_infrastructure import deploy

# Read config file
CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

# Logging
logger = logging.getLogger('workflows.handler')

def get_group_from_dir(dir_name):
    """
    """
    group = dir_name
    match = re.search(r'(.*)/\d\d/\d\d/\d\d/\d\d/\d\d', dir_name)
    if match:
        group = match.group(1)
    return group

def get_job_json(filename, sandbox_dir):
    """
    """
    filename_str = filename.replace(sandbox_dir, '')
    match = re.search(r'/([\w\-]+)/([\w\-\_]+)/\d\d/\d\d/\d\d/\d\d/\d\d/job.json', filename_str)
    if match:
        return '%s/%s/%s/job.json' % (sandbox_dir, match.group(1), match.group(2))
    return filename

def get_num_cpus(job_cpus, num_idle_jobs):
    cpus_select = job_cpus
    scaling = 1

    if job_cpus <= 4:
        cpus_select = -1
        for i in range(0, 17):
            if i*job_cpus <= 16 and i <= num_idle_jobs:
                if i*job_cpus > cpus_select:
                    cpus_select = i*job_cpus
                    scaling = i

    return (scaling, cpus_select)

def get_worker_node_status():
    # Get status of worker nodes
    logger.info('Getting status of existing worker nodes...')

def get_infrastructure_status_with_retries(infra_id):
    """
    Get infrastructure status with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    status = None
    cloud = None
    reason = None
    while count < max_retries and status is None:
        (status, reason, cloud) = get_infrastructure_status(infra_id)
        count += 1
        time.sleep(count/2)
    return (status, reason, cloud)

def get_infrastructure_status(infra_id):
    """
    Get infrastructure status
    """
    try:
        response = requests.get('%s/%s' % (CONFIG.get('imc', 'url'), infra_id),
                                auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                   CONFIG.get('imc', 'password')),
                                cert=(CONFIG.get('imc', 'ssl-cert'),
                                      CONFIG.get('imc', 'ssl-key')),
                                verify=CONFIG.get('imc', 'ssl-cert'),
                                timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return (None, None, None)
    except requests.exceptions.RequestException:
        return (None, None, None)
    if response.status_code == 200:
        return (response.json()['status'], response.json()['status_reason'], response.json()['cloud'])
    return (None, None, None)

def delete_infrastructure_with_retries(infra_id):
    """
    Delete infrastructure with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    success = -1
    while count < max_retries and success != 0:
        success = delete_infrastructure(infra_id)
        count += 1
        time.sleep(count/2)
    return success

def delete_infrastructure(infra_id):
    """
    Delete infrastructure
    """
    try:
        response = requests.delete('%s/%s' % (CONFIG.get('imc', 'url'), infra_id),
                                   auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                      CONFIG.get('imc', 'password')),
                                   cert=(CONFIG.get('imc', 'ssl-cert'),
                                         CONFIG.get('imc', 'ssl-key')),
                                   verify=CONFIG.get('imc', 'ssl-cert'),
                                   timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return 2
    except requests.exceptions.RequestException:
        return 1
    if response.status_code == 200:
        return 0
    return 1

class DatabaseGlobal():
    def __init__(self, db_file):
        self._db_file = db_file
        self.init_db()

    def init_db(self):
        """
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()

            # Create workflows table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              workflows(id INT PRIMARY KEY,
                                        status TEXT,
                                        iwd TEXT,
                                        identity TEXT,
                                        groups TEXT,
                                        uid TEXT
                          )''')

            db.commit()
            db.close()
        except Exception as err:
            logger.info('Unable to connect to DB due to: %s', err)
            exit(1)

    def add_workflow(self, workflow_id, iwd, identity, groups, uid):
        """
        Add a workflow
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('INSERT INTO workflows (id, iwd, identity, groups, uid, status) VALUES (%d, "%s", "%s", "%s", "%s", "created")' % (workflow_id, iwd, identity, groups, uid))
            db.commit()
            db.close()
        except Exception as err:
            if 'UNIQUE constraint failed' in str(err):
                return False
            else:
                return None
        return True

    def is_workflow(self, workflow_id):
        """
        Check if a workflow has been added
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('SELECT count(*) FROM workflows WHERE id=%d' % workflow_id)
            rows = cursor.fetchall()
            db.close()
        except Exception as err:
            logger.info('Got exception in is_workflow: %s', err)
            return None

        if len(rows) > 0:
            if rows[0][0] > 0:
                return True

        return False

    def set_workflow_status(self, workflow_id, status):
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute("UPDATE workflows SET status='%s' WHERE id='%s'" % (status, workflow_id))
            db.commit()
            db.close()
        except Exception as err:
            logger.info('Got exception in set_workflow_status: %s', err)
            return None
        return True

    def get_workflows_by_status(self, status):
        infras = []
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute("SELECT id,iwd,identity,groups,uid FROM workflows WHERE status='%s'" % status)
            for row in cursor.fetchall():
                infras.append({'id': row[0], 'iwd': row[1], 'identity': row[2], 'groups': row[3], 'uid': row[4]})
            db.close()
        except Exception as err:
            logger.info('Got exception in get_workflows_by_status: %s', err)

        return infras

class Database():
    def __init__(self, db_file):
        self._db_file = db_file
        self.init_db()

    def init_db(self):
        """
        Initialize database
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()

            # Create deployments table
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              deployments(infra_id TEXT PRIMARY KEY,
                                          status TEXT,
                                          created INT,
                                          job INT,
                                          cpus INT,
                                          memory INT,
                                          disk INT,
                                          nodes INT
                          )''')

            # Create jobs table - jobs in here have their own infrastructure
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              jobs(id INT PRIMARY KEY
                          )''')

            db.commit()
            db.close()
        except Exception as err:
            logger.info('Unable to connect to DB due to: %s', err)
            exit(1)

    def add_infra(self, infra_id, job_id, cpus, memory, disk, nodes):
        """
        Add an infrastructure
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('INSERT INTO deployments (infra_id, job, cpus, memory, disk, nodes, status, created) VALUES ("%s", %d, %d, %d, %d, %d, "created", %d)' % (infra_id, job_id, cpus, memory, disk, nodes, time.time()))
            db.commit()
            db.close()
        except Exception as err:
            if 'UNIQUE constraint failed' in str(err):
                return False
            else:
                logger.info('Got exception in add_infra: %s', err)
                return None
        return True

    def set_infra_status(self, infra_id, status):
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute("UPDATE deployments SET status='%s' WHERE infra_id='%s'" % (status, infra_id))
            db.commit()
            db.close()
        except Exception as err:
            logger.info('Got exception in set_infra_status: %s', err)
            return None
        return True

    def add_job(self, job_id):
        """
        Add a job
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('INSERT INTO jobs (id) VALUES (%d)' % job_id)
            db.commit()
            db.close()
        except Exception as err:
            if 'UNIQUE constraint failed' in str(err):
                return False
            else:
                return None
        return True

    def get_matching_infras(self, cpus, memory, disk, nodes, job):
        infras = []
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute("SELECT infra_id,created FROM deployments WHERE cpus=%d AND memory=%d AND disk=%d AND nodes=%d AND status='created' AND job=%d" % (cpus, memory, disk, nodes, job))
            for row in cursor.fetchall():
                infras.append({'id': row[0], 'created': row[1]})
            db.close()
        except Exception as err:
            logger.info('Got exception in get_matching_infras: %s', err)

        return infras

    def get_infras_by_status(self, status):
        infras = []
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute("SELECT infra_id FROM deployments WHERE status='%s'" % status)
            for row in cursor.fetchall():
                infras.append(row[0])
            db.close()
        except Exception as err:
            logger.info('Got exception in get_infras_by_status: %s', err)

        return infras

    def get_infra_status_from_job(self, job):
        result = (None, None)
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('SELECT status, infra_id FROM deployments WHERE job=%d' % job)
            rows = cursor.fetchall()
            db.close()
        except Exception as err:
            logger.info('Got exception in get_infra_status_from_job: %s', err)

        if len(rows) > 0:
            result = (rows[0][0], rows[0][1])

        return result

    def is_job(self, job_id):
        """
        Check if a job has been added
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('SELECT count(*) FROM jobs WHERE id=%d' % job_id)
            rows = cursor.fetchall()
            db.close()
        except Exception as err:
            logger.info('Got exception in is_job: %s', err)

        if len(rows) > 0:
            if rows[0][0] > 0:
                return True

        return False

    def num_infras(self):
        """
        Return the number of infras
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('SELECT count(*) FROM deployments')
            rows = cursor.fetchall()
            db.close()
        except Exception as err:
            logger.info('Got exception in num_infras: %s', err)
            return -1

        return rows[0][0]

#db = Database(DB_FILE)
#db.add_infra('a001', 2, 4, 10, 1)
#db.add_infra('a002', 2, 4, 10, 1)
#print(db.get_matching_infras(2, 4, 10, 1))
#db.add_job(1023)
#db.add_job(1023)
#print(db.is_job(1023))
#print(db.is_job(1024))
#print(db.set_infra_status('a001', 'running'))
#print(db.get_infras_by_status('running'))
#print(db.get_infras_by_status('created'))

def get_jobs_by_state(iwd):
    """
    Read the DAGMan log files to generate list of individual jobs by current state
    """
    details = {}
    with open('%s/job.dag' % iwd, 'r') as job:
        for line in job:
            match = re.search(r'JOB\s(.*)\s(.*)\sDIR\s(.*)', line)
            if match:
                details[match.group(1)] = match.group(3)

    jobs_submitted = []
    jobs_executing = []
    jobs_exited = []

    with open('%s/job.dag.dagman.out' % iwd, 'r') as dagman:
        for line in dagman:
            match = re.search(r'.*\sEvent:\sULOG_([\w\_]+)\sfor\sHTCondor\sNode\s(.*)\s\(([\d]+)\.[\d]+\.[\d]+\).*', line)
            if match:
                job = {}
                job['id'] = int(match.group(3))
                job['name'] = match.group(2)
                if job['name'] in details:
                    job['dir'] = details[job['name']]
                event_type = match.group(1)
                if event_type == 'SUBMIT':
                    jobs_submitted.append(job)
                elif event_type == 'EXECUTE':
                    jobs_executing.append(job)
                elif event_type in ('JOB_TERMINATED', 'JOB_HELD', 'JOB_ABORTED', 'JOB_EVICTED'):
                    jobs_exited.append(job)

                # A running or exited job will also appear in an earlier state, so ensure only
                # the most recent state is used
                if job in jobs_submitted and job in jobs_executing:
                    jobs_submitted.remove(job)
                if job in jobs_executing and job in jobs_exited:
                    jobs_executing.remove(job)
                if job in jobs_submitted and job in jobs_exited:
                    jobs_submitted.remove(job)

    return (jobs_submitted, jobs_executing, jobs_exited)

def add_workflow(dag_job_id, iwd, identity, groups, uid):
    """
    Add workflow if necessary
    """
    # Update main database
    dbg = DatabaseGlobal('/var/spool/prominence/db.dat')
    if not dbg.is_workflow(dag_job_id):
        dbg.add_workflow(dag_job_id, iwd, identity, groups, uid)

def manage_jobs(dag_job_id, iwd, identity, groups, uid):
    """
    Deploy infrastructure if necessary for running jobs from this workflow
    """
    # Get jobs by status
    (submitted, executing, exited) = get_jobs_by_state(iwd)
    logger.info('Jobs by state in workflow %d, submitted: %d, executing: %d, exited: %d', dag_job_id, len(submitted), len(executing), len(exited))

    # Unique set of directories, i.e. same resource requirements
    resources_groups = {}
    for job in submitted:
        filename = get_job_json('%s/%s/job.json' % (iwd, job['dir']), '/var/spool/prominence/sandboxes')

        group = get_group_from_dir(job['dir'])

        if group not in resources_groups:
            try:
                with open(filename, 'r') as json_file:
                    job_json = json.load(json_file)
                    resources_groups[group] = job_json['resources']
            except Exception as err:
                logger.info('Unable to open json file %s', filename)
                return

    # Consider each resource group
    for group in resources_groups:
        logger.info('Working on group "%s"', group)

        cpus = int(resources_groups[group]['cpus'])
        memory = int(resources_groups[group]['memory'])
        nodes = int(resources_groups[group]['nodes'])
        disk = int(resources_groups[group]['disk'])

        num_submitted = 0
        num_running = 0
        num_exited = 0

        for job in submitted:
            if get_group_from_dir(job['dir']) == group:
                num_submitted = num_submitted + 1
        for job in executing:
            if get_group_from_dir(job['dir']) == group:
                num_running = num_running + 1
        for job in exited:
            if get_group_from_dir(job['dir']) == group:
                num_exited = num_exited + 1

        (cpu_scaling, cpus_total) = get_num_cpus(cpus, num_submitted)

        logger.info('Group "%s" has %d submitted jobs, %d running jobs, %d exited jobs and WN load factor is %d', group, num_submitted, num_running, num_exited, cpu_scaling)

        # Connect to the DB
        db = Database('%s/db.dat' % iwd)

        # Get list of any existing infra ids matching this group which are not job specific
        ids = db.get_matching_infras(cpus, memory, disk, nodes, -1)

        # Flag if we need to deploy new infrastructure
        deploy_new_infra_now = False
        deploy_new_infra_num = 1

        # Check if we need to cleanup all infrastructure
        if num_exited > 0 and num_submitted == 0 and num_exited == 0:
            logger.info('All jobs have finished in this group "%s"', group)

            # Delete the infrastructure
            for infra in ids:
                logger.info('Deleting infrastructure %s', infra['id'])
                status = delete_infrastructure_with_retries(infra['id'])
                if status != 0:
                    logger.critical('Failed to delete infrastructure %s', infra['id'])
                else:
                    db.set_infra_status(infra['id'], 'deleted')

        # Check if we need to deploy any infras
        if num_submitted > 0 and not ids and not db.get_matching_infras(cpus, memory, disk, nodes, int(job['id'])):
            logger.info('Job resource group "%s" has no infrastructure, so creating', group)
            deploy_new_infra_now = True

        if num_submitted > 0 and int(100.0*num_submitted/cpu_scaling) > 40.0 and num_running > 0:
            logger.info('Job resource group "%s" may need more infrastructure', group)
            # Check status of any existing infrastructure
            most_recent_depl = 0
            most_recent_status = None
            for infra in ids:
                (status, reason, cloud) = get_infrastructure_status_with_retries(infra['id'])
                logger.info('Got status for infra: %s : %s, %s, %s', infra['id'], status, reason, cloud)
                if infra['created'] > most_recent_depl:
                    most_recent_depl = infra['created']
                    most_recent_status = status
            
            if most_recent_status == 'configured' and time.time() - most_recent_depl > 60*10:
                deploy_new_infra_now = True

        if deploy_new_infra_now:
            deploy_counter = 0
            while deploy_counter < deploy_new_infra_num:
                deploy_counter += 1
                logger.info('Creating new infrastructure for group "%s"', group)

                job_id = None
                for job in submitted:
                    if get_group_from_dir(job['dir']) == group:
                        job_id = job['id']

                # Deploy infrastructure
                count = db.num_infras()
                infra_id = None
                try:
                    (_, infra_id) = deploy(identity, groups, iwd, job['id'], job['name'], dag_job_id, uid, count, get_group_from_dir(job['dir']), cpu_scaling)
                except Exception as exc:
                    logger.critical('Got exception deploying infrastructure for job with id %d: %s', job['id'], exc)

                if infra_id:
                    logger.info('Got infrastructure id %s', infra_id)

                    # If job is unique specify job id, otherwise specify -1 indicating multiple jobs
                    job_id_infra = -1
                    if num_submitted == 1 and job_id:
                        job_id_infra = job_id

                    # Update database
                    status = db.add_infra(infra_id, job_id_infra, cpus, memory, disk, nodes)
                    if not status:
                        logger.info('Got invalid status from add_infra for job %d', int(job['id']))

def cleanup_all_jobs(iwd):
    """
    """
    # Get list of infrastructures to cleanup
    db = Database('%s/db.dat' % iwd)
    infras = db.get_infras_by_status('created') + db.get_infras_by_status('configured')

    for infra in infras:
        logger.info('Deleting infrastructure %s', infra)
        status = delete_infrastructure_with_retries(infra)
        if status != 0:
            return False
        else:
            db.set_infra_status(infra, 'deleted')

    return True

def cleanup_jobs(iwd):
    """
    """
    db = Database('%s/db.dat' % iwd)

    # Get jobs by status
    (_, _, exited) = get_jobs_by_state(iwd)

    # Cleanup infrastructures for individual jobs
    for job in exited:
        # TODO: check if infra is ONLY for this job
        (status, infra_id) = db.get_infra_status_from_job(int(job['id']))
        if status and infra_id:
            if status != 'deleted':
                logger.info('Deleting infrastructure %s for job %d as it finished', infra_id, int(job['id']))
                status = delete_infrastructure_with_retries(infra_id)
                if status != 0:
                    return False
                else:
                    db.set_infra_status(infra_id, 'deleted')

    # Cleanup infrastructure for groups of jobs which have all finished

    return True

def update_workflows():
    """
    Manage infrastructure as necessary for all running workflows
    """
    db = DatabaseGlobal('/var/spool/prominence/db.dat')
    workflows = db.get_workflows_by_status('created')
    for workflow in workflows:
        # Check status of workflow
        dag_status = None
        try:
            class_ads = classad.parseAds(open('%s/workflow.dag.status' % workflow['iwd'], 'r'))
            for class_ad in class_ads:
                if class_ad['Type'] == 'DagStatus':
                    dag_status = class_ad['DagStatus']
        except Exception as exc:
            logger.info('Got exception opening workflow.dag.status file for workflow %d: %s', int(workflow['id']), exc)
            continue

        if dag_status and dag_status != 3:
            # If workflow has finished for whatever reason, delete it
            logger.info('Workflow %d has completed because dag status is %d', int(workflow['id']), int(dag_status))
            status = cleanup_all_jobs(workflow['iwd'])
            if status:
                db.set_workflow_status(workflow['id'], 'deleted')
        elif dag_status and dag_status == 3:
            # If a workflow is still running, deploy any jobs if necessary and cleanup any jobs if necessary
            logger.info('Workflow %d is running', int(workflow['id']))
            manage_jobs(int(workflow['id']), workflow['iwd'], workflow['identity'], workflow['groups'], workflow['uid'])
            #cleanup_jobs(workflow['iwd'])

import configparser
from datetime import datetime
import json
import re
import os
import time
import logging
import pickle
import sqlite3
import uuid
import requests
from requests.auth import HTTPBasicAuth
import classad

from create_infrastructure import deploy
from update_db import update_workflow_db, add_job_to_workflow_db, update_job_in_db, find_incomplete_jobs, get_workflow_from_db, get_job, create_jobs, create_job

# Read config file
CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

# Logging
logger = logging.getLogger('workflows.handler')

def get_dag_start_time(iwd):
    try:
        with open('%s/job.dag.dagman.out' % iwd, 'r') as dagman:
            line = dagman.readline()
    except Exception:
        return None

    match = re.match(r'(\d\d\/\d\d\/\d\d\s\d\d:\d\d:\d\d).*', line)
    if match:
        timestamp = match.group(1)
        return condor_datetime_to_epoch(timestamp)

    return None

def datetime_to_epoch(string):
    utc_time = datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    return int((utc_time - datetime(1970, 1, 1)).total_seconds())

def condor_datetime_to_epoch(string):
    utc_time = datetime.strptime(string, "%m/%d/%y %H:%M:%S")
    return int((utc_time - datetime(1970, 1, 1)).total_seconds())

def get_group_from_dir(dir_name):
    """
    """
    group = dir_name
    match = re.search(r'(.*)/\d\d', dir_name)
    if match:
        group = match.group(1)
    return group

def get_job_json(filename, sandbox_dir):
    """
    """
    filename_str = filename.replace(sandbox_dir, '')
    match = re.search(r'/([\w\-]+)/([\w\-\_]+)/\d\d/job.json', filename_str)
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

def get_infrastructure_in_status(status):
    """
    Get list of infrastructures in specified status
    """
    try:
        response = requests.get('%s' % CONFIG.get('imc', 'url'),
                                params={'status': status},
                                auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                   CONFIG.get('imc', 'password')),
                                cert=(CONFIG.get('imc', 'ssl-cert'),
                                      CONFIG.get('imc', 'ssl-key')),
                                verify=CONFIG.get('imc', 'ssl-cert'),
                                timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.RequestException:
        return []
    if response.status_code == 200:
        return response.json()
    return []

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

            # Create lock
            cursor.execute('''CREATE TABLE IF NOT EXISTS
                              lock(status INT PRIMARY KEY,
                                   updated INT
                          )''')

            db.commit()
            db.close()
        except Exception as err:
            logger.info('Unable to connect to DB due to: %s', err)
            exit(1)

    def acquire_lock(self):
        """
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('INSERT INTO lock (status, updated) VALUES (42, %d)' % time.time())
            db.commit()
            db.close()
        except Exception as err:
            if 'UNIQUE constraint failed' in str(err):
                return False
            else:
                return None
        return True

    def release_lock(self):
        """
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('DELETE FROM lock')
            db.commit()
            db.close()
        except Exception as err:
            return None
        return True

    def get_lock_time(self):
        """
        Check the time a lock was set
        """
        try:
            db = sqlite3.connect(self._db_file)
            cursor = db.cursor()
            cursor.execute('SELECT updated FROM lock WHERE status=42')
            rows = cursor.fetchall()
            db.close()
        except Exception as err:
            logger.info('Got exception in get_lock_time: %s', err)
            return 0

        if len(rows) > 0:
            return rows[0][0]

        return 0

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
                logger.info('Workflow with id %d already known', workflow_id)
                return False
            else:
                logger.error('Got exception when trying to add workflow: %s', err)
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

def get_jobs_by_state(iwd, workflow_id):
    """
    Read the DAGMan log files to generate list of individual jobs by current state. We use the file
    job.dag.dagman.out to recreate the current status of jobs, making sure we only read new events
    from it each time as it can grow very large for large workflows.
    """
    details = {}
    dirs = []
    with open('%s/job.dag' % iwd, 'r') as job:
        for line in job:
            match = re.search(r'JOB\s(.*)\s(.*)\sDIR\s(.*)', line)
            if match:
                details[match.group(1)] = match.group(3)
                if match.group(3) not in dirs:
                    dirs.append(match.group(3))

    jobs_per_dir = {}
    for directory in dirs:
        count = 0
        for detail in details:
            if details[detail] == directory:
                count += 1
        jobs_per_dir[directory] = count

    jobs_submitted = []
    jobs_executing = []
    jobs_exited = []
    jobs_name_map = {}
    position = 0

    # Get workflow from DB
    workflow = get_workflow_from_db(workflow_id)
    if not workflow:
        logger.critical('Unable to get workflow from DB in get_jobs_by_state')
        return (None, None, None)

    # Get job json for each job
    job_jsons = {}
    for directory in dirs:
        directory_name = directory
        if '/' in directory:
            directory_name = directory.split('/')[0]

        try:
            filename = '%s/%s/job.json' % (iwd, directory_name)
            with open(filename, 'r') as json_file:
                job_jsons[directory] = json.load(json_file)
        except Exception as err:
            logger.info('Unable to open job json file %s due to: %s', filename, err)
            return (None, None, None)

    # Get workflow name
    try:
        filename = '%s/workflow.json' % iwd
        with open(filename, 'r') as json_file:
            workflow_json = json.load(json_file)
    except Exception as err:
        logger.info('Unable to open workflow json file %s due to %s', filename, err)
        return (None, None, None)

    workflow_name = None
    if 'name' in workflow_json:
        workflow_name = workflow_json['name']

    # Get existing data if it exists
    pickled_file = '%s/job.dag.nodes.log.pickle' % iwd
    if os.path.isfile(pickled_file):
        logger.info('Reading cached job status info from pickle')
        with open(pickled_file, 'rb') as fd:
            try:
                jobs = pickle.load(fd)
            except Exception as err:
                logger.error('Got exception reading from pickle: %s', err)
            else:
                jobs_submitted = jobs['submitted']
                jobs_executing = jobs['executing']
                position = jobs['position']
                jobs_name_map = jobs['map']
                logger.info('Will start reading from line %d', position)

    line_no = 0
    jobs_new_added = 0
    jobs_updated = 0

    data_jobs_added = {}
    data_jobs_removed = []

    with open('%s/job.dag.nodes.log' % iwd, 'r') as dagman:
        for line in dagman:
            # If we have already read part of the file, skip these lines
            if line_no < position:
                line_no = line_no + 1
                continue
            else:
                line_no = line_no + 1

            job = {}

            # Look for job starts
            match = re.search(r'000\s\(([\d]+)\.000\.000\)\s(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)\sJob\ssubmitted\sfrom\shost', line)
            if match:
                condor_job_id = int(match.group(1))
                condor_job_time = match.group(2)

            # Look for executing jobs
            match = re.search(r'001\s\(([\d]+)\.000\.000\)\s(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)\sJob\sexecuting\son\shost', line)
            if match:
                condor_job_id = int(match.group(1))
                condor_job_time = match.group(2)
                condor_job_name = None

                if condor_job_id in jobs_name_map:
                    condor_job_name = jobs_name_map[condor_job_id]

                #logger.info('Job executing with id %d at %s with name %s', condor_job_id, condor_job_time, condor_job_name)

                job = {'id': condor_job_id, 'name': condor_job_name}
                if condor_job_name in details:
                    job['dir'] = details[job['name']]

                jobs_executing.append(job)
                jobs_updated = jobs_updated + 1

                if job['id'] in data_jobs_added:
                    data_jobs_removed.append(job['id'])
                    create_job(data_jobs_added[job['id']])
                update_job_in_db(job['id'], 2, start_date=datetime_to_epoch(condor_job_time))

                condor_job_id = -1

            # Look for failed jobs
            match = re.search(r'\d\d\d\s\(([\d]+)\.000\.000\)\s(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)\sJob\swas', line)
            if match:
                condor_job_id = int(match.group(1))
                condor_job_time = match.group(2)
                condor_job_name = None

                if condor_job_id in jobs_name_map:
                    condor_job_name = jobs_name_map[condor_job_id]

                #logger.info('Job terminated with id %d at %s with name %s', condor_job_id, condor_job_time, condor_job_name)

                job = {'id': condor_job_id, 'name': condor_job_name}
                if condor_job_name in details:
                    job['dir'] = details[job['name']]
                jobs_exited.append(job)

                condor_job_id = -1

            # Look for finished jobs
            match = re.search(r'\d\d\d\s\(([\d]+)\.000\.000\)\s(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)\sJob\sterm', line)
            if match:
                condor_job_id = int(match.group(1))
                condor_job_time = match.group(2)
                condor_job_name = None

                if condor_job_id in jobs_name_map:
                    condor_job_name = jobs_name_map[condor_job_id]

                #logger.info('Job finished with id %d at %s with name %s', condor_job_id, condor_job_time, condor_job_name)

                job = {'id': condor_job_id, 'name': condor_job_name}
                if condor_job_name in details:
                    job['dir'] = details[job['name']]
                jobs_exited.append(job)

                condor_job_id = -1

            # Handle submitted jobs when the "DAG Node" line appears
            match = re.search(r'\s\s\s\sDAG\sNode:\s([\w\-\_]+)', line)
            if match:
                condor_job_name = match.group(1)
                if condor_job_id != -1:
                    #logger.info('Job submitted with id %d at %s with name %s', condor_job_id, condor_job_time, condor_job_name)
                    jobs_name_map[condor_job_id] = condor_job_name

                    job = {'id': condor_job_id, 'name': condor_job_name}
                    if condor_job_name in details:
                        job['dir'] = details[job['name']]

                    jobs_submitted.append(job)
                    jobs_new_added = jobs_new_added + 1

                    data_jobs_added[job['id']] = get_job(workflow,
                                                         workflow_id,
                                                         job['id'],
                                                         job['name'],
                                                         job['dir'],
                                                         iwd,
                                                         datetime_to_epoch(condor_job_time),
                                                         workflow_name,
                                                         job_jsons[job['dir']])

                    condor_job_id = -1

            # A running or exited job will also appear in an earlier state, so ensure only
            # the most recent state is used
            if job:
                if job in jobs_submitted and job in jobs_executing:
                    jobs_submitted.remove(job)
                if job in jobs_executing and job in jobs_exited:
                    jobs_executing.remove(job)
                if job in jobs_submitted and job in jobs_exited:
                    jobs_submitted.remove(job)

    # Create all new jobs as necessary
    jobs_list = []
    for job_id in data_jobs_added:
        if job_id not in data_jobs_removed:
            jobs_list.append(data_jobs_added[job_id])

    if len(jobs_list) > 0:
        logger.info('Doing a bulk create of %d new jobs', len(jobs_list))
        create_jobs(jobs_list)

    logger.info('Jobs updated in DB: %d, jobs added to DB: %d', jobs_updated, jobs_new_added)

    try:
        with open(pickled_file, 'wb') as fd:
            pickle.dump({'position': line_no,
                         'submitted': jobs_submitted,
                         'executing': jobs_executing,
                         'map': jobs_name_map}, fd)
    except Exception as err:
        logger.critical('Unable to write pickle to disk due to: %s', err)

    return (jobs_submitted, jobs_executing, jobs_exited)

def add_workflow(dag_job_id, iwd, identity, groups, uid):
    """
    Add workflow if necessary
    """
    # Update main database
    dbg = DatabaseGlobal('/var/spool/prominence/database/db.dat')
    if not dbg.is_workflow(dag_job_id):
        dbg.add_workflow(dag_job_id, iwd, identity, groups, uid)

def manage_jobs(dag_job_id, iwd, identity, groups, uid):
    """
    Deploy infrastructure if necessary for running jobs from this workflow
    """
    # Get jobs by status
    (submitted, executing, exited) = get_jobs_by_state(iwd, dag_job_id)
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

        # Check status of existing infrastructures
        logger.info('Checking status of any existing infastructures...')
        # TODO: handle situation where existing infrastructures not yet working
        for infra in ids:
            (status, reason, cloud) = get_infrastructure_status_with_retries(infra['id'])
            logger.info('Got status for infra: %s : %s, %s, %s', infra['id'], status, reason, cloud)

            # Delete infrastructure in unable state
            if status == 'unable':
                logger.info('Deleting infrastructure %s due to status %s', infra['id'], status)
                status = delete_infrastructure_with_retries(infra['id'])
                if status == 0:
                    db.set_infra_status(infra['id'], 'deleted')

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

def update_workflows():
    """
    Manage infrastructure as necessary for all running workflows
    """
    db = DatabaseGlobal('/var/spool/prominence/database/db.dat')
    workflows = db.get_workflows_by_status('created')
    for workflow in workflows:
        logger.info('Working on workflow %d', int(workflow['id']))

        # Check status of workflow
        dag_status = None
        nodes_total = 0
        nodes_done = 0
        nodes_failed = 0

        try:
            class_ads = classad.parseAds(open('%s/workflow.dag.status' % workflow['iwd'], 'r'))
            for class_ad in class_ads:
                if class_ad['Type'] == 'DagStatus':
                    dag_status = class_ad['DagStatus']
                    nodes_total = class_ad['NodesTotal']
                    nodes_done = class_ad['NodesDone']
                    nodes_failed = class_ad['NodesFailed']
                    break
        except Exception as exc:
            logger.info('Got exception opening workflow.dag.status file for workflow %d: %s', int(workflow['id']), exc)
            continue

        if dag_status:
            logger.info('Updating workflow in DB')
            update_workflow_db(int(workflow['id']), dag_status, nodes_total, nodes_done, nodes_failed)

        if not os.path.isfile('%s/first.lock' % workflow['iwd']):
            start_time = get_dag_start_time(workflow['iwd'])
            logger.info('Got dag start time %d', start_time)
            if start_time:
                update_workflow_db(int(workflow['id']), time_start=start_time)
                try:
                    open('%s/first.lock' % workflow['iwd'], 'w+').close()
                except Exception as err:
                    logger.error('Got exception when creating lock file after first run: %s', err)

        if dag_status and dag_status != 3:
            # If workflow has finished for whatever reason, delete it
            logger.info('Workflow %d has completed because dag status is %d', int(workflow['id']), int(dag_status))
            (submitted, executing, exited) = get_jobs_by_state(workflow['iwd'], int(workflow['id']))
            status = cleanup_all_jobs(workflow['iwd'])

            try:
                os.remove('%s/job.dag.nodes.log.pickle' % workflow['iwd'])
            except Exception as err:
                logger.error('Unable to remove pickle in %s', workflow['iwd'])

            if status:
                db.set_workflow_status(workflow['id'], 'deleted')
            #find_incomplete_jobs(int(workflow['id']))
        elif dag_status and dag_status == 3:
            # If a workflow is still running, deploy any jobs if necessary and cleanup any jobs if necessary
            logger.info('Workflow %d is running', int(workflow['id']))
            manage_jobs(int(workflow['id']), workflow['iwd'], workflow['identity'], workflow['groups'], workflow['uid'])


import json
import logging
import os
import re
import shutil
import uuid

from .utilities import condor_str, run
from .write_htcondor_job import write_htcondor_job

# Get an instance of a logger
logger = logging.getLogger(__name__)

def create_dir_structure(base_dir, num, total):
    """
    """
    if total > 10000:
        return '%s/%02d' % (base_dir, int(str(num)[-2:]))
    return base_dir

def write_parameter_value(value):
    """
    Write a parameter value, taking into account its type
    """
    output = None
    if isinstance(value, int) or int(value) == value:
        output = '%d' % value
    elif isinstance(value, float):
        output = '%f' % value
    elif isinstance(value, basestring):
        output = '%s' % value
    return output

def output_params(workflow):
    """
    Generate params
    """
    params = ''
    count = 0

    for job in workflow['jobs']:
        if 'outputFiles' in job:
            for filename in job['outputFiles']:
                params += ' --outfile %s=$(prominenceout%d) ' % (filename, count)
                count += 1

        if 'outputDirs' in job:
            for filename in job['outputDirs']:
                params += ' --outdir %s=$(prominenceout%d) ' % (filename, count)
                count += 1

    return params

def _output_urls(self, workflow, uid, label):
    """
    Generate output files/dirs
    """
    lists = ''
    count = 0

    for job in workflow['jobs']:
        if 'outputFiles' in job:
            for filename in job['outputFiles']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%d/%s' % (uid, label, os.path.basename(filename)),
                                                    604800)
                lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                count += 1

        if 'outputDirs' in job:
            for dirname in job['outputDirs']:
                url_put = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    'scratch/%s/%d/%s.tgz' % (uid, label, os.path.basename(dirname)),
                                                    604800)
                lists = lists + ' prominenceout%d="%s" ' % (count, url_put)
                count += 1

    return lists

def create_workflow(self, username, groups, email, uid, jwf):
    """
    Create a workflow
    """
    # Firstly, create the workflow sandbox
    job_sandbox = self.create_sandbox(uid)
    if job_sandbox is None:
        logger.critical('Unable to create workflow sandbox for user %s and job uid %s', username, uid)
        return (1, {"error":"Unable to create workflow sandbox"})

    # Workflow name
    wf_name = ''
    if 'name' in jwf:
        wf_name = str(jwf['name'])

    # Write the workflow JSON description to disk
    try:
        with open(job_sandbox + '/workflow.json', 'w') as fd:
            json.dump(jwf, fd)
    except IOError:
        logger.critical('Unable to write workflow.json for user %s and job uid %s', username, uid)
        return (1, {"error":"Unable to write workflow.json"})

    dag = []

    # Policies
    job_placement_policies = None
    if 'policies' in jwf:
        # Job retries
        if 'maximumRetries' in jwf['policies']:
            dag.append('RETRY ALL_NODES %d' % jwf['policies']['maximumRetries'])

        # If placement policies are defined in the workflow, apply these to all jobs
        if 'placement' in jwf['policies']:
            job_placement_policies = jwf['policies']['placement']

    jobs_in_dag = []
    for job in jwf['jobs']:
        # All jobs must have names
        if 'name' not in job:
            return (1, {"error":"All jobs in a workflow must have names"})

        # Add placement policies if defined
        if job_placement_policies:
            if 'policies' not in job:
                job['policies'] = {}
            job['policies']['placement'] = job_placement_policies

        # Check if this job has a factory
        job_factory = None
        if 'factories' in jwf:
            for factory in jwf['factories']:
                for job_in_factory in factory['jobs']:
                    if job['name'] == job_in_factory:
                        job_factory = factory

        # Create job sandbox
        try:
            os.makedirs(job_sandbox + '/' + job['name'])
            os.makedirs(job_sandbox + '/' + job['name'] + '/input')
        except IOError:
            logger.critical('Unable to create job sandbox directories for user %s and job uid %s', username, uid)
            return (1, {"error":"Unable to create job sandbox directories"})

        job_filename = job_sandbox + '/' + job['name'] + '/job.jdl'

        # Copy executable to job sandbox
        shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, job['name'], 'promlet.py'))
        os.chmod(job_sandbox + '/' + job['name'] + '/promlet.py', 0o775)

        if not job_factory:
            # Create dict containing HTCondor job
            (_, _, cjob) = self._create_htcondor_job(username,
                                                  groups,
                                                  email,
                                                  str(uuid.uuid4()),
                                                  job,
                                                  '%s/%s' % (job_sandbox, job['name']))
            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)

            # Write JDL
            if not write_htcondor_job(cjob, job_filename):
                return (1, {"error":"Unable to write JDL for job"})

            # Append job to DAG description
            dag.append('JOB %s job.jdl DIR %s' % ( job['name'], job['name']))
            dag.append('VARS %s prominencecount="0"' % job['name'])
            jobs_in_dag.append(job['name'])

        else:
            # Create dict containing HTCondor job
            (_, _, cjob) = self._create_htcondor_job(username,
                                                     groups,
                                                     email,
                                                     str(uuid.uuid4()),
                                                     job,
                                                     '%s/%s' % (job_sandbox, job['name']),
                                                     True)

            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)
            cjob['+ProminenceFactoryId'] = '$(prominencecount)'

            exec_copy_dirs = []

            if job_factory['type'] == 'zip':
                cjob['extra_args'] = output_params(jwf) + ' '
                for index in range(len(job_factory['parameters'])):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (job_factory['parameters'][index]['name'], index)

                for index in range(len(job_factory['parameters'][0]['values'])):
                    parameters = []
                    count = 0
                    dir_name = create_dir_structure(job['name'], index, len(job_factory['parameters'][0]['values']))
                    if dir_name not in exec_copy_dirs:
                        exec_copy_dirs.append(dir_name)

                    for parameter in job_factory['parameters']:
                        parameters.append('prominencevalue%d="%s"' % (count,
                                                                      write_parameter_value(parameter['values'][index])))
                        count += 1
                    dag.append('JOB %s%d job.jdl DIR %s' % (job['name'], index, dir_name))
                    dag.append('VARS %s%d %s prominencecount="%d" %s' % (job['name'], index,
                                                                          ' '.join(parameters),
                                                                          index,
                                                                          self._output_urls(jwf, uid, index)))
                    jobs_in_dag.append('%s%d' % (job['name'], index))
            elif job_factory['type'] == 'parameterSweep':
                num_dimensions = len(job_factory['parameters'])

                ps_num = []
                ps_name = []
                ps_start = []
                ps_end = []
                ps_step = []

                for i in range(num_dimensions):
                    ps_name.append(job_factory['parameters'][i]['name'])
                    ps_start.append(float(job_factory['parameters'][i]['start']))
                    ps_end.append(float(job_factory['parameters'][i]['end']))
                    ps_step.append(float(job_factory['parameters'][i]['step']))

                    # Determine the number of values for each parameter
                    value = ps_start[i]
                    count = 0
                    while value <= ps_end[i]:
                        value += ps_step[i]
                        count += 1
                    ps_num.append(count)

                # Generate extra_args
                cjob['extra_args'] = output_params(jwf) + ' '
                for i in range(num_dimensions):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (ps_name[i], i)

                if num_dimensions == 1:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        x1_val = ps_start[0] + x1*ps_step[0]
                        dir_name = create_dir_structure(job['name'], job_count, ps_num[0])
                        if dir_name not in exec_copy_dirs:
                            exec_copy_dirs.append(dir_name)
                        dag.append('JOB %s%d job.jdl DIR %s' % (job['name'], job_count, dir_name))
                        dag.append('VARS %s%d prominencevalue0="%s" prominencecount="%d" %s' % (job['name'], job_count, write_parameter_value(x1_val), job_count, self._output_urls(jwf, uid, job_count)))
                        jobs_in_dag.append('%s%d' % (job['name'], job_count))
                        job_count += 1

                elif num_dimensions == 2:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            x1_val = ps_start[0] + x1*ps_step[0]
                            y1_val = ps_start[1] + y1*ps_step[1]
                            dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1])
                            if dir_name not in exec_copy_dirs:
                                exec_copy_dirs.append(dir_name)
                            dag.append('JOB %s%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                            dag.append('VARS %s%d prominencevalue0="%s" prominencevalue1="%s" prominencecount="%d" %s' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), job_count, self._output_urls(jwf, uid, job_count)))
                            jobs_in_dag.append('%s%d' % (job['name'], job_count))
                            job_count += 1

                elif num_dimensions == 3:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            for z1 in range(ps_num[2]):
                                x1_val = ps_start[0] + x1*ps_step[0]
                                y1_val = ps_start[1] + y1*ps_step[1]
                                z1_val = ps_start[2] + z1*ps_step[2]
                                dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1]*ps_num[2])
                                if dir_name not in exec_copy_dirs:
                                    exec_copy_dirs.append(dir_name)
                                dag.append('JOB %s%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                                dag.append('VARS %s%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencecount="%d" %s' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), job_count, self._output_urls(jwf, uid, job_count)))
                                jobs_in_dag.append('%s%d' % (job['name'], job_count))
                                job_count += 1

                elif num_dimensions == 4:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            for z1 in range(ps_num[2]):
                                for t1 in range(ps_num[3]):
                                    x1_val = ps_start[0] + x1*ps_step[0]
                                    y1_val = ps_start[1] + y1*ps_step[1]
                                    z1_val = ps_start[2] + z1*ps_step[2]
                                    t1_val = ps_start[3] + t1*ps_step[3]
                                    dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1]*ps_num[2]*ps_num[3])
                                    if dir_name not in exec_copy_dirs:
                                        exec_copy_dirs.append(dir_name)
                                    dag.append('JOB %s%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                                    dag.append('VARS %s%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencevalue3="%s" prominencecount="%d" %s' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), write_parameter_value(t1_val), job_count, self._output_urls(jwf, uid, job_count)))
                                    jobs_in_dag.append('%s%d' % (job['name'], job_count))
                                    job_count += 1

                elif num_dimensions > 4:
                    return (1, {"error": "Currently only parameter sweeps up to 4D are supported"})

            if not write_htcondor_job(cjob, '%s/%s/job.jdl' % (job_sandbox, job['name'])):
                return (1, {"error":"Unable to write JDL for job"})

            for to_dir in exec_copy_dirs:
                if to_dir != job['name']:
                    os.mkdir('%s/%s' % (job_sandbox, to_dir))
                    shutil.copyfile('%s/%s/job.jdl' % (job_sandbox, job['name']), '%s/%s/job.jdl' % (job_sandbox, to_dir))
                    shutil.copyfile(self._promlet_file, '%s/%s/promlet.py' % (job_sandbox, to_dir))

    # Define dependencies if necessary
    if 'dependencies' in jwf:
        # Generate full list of parents, taking into account job factories
        parents = {}
        for parent in jwf['dependencies']:
            for item in jobs_in_dag:
                if parent in item:
                    parents[item] = parent

        for parent in parents:
            # Generate full list of children, taking into account job factories
            children = []
            for child in jwf['dependencies'][parents[parent]]:
                for item in jobs_in_dag:
                    if child in item:
                        children.append(item)

            # Create relationship
            dag.append('PARENT %s CHILD %s' % (parent,
                                               " ".join(children)))

    # DAGMan status file
    dag.append('NODE_STATUS_FILE workflow.dag.status')

    # Dot file
    dag.append('DOT dag.dot')

    # Write DAGMan definition file
    try:
        with open(job_sandbox + '/job.dag', 'w') as fd:
            fd.write('\n'.join(dag))
    except IOError:
        logger.critical('Unable to write DAG file for job for user %s and job uid %s', username, uid)
        return (1, {"error":"Unable to write DAG file for job"})

    # Handle labels
    dag_appends = []
    if 'labels' in jwf:
        for label in jwf['labels']:
            value = jwf['labels'][label]
            dag_appends.append("'+ProminenceUserMetadata_%s=\"%s\"'" % (label, value))

    # Create command to submit to DAGMan
    dag_appends.append("'+ProminenceType=\"workflow\"'")
    dag_appends.append("'+ProminenceIdentity=\"%s\"'" % username)
    dag_appends.append("'+ProminenceGroup=\"%s\"'" % groups)
    dag_appends.append("'+ProminenceJobUniqueIdentifier=\"%s\"'" % uid)

    if email:
        dag_appends.append("'+ProminenceEmail=\"%s\"'" % email)

    cmd = "condor_submit_dag -maxidle %d -batch-name %s " % (int(self._config['WORKFLOW_MAX_IDLE']), wf_name)
    for dag_append in dag_appends:
        cmd += " -append %s " % dag_append
    cmd += " job.dag "

    # Submit to DAGMan
    (_, stdout, _, _) = run(cmd, job_sandbox, 30)
    match = re.search(r'submitted to cluster\s(\d+)', str(stdout))
    data = {}
    if match:
        retval = 201
        data['id'] = int(match.group(1))
    else:
        retval = 1
        data = {"error":"Workflow submission failed"}

    return (retval, data)

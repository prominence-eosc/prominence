import json
import os
import re
import shutil
import uuid

from .utilities import condor_str, run
from .write_htcondor_job import write_htcondor_job

def write_parameter_value(value):
    """
    Write a parameter value, taking into account its type
    """
    if isinstance(value, int):
        return '%d' % value
    elif isinstance(value, float):
        return '%f' % value
    elif isinstance(value, basestring):
        return '%s' % value

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

def create_workflow(self, username, groups, email, uid, jjob):
    """
    Create a workflow
    """
    # Firstly, create the workflow sandbox
    job_sandbox = self.create_sandbox(uid)
    if job_sandbox is None:
        return (1, {"error":"Unable to create workflow sandbox"})

    # Workflow name
    wf_name = ''
    if 'name' in jjob:
        wf_name = str(jjob['name'])

    # Write the workflow JSON description to disk
    try:
        with open(job_sandbox + '/workflow.json', 'w') as fd:
            json.dump(jjob, fd)
    except IOError:
        return (1, {"error":"Unable to write workflow.json"})

    dag = []

    # Policies
    job_placement_policies = None
    if 'policies' in jjob:
        # Job retries
        if 'maximumRetries' in jjob['policies']:
            dag.append('RETRY ALL_NODES %d' % jjob['policies']['maximumRetries'])

        # If placement policies are defined in the workflow, apply these to all jobs
        if 'placement' in jjob['policies']:
            job_placement_policies = jjob['policies']['placement']

    if 'dependencies' in jjob or 'factory' not in jjob:
        # Handle DAG workflows & bags of jobs
        for job in jjob['jobs']:
            # All jobs must have names
            if 'name' not in job:
                return (1, {"error":"All jobs in a workflow must have names"})

            # Add placement policies if defined
            if job_placement_policies:
                if 'policies' not in job:
                    job['policies'] = {}
                    job['policies']['placement'] = job_placement_policies
                elif 'placement' not in job['policies']:
                    job['policies']['placement'] = job_placement_policies

            # Create job sandbox
            try:
                os.makedirs(job_sandbox + '/' + job['name'])
                os.makedirs(job_sandbox + '/' + job['name'] + '/input')
            except IOError:
                return (1, {"error":"Unable to create job sandbox directories"})

            job_filename = job_sandbox + '/' + job['name'] + '/job.jdl'

            # Create dict containing HTCondor job
            (status, msg, cjob) = self._create_htcondor_job(username, groups, email, str(uuid.uuid4()), job, job_sandbox + '/' + job['name'])
            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)

            # Write JDL
            if not write_htcondor_job(cjob, job_filename):
                return (1, {"error":"Unable to write JDL for job"})

            # Append job to DAG description
            dag.append('JOB ' + job['name'] + ' job.jdl DIR ' + job['name'])
            dag.append('VARS ' + job['name'] + ' prominencecount="0"')

            # Copy executable to job sandbox
            shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, job['name'], 'promlet.py'))
            os.chmod(job_sandbox + '/' + job['name'] + '/promlet.py', 0o775)

        # Define dependencies if necessary
        if 'dependencies' in jjob:
            for parent in jjob['dependencies']:
                children = " ".join(jjob['dependencies'][parent])
                dag.append('PARENT ' + parent + ' CHILD ' + children)

    elif 'factory' in jjob:
        # Copy executable to job sandbox
        shutil.copyfile(self._promlet_file, os.path.join(job_sandbox, 'promlet.py'))
        os.chmod(job_sandbox + '/promlet.py', 0o775)

        # Create dict containing HTCondor job
        (status, msg, cjob) = self._create_htcondor_job(username, groups, email, str(uuid.uuid4()), jjob['jobs'][0], job_sandbox, True)
        cjob['+ProminenceWorkflowName'] = condor_str(wf_name)
        cjob['+ProminenceFactoryId'] = '$(prominencecount)'

        if jjob['factory']['type'] == 'parametricSweep':
            num_dimensions = len(jjob['factory']['parameters'])

            if num_dimensions == 1:
                ps_name = jjob['factory']['parameters'][0]['name']
                ps_start = float(jjob['factory']['parameters'][0]['start'])
                ps_end = float(jjob['factory']['parameters'][0]['end'])
                ps_step = float(jjob['factory']['parameters'][0]['step'])

                cjob['extra_args'] = '--param %s=$(prominencevalue0) %s' % (ps_name, output_params(jjob))

                value = ps_start
                job_count = 0
                while value <= ps_end:
                    dag.append('JOB job%d job.jdl' % job_count)
                    dag.append('VARS job%d prominencevalue0="%s" prominencecount="%d" %s' % (job_count,
                                                                                             write_parameter_value(value),
                                                                                             job_count,
                                                                                             self._output_urls(jjob, uid, job_count)))
                    value += ps_step
                    job_count += 1

            else:
                ps_num = []
                ps_name = []
                ps_start = []
                ps_end = []
                ps_step = []

                for i in range(num_dimensions):
                    ps_name.append(jjob['factory']['parameters'][i]['name'])
                    ps_start.append(float(jjob['factory']['parameters'][i]['start']))
                    ps_end.append(float(jjob['factory']['parameters'][i]['end']))
                    ps_step.append(float(jjob['factory']['parameters'][i]['step']))

                    # Determine the number of values for each parameter
                    value = ps_start[i]
                    count = 0
                    while value <= ps_end[i]:
                        value += ps_step[i]
                        count += 1
                    ps_num.append(count)

                # Generate extra_args
                cjob['extra_args'] = output_params(jjob) + ' '
                for i in range(num_dimensions):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (ps_name[i], i)

                # TODO: need to work out how to have n nested for loops, for arbitrary n

                if num_dimensions == 2:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            x1_val = ps_start[0] + x1*ps_step[0]
                            y1_val = ps_start[1] + y1*ps_step[1]
                            dag.append('JOB job%d job.jdl' % job_count)
                            dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencecount="%d" %s' % (job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), job_count, self._output_urls(jjob, uid, job_count)))
                            job_count += 1

                elif num_dimensions == 3:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            for z1 in range(ps_num[2]):
                                x1_val = ps_start[0] + x1*ps_step[0]
                                y1_val = ps_start[1] + y1*ps_step[1]
                                z1_val = ps_start[2] + z1*ps_step[2]
                                dag.append('JOB job%d job.jdl' % job_count)
                                dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencecount="%d" %s' % (job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), job_count, self._output_urls(jjob, uid, job_count)))
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
                                    dag.append('JOB job%d job.jdl' % job_count)
                                    dag.append('VARS job%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencevalue3="%s" prominencecount="%d" %s' % (job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), write_parameter_value(t1_val), job_count, self._output_urls(jjob, uid, job_count)))
                                    job_count += 1

                elif num_dimensions > 4:
                    return (1, {"error": "Currently only parameter sweeps up to 4D are supported"})

        elif jjob['factory']['type'] == 'zip':

            cjob['extra_args'] = output_params(jjob) + ' '
            for index in range(len(jjob['factory']['parameters'])):
                cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (jjob['factory']['parameters'][index]['name'], index)
            for index in range(len(jjob['factory']['parameters'][0]['values'])):
                parameters = []
                count = 0
                for parameter in jjob['factory']['parameters']:
                    parameters.append('prominencevalue%d="%s"' % (count, write_parameter_value(parameter['values'][index])))
                    count += 1
                dag.append('JOB job%d job.jdl' % index)
                dag.append('VARS job%d %s prominencecount="%d" %s' % (index,
                                                                      ' '.join(parameters),
                                                                      index,
                                                                      self._output_urls(jjob, uid, index)))

        # Write JDL
        if not write_htcondor_job(cjob, '%s/job.jdl' % job_sandbox):
            return (1, {"error":"Unable to write JDL for job"})

    # DAGMan status file
    dag.append('NODE_STATUS_FILE workflow.dag.status')

    # Write DAGMan definition file
    try:
        with open(job_sandbox + '/job.dag', 'w') as fd:
            fd.write('\n'.join(dag))
    except IOError:
        return (1, {"error":"Unable to write DAG file for job"})

    # Handle labels
    dag_appends = []
    if 'labels' in jjob:
        for label in jjob['labels']:
            value = jjob['labels'][label]
            dag_appends.append("'+ProminenceUserMetadata_%s=\"%s\"'" % (label, value))

    # Create command to submit to DAGMan
    dag_appends.append("'+ProminenceType=\"workflow\"'")
    dag_appends.append("'+ProminenceIdentity=\"%s\"'" % username)
    dag_appends.append("'+ProminenceJobUniqueIdentifier=\"%s\"'" % uid)

    if email:
        dag_appends.append("'+ProminenceEmail=\"%s\"'" % email)

    cmd = "condor_submit_dag -maxidle %d -batch-name %s " % (int(self._config['WORKFLOW_MAX_IDLE']), wf_name)
    for dag_append in dag_appends:
        cmd += " -append %s " % dag_append
    cmd += " job.dag "

    # Submit to DAGMan
    (return_code, stdout, stderr, timedout) = run(cmd, job_sandbox, 30)
    m = re.search(r'submitted to cluster\s(\d+)', str(stdout))
    data = {}
    if m:
        retval = 201
        data['id'] = int(m.group(1))
    else:
        retval = 1
        data = {"error":"Workflow submission failed"}

    return (retval, data)

import json
import logging
import os
import re
import shutil
from string import Template
from urllib.parse import unquote
import uuid

from .utilities import condor_str, run, validate_presigned_url
from .write_htcondor_job import write_htcondor_job

# Get an instance of a logger
logger = logging.getLogger(__name__)

def create_dir_structure(base_dir, num, total):
    """
    """
    return base_dir

def write_parameter_value(value):
    """
    Write a parameter value, taking into account its type
    """
    output = None
    try:
        if isinstance(value, int) or int(value) == value:
            output = '%d' % value
        elif isinstance(value, float):
            output = str(value)
        elif isinstance(value, basestring):
            output = value
    except:
        output = value
    return output

def _create_mapped_json(self, path, job_index, mapping, job_name):
    """
    Create mapped JSON file
    """
    try:
        with open('%s/.job.mapped.json' % path) as fh:
            job_json = json.load(fh)
    except:
        return None, None
    new_job_json = job_json.copy()

    # Update artifact URLs
    if 'artifacts' in job_json:
        new_artifacts = []
        for artifact in job_json['artifacts']:
            if 'url' in artifact:
                # Exctract object name from presigned URL
                name = unquote(artifact['url'].split(self._config['S3_BUCKET'])[1].split('?AWSAccessKeyId')[0][1:])

                # Apply template
                for key in mapping:
                    value = mapping[key]
                    name = Template(name).safe_substitute({key:value})

                # Create new presigned URL
                new_url = self.create_presigned_url('get',
                                                    self._config['S3_BUCKET'],
                                                    name,
                                                    864000)

                # Validate
                url_exists = validate_presigned_url(new_url)
                if not url_exists:
                    pieces = name.split('/')
                    artifact_name = pieces[len(pieces)-1]
                    return False, {"error":"Artifact %s does not exist" % artifact_name}

                new_artifacts.append({'url': new_url})
        new_job_json['artifacts'] = new_artifacts

    # Update output file URLs
    if 'outputFiles' in job_json:
        new_outputs = []
        for output in job_json['outputFiles']:
            if 'url' in output:
                # Exctract object name from presigned URL
                name = unquote(output['url'].split(self._config['S3_BUCKET'])[1].split('?AWSAccessKeyId')[0][1:])
                name_pieces = name.split(job_name)
                name = '%s%s/%d%s' % (name_pieces[0], job_name, job_index, name_pieces[1])

                # Apply template
                for key in mapping:
                    value = mapping[key]
                    name = Template(name).safe_substitute({key:value})

                # Create new presigned URL
                new_url = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    name,
                                                    864000)
                new_outputs.append({'url': new_url, 'name': output['name']})
        new_job_json['outputFiles'] = new_outputs

    # Update output file URLs
    if 'outputDirs' in job_json:
        new_outputs = []
        for output in job_json['outputDirs']:
            if 'url' in output:
                # Exctract object name from presigned URL
                name = unquote(output['url'].split(self._config['S3_BUCKET'])[1].split('?AWSAccessKeyId')[0][1:])
                name_pieces = name.split(job_name)
                name = '%s%s/%d%s' % (name_pieces[0], job_name, job_index, name_pieces[1])

                # Apply template
                for key in mapping:
                    value = mapping[key]
                    name = Template(name).safe_substitute({key:value})

                # Create new presigned URL
                new_url = self.create_presigned_url('put',
                                                    self._config['S3_BUCKET'],
                                                    name,
                                                    864000)
                new_outputs.append({'url': new_url, 'name': output['name']})
        new_job_json['outputDirs'] = new_outputs

    # Write new mapped JSON file
    try:
        with open('%s/.job.mapped.%d.json' % (path, job_index), 'w') as fh:
            json.dump(new_job_json, fh)
    except:
        return None, None

    return True, None

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
            (cjs, msg, cjob) = self._create_htcondor_job(username,
                                                         groups,
                                                         email,
                                                         str(uuid.uuid4()),
                                                         job,
                                                         '%s/%s' % (job_sandbox, job['name']),
                                                         True,
                                                         False,
                                                         uid,
                                                         job['name'])

            if cjs != 0:
                return (1, msg)

            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)

            # Write JDL
            if not write_htcondor_job(cjob, job_filename):
                return (1, {"error":"Unable to write JDL for job"})

            # Append job to DAG description
            dag.append('JOB %s job.jdl DIR %s' % ( job['name'], job['name']))
            dag.append('VARS %s prominencecount="0" mappedjson=".job.mapped.json"' % job['name'])
            jobs_in_dag.append(job['name'])

        else:
            # Create dict containing HTCondor job
            (cjs, msg, cjob) = self._create_htcondor_job(username,
                                                         groups,
                                                         email,
                                                         str(uuid.uuid4()),
                                                         job,
                                                         '%s/%s' % (job_sandbox, job['name']),
                                                         True,
                                                         True,
                                                         uid,
                                                         job['name'])

            if cjs != 0:
                return (1, msg)

            cjob['+ProminenceWorkflowName'] = condor_str(wf_name)
            cjob['+ProminenceFactoryId'] = '$(prominencecount)'

            exec_copy_dirs = []

            mappings_maps = []
            mappings_indexes = []

            if job_factory['type'] == 'repeat':
                cjob['extra_args'] = ''

                for index in range(job_factory['number']):
                    dir_name = create_dir_structure(job['name'], index, job_factory['number'])
                    if dir_name not in exec_copy_dirs:
                        exec_copy_dirs.append(dir_name)

                    dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], index, dir_name))
                    dag.append('VARS %s_%d prominencecount="%d" mappedjson=".job.mapped.json"' % (job['name'], index, index))
                    jobs_in_dag.append('%s_%d' % (job['name'], index))

            elif job_factory['type'] == 'zip':
                cjob['extra_args'] = ''
                for index in range(len(job_factory['parameters'])):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (job_factory['parameters'][index]['name'], index)

                for index in range(len(job_factory['parameters'][0]['values'])):
                    parameters = []
                    count = 0
                    dir_name = create_dir_structure(job['name'], index, len(job_factory['parameters'][0]['values']))
                    if dir_name not in exec_copy_dirs:
                        exec_copy_dirs.append(dir_name)

                    mapping = {}
                    for parameter in job_factory['parameters']:
                        parameters.append('prominencevalue%d="%s"' % (count,
                                                                      write_parameter_value(parameter['values'][index])))
                        mapping[parameter['name']] = write_parameter_value(parameter['values'][index])
                        count += 1
                    dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], index, dir_name))
                    dag.append('VARS %s_%d %s prominencecount="%d" mappedjson="%s"' % (job['name'], index,
                                                                                       ' '.join(parameters),
                                                                                       index,
                                                                                       '.job.mapped.%d.json' % index))
                    jobs_in_dag.append('%s_%d' % (job['name'], index))
                    mappings_maps.append(mapping)
                    mappings_indexes.append(index)
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
                cjob['extra_args'] = ''
                for i in range(num_dimensions):
                    cjob['extra_args'] += '--param %s=$(prominencevalue%d) ' % (ps_name[i], i)

                if num_dimensions == 1:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        mapping = {}
                        x1_val = ps_start[0] + x1*ps_step[0]
                        dir_name = create_dir_structure(job['name'], job_count, ps_num[0])
                        if dir_name not in exec_copy_dirs:
                            exec_copy_dirs.append(dir_name)
                        dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], job_count, dir_name))
                        dag.append('VARS %s_%d prominencevalue0="%s" prominencecount="%d" mappedjson="%s"' % (job['name'], job_count, write_parameter_value(x1_val), job_count, '.job.mapped.%d.json' % job_count))
                        jobs_in_dag.append('%s_%d' % (job['name'], job_count))
                        mapping[ps_name[0]] = write_parameter_value(x1_val)
                        mappings_maps.append(mapping)
                        mappings_indexes.append(job_count)
                        job_count += 1

                elif num_dimensions == 2:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            mapping = {}
                            x1_val = ps_start[0] + x1*ps_step[0]
                            y1_val = ps_start[1] + y1*ps_step[1]
                            dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1])
                            if dir_name not in exec_copy_dirs:
                                exec_copy_dirs.append(dir_name)
                            dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                            dag.append('VARS %s_%d prominencevalue0="%s" prominencevalue1="%s" prominencecount="%d" mappedjson="%s"' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), job_count, '.job.mapped.%d.json' % job_count))
                            jobs_in_dag.append('%s_%d' % (job['name'], job_count))
                            mapping[ps_name[0]] = write_parameter_value(x1_val)
                            mapping[ps_name[1]] = write_parameter_value(y1_val)
                            mappings_maps.append(mapping)
                            mappings_indexes.append(job_count)
                            job_count += 1

                elif num_dimensions == 3:
                    mapping = {}
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            for z1 in range(ps_num[2]):
                                mapping = {}
                                x1_val = ps_start[0] + x1*ps_step[0]
                                y1_val = ps_start[1] + y1*ps_step[1]
                                z1_val = ps_start[2] + z1*ps_step[2]
                                dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1]*ps_num[2])
                                if dir_name not in exec_copy_dirs:
                                    exec_copy_dirs.append(dir_name)
                                dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                                dag.append('VARS %s_%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencecount="%d" mappedjson="%s"' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), job_count, '.job.mapped.%d.json' % job_count))
                                jobs_in_dag.append('%s_%d' % (job['name'], job_count))
                                mapping[ps_name[0]] = write_parameter_value(x1_val)
                                mapping[ps_name[1]] = write_parameter_value(y1_val)
                                mapping[ps_name[2]] = write_parameter_value(z1_val)
                                mappings_maps.append(mapping)
                                mappings_indexes.append(job_count)
                                job_count += 1

                elif num_dimensions == 4:
                    job_count = 0
                    for x1 in range(ps_num[0]):
                        for y1 in range(ps_num[1]):
                            for z1 in range(ps_num[2]):
                                for t1 in range(ps_num[3]):
                                    mapping = {}
                                    x1_val = ps_start[0] + x1*ps_step[0]
                                    y1_val = ps_start[1] + y1*ps_step[1]
                                    z1_val = ps_start[2] + z1*ps_step[2]
                                    t1_val = ps_start[3] + t1*ps_step[3]
                                    dir_name = create_dir_structure(job['name'], job_count, ps_num[0]*ps_num[1]*ps_num[2]*ps_num[3])
                                    if dir_name not in exec_copy_dirs:
                                        exec_copy_dirs.append(dir_name)
                                    dag.append('JOB %s_%d job.jdl DIR %s' % (job['name'], job_count, job['name']))
                                    dag.append('VARS %s_%d prominencevalue0="%s" prominencevalue1="%s" prominencevalue2="%s" prominencevalue3="%s" prominencecount="%d" mappedjson="%s"' % (job['name'], job_count, write_parameter_value(x1_val), write_parameter_value(y1_val), write_parameter_value(z1_val), write_parameter_value(t1_val), job_count, '.job.mapped.%d.json' % job_count))
                                    jobs_in_dag.append('%s_%d' % (job['name'], job_count))
                                    mapping[ps_name[0]] = write_parameter_value(x1_val)
                                    mapping[ps_name[1]] = write_parameter_value(y1_val)
                                    mapping[ps_name[2]] = write_parameter_value(z1_val)
                                    mapping[ps_name[3]] = write_parameter_value(t1_val)
                                    mappings_maps.append(mapping)
                                    mappings_indexes.append(job_count)
                                    job_count += 1

                elif num_dimensions > 4:
                    return (1, {"error": "Currently only parameter sweeps up to 4D are supported"})

            if not write_htcondor_job(cjob, '%s/%s/job.jdl' % (job_sandbox, job['name'])):
                return (1, {"error":"Unable to write JDL for job"})

            for map_count in range(0, len(mappings_maps)):
                status, msg = self._create_mapped_json('%s/%s' % (job_sandbox, job['name']),
                                                       mappings_indexes[map_count],
                                                       mappings_maps[map_count],
                                                       job['name'])
                if not status and msg:
                    return (1, msg)

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
    dag_appends.append("'+ProminenceAPI=1.0'")

    # Should the workflow be removed from the queue once finished?
    prfq = '+ProminenceRemoveFromQueue=True'
    if 'policies' in jwf:
        if 'leaveInQueue' in jwf['policies']:
            if jwf['policies']['leaveInQueue']:
                prfq = '+ProminenceRemoveFromQueue=False'
    dag_appends.append("'%s'" % prfq)
    dag_appends.append("'leave_in_queue = (JobStatus == 4 || JobStatus == 3) && ProminenceRemoveFromQueue =?= False'")

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

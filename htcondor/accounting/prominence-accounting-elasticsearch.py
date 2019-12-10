#!/usr/bin/python
from __future__ import print_function
import ConfigParser
import json
import os
import sys
import time
import classad
from elasticsearch import Elasticsearch

def datetime_format(epoch):
    """
    Convert a unix epoch in a formatted date/time string
    """
    datetime_fmt = '%Y-%m-%dT%H:%M:%SZ'
    return time.strftime(datetime_fmt, time.gmtime(epoch))

def process_record(filename):
    """
    Process the specified file containing a ClassAd
    """

    start_date = None
    submit_date = None
    routed_by = None
    group = ''
    infra_id = None
    infra_site = None
    infra_type = None
    exit_code = None
    workload_type = None
    iwd = None
    factory_id = 0

    with open(filename, 'r') as fd:
        ad = classad.parseOne(fd, parser=classad.Parser.Old)

    if 'ProminenceGroup' in ad:
        group = ad['ProminenceGroup']
    if 'ProminenceType' in ad:
        workload_type = ad['ProminenceType']
    if 'RoutedBy' in ad:
        routed_by = ad['RoutedBy']
    if 'ClusterId' in ad:
        cluster_id = int(ad['ClusterId'])
    if 'JobStatus' in ad:
        job_status = int(ad['JobStatus'])
    if 'ProminenceInfrastructureSite' in ad:
        infra_site = ad['ProminenceInfrastructureSite']
    if 'ProminenceInfrastructureType' in ad:
        infra_type = ad['ProminenceInfrastructureType']
    if 'ProminenceJobUniqueIdentifier' in ad:
        identifier = ad['ProminenceJobUniqueIdentifier']
    if 'ProminenceIdentity' in ad:
        identity = ad['ProminenceIdentity']
    if 'QDate' in ad:
        submit_date = int(ad['QDate'])
    if 'JobStartDate' in ad:
        start_date = int(ad['JobStartDate'])
    if 'CompletionDate' in ad:
        completion_date = int(ad['CompletionDate'])
    if 'Iwd' in ad:
        iwd = ad['Iwd']
    if 'ExitCode' in ad:
        exit_code = ad['ExitCode']
    if 'ProminenceInfrastructureId' in ad:
        infra_id = ad['ProminenceInfrastructureId']
    if 'ProminenceFactoryId' in ad:
        factory_id = int(ad['ProminenceFactoryId'])

    if routed_by == 'jobrouter':
        exit(0)

    # Is this a job or a workflow?
    is_job = False
    if workload_type == 'job':
        is_job = True

    # Original job or workflow json
    if is_job:
        workload_json_file = '%s/.job.json' % iwd
    else:
        workload_json_file = '%s/workflow.json' % iwd

    if not os.path.exists(workload_json_file):
        exit(1)

    with open(workload_json_file, 'r') as workload_json_fd:
        job = json.load(workload_json_fd)

    if is_job:
        # Remove content of input files from job as we don't need it
        if 'inputs' in job:
            for input_file in job['inputs']:
                if 'content' in input_file:
                    input_file['content'] = ''

        # Job execution
        job['execution'] = {}
        if infra_site:
            job['execution']['site'] = infra_site
        if infra_type:
            job['execution']['type'] = infra_type
        if infra_id:
            job['execution']['id'] = infra_id

        # Promlet job stats
        promlet_json_file = '%s/promlet.%d.json' % (iwd, factory_id)
        if os.path.exists(promlet_json_file):
            with open(promlet_json_file, 'r') as promlet_json_fd:
                promlet_json = json.load(promlet_json_fd)

            tasks = []
            for task in promlet_json['tasks']:
                if 'maxMemoryUsageKB' in task:
                    job['execution']['maxMemoryUsageKB'] = task['maxMemoryUsageKB']
                else:
                    tasks.append(task)

            job['execution']['tasks'] = tasks
    else:
        # Remove content of input files from job as we don't need it
        if 'jobs' in job:
            for myjob in job['jobs']:
                if 'inputs' in myjob:
                    for input_file in myjob['inputs']:
                        if 'content' in input_file:
                            input_file['content'] = ''

    job['id'] = cluster_id
    job['uid'] = identifier
    job['username'] = identity
    job['group'] = group
    job['type'] = workload_type
    job['date'] = datetime_format(submit_date)

    job['events'] = {}
    job['events']['createTime'] = submit_date

    job['htcondor'] = {}
    job['htcondor']['JobStatus'] = job_status
    if exit_code:
        job['htcondor']['ExitCode'] = exit_code
    if start_date:
        job['htcondor']['JobStartDate'] = start_date
    if completion_date:
        job['htcondor']['CompletionDate'] = completion_date

    return job

if __name__ == "__main__":
    # Read config file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read('/opt/prominence/etc/prominence.ini')

    # Create record from job ClassAd
    job = process_record(sys.argv[1])

    # Send record to ElasticSearch
    es = Elasticsearch([{'host':CONFIG.get('elasticsearch', 'host'),
                         'port':CONFIG.get('elasticsearch', 'port')}])
    result = es.index(index=CONFIG.get('elasticsearch', 'index'),
                      doc_type='job',
                      id=job['id'],
                      body=job)
    print(result)

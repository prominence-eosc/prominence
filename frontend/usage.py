"""Get resource usage data from ElasticSearch"""
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

def get_usage(username,
              group,
              start_date,
              end_date,
              show_users,
              show_all_users,
              show_groups,
              config):
    """
    Get resource usage data from ElasticSearch
    """

    client = Elasticsearch([{'host':config['ELASTICSEARCH_HOST'],
                             'port':config['ELASTICSEARCH_PORT']}])

    if show_users and not show_all_users and not show_groups:
        query = Q('match', group__keyword=group) & Q('match', username=username)
    else:
        query = Q('match', group__keyword=group)

    search = Search(using=client, index=config['ELASTICSEARCH_INDEX']) \
             .filter('range', date={'gte':start_date, 'lte':end_date}) \
             .query(query) \
             .scan()

    wall_time = {}
    cpu_time = {}
    num_jobs = {}

    for hit in search:
        if hit.type == 'job':
            cpus = hit.resources['cpus']
            if 'tasks' in hit.execution:
                if hit.username not in wall_time:
                    wall_time[hit.username] = 0
                    cpu_time[hit.username] = 0
                    num_jobs[hit.username] = 0
                num_jobs[hit.username] += 1
                for task in hit.execution['tasks']:
                    if 'wallTimeUsage' in task:
                        wall_time[hit.username] += task['wallTimeUsage']*cpus
                    if 'cpuTimeUsage' in task:
                        cpu_time[hit.username] += task['cpuTimeUsage']

    data = {}
    data['usage'] = {}
    data['usage']['groups'] = {}
    data['usage']['users'] = {}

    if show_groups:
        data['usage']['groups'][group] = {}
        data['usage']['groups'][group]['cpuTime'] = sum(cpu_time.values())/3600.0
        data['usage']['groups'][group]['wallTime'] = sum(wall_time.values())/3600.0
        data['usage']['groups'][group]['numberOfJobs'] = sum(num_jobs.values())
    
    if show_users or show_all_users:
        for username_to_use in wall_time:
            if username_to_use == username or show_all_users:
                data['usage']['users'][username_to_use] = {}
                data['usage']['users'][username_to_use]['cpuTime'] = cpu_time[username_to_use]/3600.0
                data['usage']['users'][username_to_use]['wallTime'] = wall_time[username_to_use]/3600.0
                data['usage']['users'][username_to_use]['numberOfJobs'] = num_jobs[username_to_use]

    return data

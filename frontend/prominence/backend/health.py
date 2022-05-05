"""Return status of HTCondor daemons and other services"""
import socket
import requests
import htcondor

def get_health(self):
    errors = []

    # Check collector
    try:
        coll = htcondor.Collector(socket.gethostname())
        if not coll.query(htcondor.AdTypes.Collector, 'true', ['Name']):
            errors.append('HTCONDOR_COLLECTOR')
    except:
        errors.append('HTCONDOR_COLLECTOR')

    # Check schedd
    try:
        coll = htcondor.Collector(socket.gethostname())
        schedds = coll.query(htcondor.AdTypes.Schedd, 'true', ['Name'])
        if not schedds:
            errors.append('HTCONDOR_SCHEDD')
    except:
        errors.append('HTCONDOR_SCHEDD')

    # Check Elasticsearch
    try:
        resp = requests.get('http://%s:%s' % (self._config['ELASTICSEARCH_HOST'], self._config['ELASTICSEARCH_PORT']))
        if resp.status_code != 200:
            errors.append('ELASTICSEARCH_STATUS')
    except:
        errors.append('ELASTICSEARCH_EXCP')

    # Check etcd
    try:
        resp = requests.get('http://127.0.0.1:2379/health')
        if resp.status_code != 200:
            errors.append('ETCD_STATUS')
    except:
        errors.append('ETCD_EXCP')

    # Check InfluxDB
    try:
        resp = requests.get('%s/health' % self._config['INFLUXDB_URL'])
        if resp.status_code != 200:
            errors.append('INFLUXDB_STATUS')
    except:
        errors.append('INFLUXDB_EXCP')

    if errors:
        return (False, {'errors': ','.join(errors)})

    return (True, {})


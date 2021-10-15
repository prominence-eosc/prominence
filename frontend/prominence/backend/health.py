"""Return status of HTCondor daemons and other services"""
import requests
import socket
import htcondor

def get_health(self):
    # Check collector
    try:
        coll = htcondor.Collector(socket.gethostname())
        if not coll.query(htcondor.AdTypes.Collector, 'true', ['Name']):
            return (False, {'error': 'HTCONDOR_COLLECTOR'})
    except:
        return (False, {'error': 'HTCONDOR_COLLECTOR'})

    # Check schedd
    try:
        coll = htcondor.Collector(socket.gethostname())
        schedds = coll.query(htcondor.AdTypes.Schedd, 'true', ['Name'])
        if not schedds:
            return (False, {'error': 'HTCONDOR_SCHEDD'})
    except:
        return (False, {'error': 'HTCONDOR_SCHEDD'})

    # Check Elasticsearch
    try:
        resp = requests.get('http://%s:%s' % (self._config['ELASTICSEARCH_HOST'], self._config['ELASTICSEARCH_PORT']))
        if resp.status_code != 200:
            return (False, {'error': 'ELASTICSEARCH'})
    except:
        return (False, {'error': 'ELASTICSEARCH'})

    return (True, {})


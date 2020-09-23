import influxdb_client

class JobMetrics(object):
    def __init__(self, config):
        self.client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'],
                                                     token=config['INFLUXDB_TOKEN'],
                                                     org=config['INFLUXDB_ORG'])
        self.bucket = config['INFLUXDB_BUCKET']
        self.org = config['INFLUXDB_ORG']

    def get_jobs(self, identity, since):
        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str(since) + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_by_identity")'
                 '|> filter(fn:(r) => r.identity == "' + identity + '")')

        try:
            results = self.client.query_api().query(org=self.org, query=query)
        except:
            return {'idle': [], 'running': []}

        jobs_running = []
        jobs_idle = []

        for table in results:
            for row in table.records:
                raw_time = row.values["_time"]
                new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                if row.values["_field"] == "idle":
                    jobs_idle.append({'t': new_time, 'y': int(row.values["_value"])})
                if row.values["_field"] == "running":
                    jobs_running.append({'t': new_time, 'y': int(row.values["_value"])})

        return {'idle': jobs_idle, 'running': jobs_running}

class JobResourceUsageMetrics(object):
    def __init__(self, config):
        self.client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'],
                                                     token=config['INFLUXDB_TOKEN'],
                                                     org=config['INFLUXDB_ORG'])
        self.bucket = config['INFLUXDB_BUCKET']
        self.org = config['INFLUXDB_ORG']

    def get_job(self, job_id, since):
        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str(since) + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_resource_usage")'
                 '|> filter(fn:(r) => r.id == "' + str(job_id) + '")')

        try:
            results = self.client.query_api().query(org=self.org, query=query)
        except:
            return return {'memory': []}

        memory = []

        for table in results:
            for row in table.records:
                if row.values["_field"] == "memory":
                    raw_time = row.values["_time"]
                    new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    memory.append({'t': new_time, 'y': int(row.values["_value"])/1000})

        return {'memory': memory}

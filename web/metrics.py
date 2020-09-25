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

class JobMetricsByCloud(object):
    def __init__(self, config):
        self.client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'],
                                                     token=config['INFLUXDB_TOKEN'],
                                                     org=config['INFLUXDB_ORG'])
        self.bucket = config['INFLUXDB_BUCKET']
        self.org = config['INFLUXDB_ORG']

    def _create_dataset(self, identifier, label, data):
        colors = []
        colors.append([0, 0, 0])
        colors.append([255, 0, 0])
        colors.append([0, 255, 0])
        colors.append([0, 0, 255])

        return {'label': "%s" % label,
                'borderColor': "rgb(%d, %d, %d)" % (colors[identifier][0],
                                                    colors[identifier][1],
                                                    colors[identifier][2]),
                'backgroundColor': "rgb(%d, %d, %d)" % (colors[identifier][0],
                                                    colors[identifier][1],
                                                    colors[identifier][2]),
                'fill': 'false',
                'pointRadius': 0,
                'data': data}

    def get_jobs(self, identity, since):
        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str(since) + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_by_identity_by_site")'
                 '|> filter(fn:(r) => r.identity == "' + identity + '")')

        try:
            results = self.client.query_api().query(org=self.org, query=query)
        except:
            return []

        data = {}

        for table in results:
            for row in table.records:
                site = row.values['site']
                raw_time = row.values["_time"]
                new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")

                if site not in data:
                    data[site] = []
                data[site].append({'t': new_time, 'y': int(row.values["_value"])})

        counter = 0
        results = []
        for site in data:
            results.append(self._create_dataset(counter, site, data[site]))
            counter += 1

        return {'data': results}

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
            return {'memory': []}

        memory = []

        for table in results:
            for row in table.records:
                if row.values["_field"] == "memory":
                    raw_time = row.values["_time"]
                    new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    memory.append({'t': new_time, 'y': int(row.values["_value"])/1000})

        return {'memory': memory}

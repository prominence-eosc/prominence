import influxdb_client

class JobMetrics(object):
    def __init__(self, url, token, org, bucket):
        self.client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.org = org

    def get_jobs(self, identity, since):
        query_api = self.client.query_api()

        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -"' + since + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_by_identity")'
                 '|> filter(fn:(r) => r.identity == "' + identity + '")')

        results = self.client.query_api().query(org=self.org, query=query)

        jobs_running = []
        jobs_idle = []

        for table in results:
            for row in table.records:
                if row.values["_field"] == "idle":
                    jobs_idle.append({row.values["_time"], row.values["_value"]})
                if row.values["_field"] == "running":
                    jobs_running.append({row.values["_time"], row.values["_value"]})

        return jobs_idle, jobs_running

class JobResourceUsageMetrics(object):
    def __init__(self, url, token, org, bucket):
        self.client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.org = org

    def get_job(self, job_id, since):
        query_api = self.client.query_api()

        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str(since) + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_resource_usage")'
                 '|> filter(fn:(r) => r.id == "' + str(job_id) + '")')

        results = self.client.query_api().query(org=self.org, query=query)

        memory = []

        for table in results:
            for row in table.records:
                if row.values["_field"] == "memory":
                    raw_time = row.values["_time"]
                    new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    memory.append({'t': new_time, 'y': int(row.values["_value"])/1000})

        return {'memory': memory}

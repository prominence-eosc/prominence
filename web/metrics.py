import logging
import influxdb_client

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Metrics:
    """
    Base class for setting up configuration for accessing InfluxDB
    """
    def __init__(self, config):
        self.client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'],
                                                     token=config['INFLUXDB_TOKEN'],
                                                     org=config['INFLUXDB_ORG'])
        self.bucket = config['INFLUXDB_BUCKET']
        self.org = config['INFLUXDB_ORG']

class ResourceUsage(Metrics):
    """
    Get resource usage info
    """
    def get(self, identity, group, start_date, end_date, show_users, show_all_users, show_groups):
        if not show_all_users:
            query = (' from(bucket:"' + self.bucket + '")'
                    '|> range(start: -' + str(since) + 'm)'
                    '|> filter(fn:(r) => r._measurement == "accounting")')
        else:
            query = (' from(bucket:"' + self.bucket + '")'
                    '|> range(start: -' + str(since) + 'm)'
                    '|> filter(fn:(r) => r._measurement == "accounting")'
                    '|> filter(fn:(r) => r.identity == "' + identity + '")')

        return None

class JobMetrics(Metrics):
    """
    Get idle and running jobs for the specified user
    """
    def get_jobs(self, identity, since):
        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str("test") + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_by_identity")'
                 '|> filter(fn:(r) => r.identity == "' + identity + '")')

        try:
            results = self.client.query_api().query(org=self.org, query=query)
        except Exception as err:
            logger.critical('Exception when querying InfluxDB in JobMetrics: %s', err)
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

class JobMetricsByCloud(Metrics):
    """
    Get running jobs by resource for the specified user
    """
    def _create_dataset(self, identifier, label, data):
        # Colours taken from https://colorbrewer2.org
        colors = []
        colors.append([166,206,227])
        colors.append([31,120,180])
        colors.append([178,223,138])
        colors.append([51,160,44])
        colors.append([251,154,153])
        colors.append([227,26,28])
        colors.append([253,191,111])
        colors.append([255,127,0])
        colors.append([202,178,214])
        colors.append([106,61,154])
        colors.append([255,255,153])
        colors.append([177,89,40])

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
        except Exception as err:
            logger.critical('Exception when querying InfluxDB in JobMetricsByCloud: %s', err)
            return {}

        data = {}
        sites = []
        times = []

        for table in results:
            for row in table.records:
                site = row.values['site']
                raw_time = row.values["_time"]
                new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")

                if site not in data:
                    data[site] = []
                    sites.append(site)
                data[site].append({'t': new_time, 'y': int(row.values["_value"])})
                if new_time not in times:
                    times.append(new_time)

        times = sorted(times)

        # For chart.js stacked bar charts we need to ensure that every dataset has a value at the
        # same time
        for site in data:
            new = []
            for time in times:
                value = 0
                for myvalues in data[site]:
                    if time == myvalues['t']:
                        value = myvalues['y']
                pair = ({'t': time, 'y': value})
                new.append(pair)
            data[site] = new

        counter = 0
        results = []
        for site in data:
            results.append(self._create_dataset(counter, site, data[site]))
            counter += 1

        return {'data': results}

class JobResourceUsageMetrics(Metrics):
    """
    Get resource usage metrics for the specified job
    """
    def get_job(self, job_id, since):
        query = (' from(bucket:"' + self.bucket + '")'
                 '|> range(start: -' + str(since) + 'm)'
                 '|> filter(fn:(r) => r._measurement == "jobs_resource_usage")'
                 '|> filter(fn:(r) => r.id == "' + str(job_id) + '")')

        try:
            results = self.client.query_api().query(org=self.org, query=query)
        except Exception as err:
            logger.critical('Exception when querying InfluxDB in JobResourceUsageMetrics: %s', err)
            return {'memory': []}

        memory = []

        for table in results:
            for row in table.records:
                if row.values["_field"] == "memory":
                    raw_time = row.values["_time"]
                    new_time = raw_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    memory.append({'t': new_time, 'y': int(row.values["_value"])/1000})

        return {'memory': memory}

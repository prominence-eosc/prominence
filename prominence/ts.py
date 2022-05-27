"""Routes for the time-series database"""
import base64
import re
import sys
import time
from flask import Blueprint, jsonify, request
from flask import current_app as app

from .backend import ProminenceBackend
from .errors import func_disabled, no_such_job, not_auth_job
from .utilities import get_remote_addr
from .validate import validate_point

from .auth import requires_auth, requires_auth_ts

from influxdb_client import InfluxDBClient, WritePrecision, Point
from influxdb_client.client.write_api import SYNCHRONOUS

ts = Blueprint('ts', __name__)

@ts.route("/prominence/v1/ts/<int:job_id>", methods=['GET'])
@requires_auth
def get_points(username, group, email, job_id):
    """
    Get time series data
    """
    app.logger.info('%s GetPoints user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_TS'] != 'True':
        return func_disabled()

    backend = ProminenceBackend(app.config)
    (job_uuid, identity, _, _, _, _, _, qdate) = backend.get_job_unique_id(job_id, True)

    if not identity:
        return no_such_job()
    if username != identity:
        return not_auth_job()

    start = int((time.time() - qdate)/60) + 30
    if 'start' in request.args:
        start = int(request.args.get('start'))

    if 'stop' in request.args:
        stop = int(request.args.get('stop'))
        range_expr = 'range(start: -%dm, stop: -%dm)' % (start, stop)
    else:
        range_expr = 'range(start: -%dm)' % start

    flux_mean = ''
    if 'mean' in request.args:
        flux_mean = ' |> mean() '

    output = []
    try:
        client = InfluxDBClient(url=app.config['INFLUXDB_URL'],
                                token=app.config['INFLUXDB_TOKEN'],
                                org=app.config['INFLUXDB_ORG'])

        query_api = client.query_api()
        tables = query_api.query('from(bucket:"user") |> %s |> filter(fn: (r) => r["jobuid"] == "%s") %s' % (range_expr, job_uuid, flux_mean))

        have_tags = False
        tags = {}
        for table in tables:
            data = {}
            ts = []
            vals = []
            for record in table.records:
                if 'mean' not in request.args:
                    ts.append(record.values['_time'].strftime("%Y-%m-%d %H:%M:%S"))
                vals.append(record.values['_value'])
                if not have_tags:
                    have_tags = True
                    for item in record.values:
                        if item not in ('_time', '_start', '_stop', '_field', '_measurement', '_value', 'result', 'table', 'jobuid'):
                            tags[item] = record.values[item]

            if 'mean' not in request.args:
                data['times'] = ts
            data['values'] = vals
            data['measurement'] = record.values['_measurement']
            data['field'] = record.values['_field']
            data['tags'] = tags
            output.append(data)
    except:
        return jsonify({'error':'Unable to get time-series data'}), 400

    return jsonify(output)

@ts.route("/prominence/v1/ts", methods=['POST'])
@requires_auth_ts
def set_point(username, group, email, job_uuid):
    """
    Set time series data
    """
    app.logger.info('%s SetPoint user:%s group:%s' % (get_remote_addr(request), username, group))

    if app.config['ENABLE_TS'] != 'True':
        return func_disabled()

    # Validate the input JSON
    (status, msg) = validate_point(request.get_json())
    if not status:
        return jsonify({'error': msg}), 400

    try:
        client = InfluxDBClient(url=app.config['INFLUXDB_URL'],
                                token=app.config['INFLUXDB_TOKEN'],
                                org=app.config['INFLUXDB_ORG'])

        write_api = client.write_api(write_options=SYNCHRONOUS)

        dictionary = {}
        dictionary['name'] = request.get_json()['measurement']
        fields = []
        for field in request.get_json()['fields']:
            dictionary[field] = request.get_json()['fields'][field]
            fields.append(field)
        if 'time' in request.get_json():
            dictionary['time'] = request.get_json()['time']
        else:
            dictionary['time'] = int(time.time())
        tags = ['jobuid']
        dictionary['jobuid'] = job_uuid
        if 'tags' in request.get_json():
            for tag in request.get_json()['tags']:
                dictionary[tag] = request.get_json()['tags'][tag]
                tags.append(tag)

        point = Point.from_dict(dictionary,
                    write_precision=WritePrecision.S,
                    record_measurement_key="name",
                    record_time_key="time",
                    record_tag_keys=tags,
                    record_field_keys=fields)
           
        write_api.write(bucket=app.config['INFLUXDB_BUCKET'], record=point)
    except Exception as err:
        return jsonify({'error':'A JWT token is required: %s' % err}), 400

    return jsonify({}), 201

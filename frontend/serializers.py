from collections import OrderedDict
import json
import re
import os

from rest_framework import serializers
from frontend.models import Job, Workflow
from server.backend.utilities import datetime_format, elapsed, redact_storage_creds
from server.backend import ProminenceBackend
import server.settings

def get_workflow_json(obj):
    try:
        with open('%s/workflow.json' % obj.sandbox, 'r') as json_file:
            return json.load(json_file)
    except Exception:
        return None

def get_uuid(obj):
    uuid = obj.sandbox.replace(server.settings.CONFIG['SANDBOX_PATH'], '')
    match = re.search(r'/([\w\-]+).*', uuid)
    if match:
        return match.group(1)
    return None

def get_factory_id(obj):
    match = re.search(r'([\w\_\-]+)\/([\w\_\-]+)\/([\d]+)', obj.name)
    if match and obj.workflow:
        return int(match.group(3))    
    return None

def get_job_run_json(obj):
    if not obj.workflow:
        try:
            with open('%s/promlet.0.json' % obj.sandbox, 'r') as json_file:
                return json.load(json_file)
        except Exception:
            return None
    else:
        factory_id = 0
        match = re.search(r'([\w\_\-]+)\/([\w\_\-]+)\/([\d]+)', obj.name)
        if match:
            factory_id = int(match.group(3))
   
        try:
            with open('%s/promlet.%d.json' % (obj.sandbox, factory_id), 'r') as json_file:
                return json.load(json_file)
        except Exception as err:
            return None

def get_job_json(obj):
    if not obj.workflow:
        try:
            with open('%s/job.json' % obj.sandbox, 'r') as json_file:
                return json.load(json_file)
        except Exception:
            return None
    else:
        iwd = obj.sandbox.replace(server.settings.CONFIG['SANDBOX_PATH'], '')
        match = re.search(r'/([\w\-]+)/([\w\-\_]+)/\d\d', iwd)
        if match:
            iwd = '%s/%s/%s' % (server.settings.CONFIG['SANDBOX_PATH'], match.group(1), match.group(2))
        else:
            iwd = obj.sandbox

        try:
            with open('%s/job.json' % iwd, 'r') as json_file:
                return json.load(json_file)
        except Exception as err:
            return None

class WorkflowSerializer(serializers.ModelSerializer):
    events = serializers.SerializerMethodField()
    progress = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta: 
        model = Workflow
        fields = ['id', 'name', 'status', 'events', 'progress']

    def get_status(self, obj):
        return dict(Workflow.WORKFLOW_STATUSES)[obj.status]

    def get_events(self, obj):
        events = {}
        events['createTime'] = obj.created
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        return events

    def get_progress(self, obj):
        return {"total": obj.jobs_total, "done": obj.jobs_done, "failed": obj.jobs_failed}

class WorkflowDisplaySerializer(WorkflowSerializer):
    elapsedTime = serializers.SerializerMethodField()

    class Meta(WorkflowSerializer.Meta):
        fields = WorkflowSerializer.Meta.fields + ['elapsedTime']

    def get_progress(self, obj):
        data = {"total": obj.jobs_total,
                "done": obj.jobs_done,
                "failed": obj.jobs_failed}

        if obj.jobs_total > 0:
            data["donePercentage"] = int(100.0*obj.jobs_done/obj.jobs_total)
            data["failedPercentage"] = int(100.0*obj.jobs_failed/obj.jobs_total)
        else:
            data["donePercentage"] = 0
            data["failedPercentage"] = 0

        return data

    def get_elapsedTime(self, obj):
        events = {}
        events['createTime'] = obj.created
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        return elapsed(events)

    def get_events(self, obj):
        events = {}
        events['createTime'] = obj.created
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        if 'createTime' in events:
            events['createTime'] = datetime_format(events['createTime'])

        if 'startTime' in events:
            events['startTime'] = datetime_format(events['startTime'])

        if 'endTime' in events:
            events['endTime'] = datetime_format(events['endTime'])

        return events

class WorkflowDetailsSerializer(WorkflowSerializer):
    jobs = serializers.SerializerMethodField()
    factories = serializers.SerializerMethodField()
    dependencies = serializers.SerializerMethodField()
    labels = serializers.SerializerMethodField()

    def to_representation(self, instance):
        result = super(WorkflowDetailsSerializer, self).to_representation(instance)
        return OrderedDict([(key, result[key]) for key in result if result[key]])

    class Meta(WorkflowSerializer.Meta):
        fields = WorkflowSerializer.Meta.fields + ['jobs',
                                                   'factories',
                                                   'dependencies',
                                                   'labels']

    def get_jobs(self, obj):
        workflow_json = get_workflow_json(obj)
        if not workflow_json:
            return []
        if 'jobs' in workflow_json:
            return workflow_json['jobs']
        return []

    def get_factories(self, obj):
        workflow_json = get_workflow_json(obj)
        if not workflow_json:
            return []
        if 'factories' in workflow_json:
            return workflow_json['factories']
        return []

    def get_dependencies(self, obj):
        workflow_json = get_workflow_json(obj)
        if not workflow_json:
            return []
        if 'dependencies' in workflow_json:
            return workflow_json['dependencies']
        return {}

    def get_labels(self, obj):
        labels = {}
        if obj.labels:
            for label in obj.labels.all():
                labels[label.key] = label.value
        return labels

class WorkflowDetailsDisplaySerializer(WorkflowDetailsSerializer):
    def get_events(self, obj):
        events = {}
        events['createTime'] = obj.created
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        if 'createTime' in events:
            events['createTime'] = datetime_format(events['createTime'])

        if 'startTime' in events:
            events['startTime'] = datetime_format(events['startTime'])

        if 'endTime' in events:
            events['endTime'] = datetime_format(events['endTime'])

        return events

class JobSerializer(serializers.ModelSerializer):
    events = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    statusReason = serializers.SerializerMethodField()
    tasks = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = ['id', 'name', 'status', 'statusReason', 'events', 'tasks']

    def get_status(self, obj):
        return dict(Job.JOB_STATUSES)[obj.status]

    def get_statusReason(self, obj):
        return dict(Job.JOB_STATUS_REASONS)[obj.status_reason]

    def get_events(self, obj):
        events = {"createTime": obj.created}
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        return events

    def get_tasks(self, obj):
        # Unless users have requested all details, to keep things fast we don't show the full tasks
        task = {'image': obj.image}
        if obj.command:
            task['cmd'] = obj.command
        return [task]

class JobDisplaySerializer(JobSerializer):
    elapsedTime = serializers.SerializerMethodField()

    class Meta(JobSerializer.Meta):
        fields = JobSerializer.Meta.fields + ['elapsedTime']

    def get_elapsedTime(self, obj):
        events = {}
        events['createTime'] = obj.created
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        return elapsed(events)

    def get_events(self, obj):
        events = {"createTime": obj.created}
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        if 'createTime' in events:
            events['createTime'] = datetime_format(events['createTime'])

        if 'startTime' in events:
            events['startTime'] = datetime_format(events['startTime'])

        if 'endTime' in events:
            events['endTime'] = datetime_format(events['endTime'])

        return events

class JobDetailsSerializer(JobSerializer):
    # see https://stackoverflow.com/questions/49900629/django-serializer-inherit-and-extend-fields
    resources = serializers.SerializerMethodField()
    policies = serializers.SerializerMethodField()
    notifications = serializers.SerializerMethodField()
    artifacts = serializers.SerializerMethodField()
    outputFiles = serializers.SerializerMethodField()
    outputDirs = serializers.SerializerMethodField()
    execution = serializers.SerializerMethodField()
    parameters = serializers.SerializerMethodField()
    storage = serializers.SerializerMethodField()
    inputs = serializers.SerializerMethodField()
    labels = serializers.SerializerMethodField()

    def to_representation(self, instance):
        result = super(JobDetailsSerializer, self).to_representation(instance)
        return OrderedDict([(key, result[key]) for key in result if result[key]])

    class Meta(JobSerializer.Meta):
        fields = JobSerializer.Meta.fields + ['resources',
                                              'policies',
                                              'notifications',
                                              'inputs',
                                              'artifacts',
                                              'outputFiles',
                                              'outputDirs',
                                              'execution',
                                              'parameters',
                                              'labels',
                                              'storage']

    def get_resources(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'resources' in job_json:
                return job_json['resources']
        return {}

    def get_policies(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'policies' in job_json:
                return job_json['policies']
        return {}

    def get_inputs(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'inputs' in job_json:
                return job_json['inputs']
        return {}

    def get_storage(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'storage' in job_json:
                return redact_storage_creds(job_json['storage'])
        return {}

    def get_notifications(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'notifications' in job_json:
                return job_json['notifications']
        return []

    def get_artifacts(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'artifacts' in job_json:
                return job_json['artifacts']
        return []

    def get_tasks(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'tasks' in job_json:
                return job_json['tasks']
        return []

    def get_labels(self, obj):
        labels = {}
        if obj.labels:
            for label in obj.labels.all():
                labels[label.key] = label.value
        return labels

    def get_outputFiles(self, obj):
        job_json = get_job_json(obj)
        promlet_json = get_job_run_json(obj)

        backend = ProminenceBackend(server.settings.CONFIG)

        if job_json:
            if 'outputFiles' not in job_json:
                return []

            if not promlet_json:
                return job_json['outputFiles']

            if 'stageout' not in promlet_json:
                return job_json['outputFiles']

            location = get_uuid(obj)
            factory_id = get_factory_id(obj)
            if factory_id is not None:
                location = '%s/%d' % (location, factory_id)
            elif obj.workflow:
                pieces = obj.name.split('/')
                if len(pieces) == 2:
                    location = '%s/%s' % (location, pieces[1])

            outputs = []
            for output_file in job_json['outputFiles']:
                filename = os.path.basename(output_file)
                url = ''
                size = None

                if 'files' in promlet_json['stageout']:
                    for myfile in promlet_json['stageout']['files']:
                        if myfile['name'] == output_file and myfile['status'] == 'success':
                            url = backend.create_presigned_url('get',
                                                               server.settings.CONFIG['S3_BUCKET'],
                                                               'scratch/%s/%s' % (location, filename),
                                                               600)
                            size = backend.get_object_size(server.settings.CONFIG['S3_BUCKET'],
                                                           'scratch/%s/%s' % (location, filename))
                file_map = {'name':output_file, 'url':url}
                if size:
                    file_map['size'] = size

                outputs.append(file_map)

            return outputs
        return []

    def get_outputDirs(self, obj):
        job_json = get_job_json(obj)
        promlet_json = get_job_run_json(obj)

        backend = ProminenceBackend(server.settings.CONFIG)

        if job_json:
            if 'outputDirs' not in job_json:
                return []

            if not promlet_json:
                return job_json['outputDirs']

            if 'stageout' not in promlet_json:
                return job_json['outputDirs']

            location = get_uuid(obj)
            factory_id = get_factory_id(obj)
            if factory_id is not None:
                location = '%s/%d' % (location, factory_id)
            elif obj.workflow:
                pieces = obj.name.split('/')
                if len(pieces) == 2:
                    location = '%s/%s' % (location, pieces[1])

            outputs = []
            for output_dir in job_json['outputDirs']:
                dirs = output_dir.split('/')
                dirname_base = dirs[len(dirs) - 1]
                url = ''
                size = None

                if 'directories' in promlet_json['stageout']:
                    for myfile in promlet_json['stageout']['directories']:
                        if myfile['name'] == output_dir and myfile['status'] == 'success':
                            url = backend.create_presigned_url('get',
                                                               server.settings.CONFIG['S3_BUCKET'],
                                                               'scratch/%s/%s.tgz' % (location, dirname_base),
                                                               600)
                            size = backend.get_object_size(server.settings.CONFIG['S3_BUCKET'],
                                                           'scratch/%s/%s.tgz' % (location, dirname_base))

                file_map = {'name':output_dir, 'url':url}
                if size:
                    file_map['size'] = size

                outputs.append(file_map)                

            return outputs
        return []

    def get_execution(self, obj):
        if obj.status <= 2:
            if obj.site:
                if obj.site != '':
                    return {'site': obj.site}
            return {}
        else:
            execution = {}

            execution['site'] = 'Unknown'
            if obj.site:
                if obj.site != '':
                    execution['site'] = obj.site

            promlet_json = get_job_run_json(obj)
            if not promlet_json:
                return execution

            if 'cpu_vendor' in promlet_json and 'cpu_model' in promlet_json and 'cpu_clock' in promlet_json:
                execution['cpu'] = {'vendor': promlet_json['cpu_vendor'],
                                    'model': promlet_json['cpu_model'],
                                    'clock': promlet_json['cpu_clock']}

            if 'tasks' in promlet_json:
                tasks = []
                for task in promlet_json['tasks']:
                    if 'maxMemoryUsageKB' in task:
                        execution['maxMemoryUsageKB'] = task['maxMemoryUsageKB']
                    elif 'error' not in task:
                        tasks.append(task)

                execution['tasks'] = tasks
           
            stagein_time = 0
            if 'stagein' in promlet_json:
                for artifact in promlet_json['stagein']:
                    if 'time' in artifact:
                        stagein_time += artifact['time']

                execution['artifactDownloadTime'] = stagein_time

            return execution

    def get_parameters(self, obj):
        return {}

class JobDetailsDisplaySerializer(JobDetailsSerializer):
    workflow = serializers.SerializerMethodField()

    class Meta(JobSerializer.Meta):
        fields = JobDetailsSerializer.Meta.fields + ['workflow']

    def get_workflow(self, obj):
        if obj.workflow:
            return obj.workflow.id
        return None

    def get_events(self, obj):
        events = {"createTime": obj.created}
        if obj.time_start:
            events['startTime'] = obj.time_start
        if obj.time_end:
            events['endTime'] = obj.time_end

        if 'createTime' in events:
            events['createTime'] = datetime_format(events['createTime'])

        if 'startTime' in events:
            events['startTime'] = datetime_format(events['startTime'])

        if 'endTime' in events:
            events['endTime'] = datetime_format(events['endTime'])

        return events

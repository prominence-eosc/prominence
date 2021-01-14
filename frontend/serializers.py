import json
import re

from rest_framework import serializers
from frontend.models import Job, Workflow

def get_job_json(obj):
    if not obj.workflow:
        try:
            with open('%s/job.json' % obj.sandbox, 'r') as json_file:
                return json.load(json_file)
        except Exception as err:
            return None
    else:
        name = None
        factory_id = None

        match = re.search(r'([\w\_\-]+)\/([\w\_\-]+)\/([\d]+)', obj.name)
        if match:
            name = match.group(2)
            factory_id = int(match.group(3))
        else:
            match = re.search(r'([\w\_\-]+)\/([\w\_\-]+)', obj.name)
            if match:
                name = match.group(2)

        if not name:
            return None

        try:
            with open('%s/%s/job.json' % (obj.sandbox, name), 'r') as json_file:
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
        return {"createTime": obj.created}

    def get_progress(self, obj):
        return {"total": obj.jobs_total, "done": obj.jobs_done, "failed": obj.jobs_failed}

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

class JobDetailsSerializer(JobSerializer):
    # see https://stackoverflow.com/questions/49900629/django-serializer-inherit-and-extend-fields
    resources = serializers.SerializerMethodField()

    class Meta(JobSerializer.Meta):
        fields = JobSerializer.Meta.fields + ['resources',]

    def get_resources(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'resources' in job_json:
                return job_json['resources']
        return {}

    def get_tasks(self, obj):
        job_json = get_job_json(obj)
        if job_json:
            if 'tasks' in job_json:
                return job_json['tasks']
        return []

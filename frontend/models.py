from django.conf import settings
from django.db import models

class Workflow(models.Model):
    WORKFLOW_STATUSES = (
        (0, 'pending'),
        (1, 'pending'),
        (2, 'running'),
        (3, 'completed'),
        (4, 'deleted'),
        (5, 'failed'),
        (6, 'killed'),
        (7, 'unknown'),
    )

    WORKFLOW_STATUS_REASONS = (
        (0, ''),
        (1, 'Workflow deleted by user'),
        (2, 'One or more jobs failed'),
    )

    backend_id = models.PositiveIntegerField(db_index=True, unique=True, blank=True, null=True, default=None)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="workflows", null=True)
    name = models.CharField(max_length=512)
    status = models.PositiveSmallIntegerField(choices=WORKFLOW_STATUSES, default=0)
    #status_reason = models.PositiveSmallIntegerField(choices=WORKFLOW_STATUS_REASONS, default=0)
    updated = models.BooleanField(default=False)
    #in_queue = models.BooleanField(default=False)
    created = models.PositiveIntegerField(default=0)
    time_start = models.PositiveIntegerField(default=0)
    time_end = models.PositiveIntegerField(default=0)
    sandbox = models.CharField(max_length=100)
    uuid = models.CharField(max_length=36)
    jobs_total = models.PositiveIntegerField(default=0)
    jobs_done = models.PositiveIntegerField(default=0)
    jobs_failed = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.name

class Job(models.Model):
    JOB_STATUSES = (
        (0, 'pending'),
        (1, 'pending'),
        (2, 'running'),
        (3, 'completed'),
        (4, 'deleted'),
        (5, 'failed'),
        (6, 'killed'),
        (7, 'unknown'),
    )

    JOB_STATUS_REASONS = (
        (0, ''),
        (1, 'Creating infrastructure to run job'),
        (2, 'No matching resources'),
        (3, 'No matching resources currently available'),
        (4, 'Deployment failed'),
        (5, 'Unable to mount storage volume'),
        (6, 'Artifact download failed'),
        (7, 'Artifact uncompress failed'),
        (8, 'Stageout failed due to no such file or directory'),
        (9, 'Unable to stageout output to object storage'),
        (10, 'Container image pull failed'),
        (11, 'Job part of a workflow which was deleted by user'),
        (12, 'Walltime limit exceeded'),
        (13, 'Maximum time queued was exceeded'),
        (14, 'Executable exited with a non-zero exit code'),
        (15, 'Infrastructure took too long to be deployed'),
        (16, 'Job deleted by user'),
        (17, 'Quota exceeded'),
        (18, 'Required VM image not found'),
    )

    backend_id = models.PositiveIntegerField(db_index=True, unique=True, blank=True, null=True, default=None)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="jobs", null=True)
    name = models.CharField(max_length=512)
    status = models.PositiveSmallIntegerField(choices=JOB_STATUSES, default=0)
    status_reason = models.PositiveSmallIntegerField(choices=JOB_STATUS_REASONS, default=0)
    updated = models.BooleanField(default=False)
    in_queue = models.BooleanField(default=False)
    created = models.PositiveIntegerField(default=0)
    time_start = models.PositiveIntegerField(default=0)
    time_end = models.PositiveIntegerField(default=0)
    site = models.CharField(max_length=32, blank=True)
    sandbox = models.CharField(max_length=100)
    uuid = models.CharField(max_length=36)
    image = models.CharField(max_length=512)
    command = models.CharField(max_length=1024)
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE, blank=True, null=True, related_name="jobs")
    #workflow_node_name = models.CharField(max_length=512)
    #workflow_node_factory_id = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.name

class JobLabel(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, db_index=True, related_name="labels")
    key = models.CharField(max_length=512)
    value = models.CharField(max_length=512)

    def __str__(self):
        return self.name

class WorkflowLabel(models.Model):
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE, db_index=True, related_name="labels")
    key = models.CharField(max_length=512)
    value = models.CharField(max_length=512)

    def __str__(self):
        return self.name

class Storage(models.Model):
    WEBDAV = 1
    ONEDATA = 2
    STORAGE_TYPES = (
        (WEBDAV, 'WebDAV'),
        (ONEDATA, 'OneData'),
    )
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="storage_systems", null=True)
    name = models.CharField(max_length=120)
    hostname = models.CharField(max_length=120)
    username = models.CharField(max_length=120, blank=True)
    password = models.CharField(max_length=250, blank=True)
    token = models.CharField(max_length=250, blank=True)
    space = models.CharField(max_length=120, blank=True)
    storage_type = models.PositiveSmallIntegerField(choices=STORAGE_TYPES, default=2, blank=False)

    def __str__(self):
        return self.name

class Compute(models.Model):
    RESOURCE_TYPES = (
        (1, 'OpenStack'),
        (2, 'GCP'),
        (3, 'Azure'),
        (4, 'AWS'),
        (5, 'Kubernetes')
    )

    AUTH_VERSIONS = (
        (1, '2.0_password'),
        (2, '3.x_password'),
        (3, '3.x_oidc_access_token')
    )

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="resources", null=True)
    resource_type = models.PositiveSmallIntegerField(choices=RESOURCE_TYPES, default=1, blank=False)
    name = models.CharField(max_length=120)
    image_name = models.CharField(max_length=120, blank=True)

    ost_host = models.CharField(max_length=120, blank=True)
    ost_username = models.CharField(max_length=120, blank=True)
    ost_password = models.CharField(max_length=250, blank=True)
    ost_tenant = models.CharField(max_length=120, blank=True)
    ost_domain = models.CharField(max_length=120, blank=True)
    ost_auth_version = models.PositiveSmallIntegerField(choices=AUTH_VERSIONS, default=2, blank=False)
    ost_service_region = models.CharField(max_length=120, blank=True)
    ost_tenant_domain_id = models.CharField(max_length=120, blank=True)

    gcp_sa_email = models.CharField(max_length=500, blank=True)
    gcp_sa_private_key = models.CharField(max_length=5000, blank=True)
    gcp_project = models.CharField(max_length=120, blank=True)
    gcp_regions = models.CharField(max_length=120, blank=True)

    def __str__(self):
        return self.name

from django.conf import settings
from django.db import models
from django.contrib.auth.models import User

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
    storage_type = models.PositiveSmallIntegerField(choices=STORAGE_TYPES)

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
    image_name = models.CharField(max_length=120)

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

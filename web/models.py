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


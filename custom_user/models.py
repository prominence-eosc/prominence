from django.db import models
from django.contrib.auth.models import AbstractUser
from django.dispatch import receiver
from allauth.account.signals import user_logged_in

class User(AbstractUser):
    entitlements = models.TextField(max_length=2000, blank=True)

@receiver(user_logged_in)
def set_gender(sender, **kwargs):
    user = kwargs.pop('user')
    extra_data = user.socialaccount_set.filter(provider='egicheckin')[0].extra_data
    entitlements = extra_data['edu_person_entitlements']

    if entitlements:
        user.entitlements = ",".join(entitlements)

    user.save()

from django import forms
from .models import Storage

class StorageForm(forms.ModelForm):
    class Meta:
        model = Storage
        fields = ('name', 'storage_type', 'hostname', 'username', 'password')

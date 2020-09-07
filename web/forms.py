from django import forms
from django.forms.formsets import formset_factory

from .models import Storage

class StorageForm(forms.ModelForm):
    class Meta:
        model = Storage
        fields = ('name', 'storage_type', 'hostname', 'username', 'password')

CONTAINER_RUNTIMES = (
    ('singularity', 'Singularity'),
    ('udocker', 'udocker'),
)

TASK_TYPES = (
    ('standard', 'Standard'),
    ('openmpi', 'Open MPI'),
    ('intelmpi', 'Intel MPI'),
    ('mpich', 'MPICH'),
)

class LabelForm(forms.Form):
    key = forms.CharField(widget=forms.TextInput(attrs={'placeholder': 'Key'}), required=False)
    value = forms.CharField(widget=forms.TextInput(attrs={'placeholder': 'Value'}), required=False)

class EnvVarForm(forms.Form):
    key = forms.CharField(widget=forms.TextInput(attrs={'placeholder': 'Key'}), required=False)
    value = forms.CharField(widget=forms.TextInput(attrs={'placeholder': 'Value'}), required=False)

class ArtifactForm(forms.Form):
    url = forms.CharField(label='URL', required=False)
    executable = forms.BooleanField(required=False)

class JobForm(forms.Form):
    name = forms.CharField(required=False)
    task_type = forms.ChoiceField(choices=TASK_TYPES)
    container_image = forms.CharField()
    container_runtime = forms.ChoiceField(choices=CONTAINER_RUNTIMES)
    command = forms.CharField(required=False)
    workdir = forms.CharField(required=False)

    nodes = forms.IntegerField(label='Nodes', initial=1, min_value=1, max_value=128)
    cpus = forms.IntegerField(label='CPUs', initial=1, min_value=1, max_value=128)
    memory = forms.IntegerField(label='Memory (GB)', initial=1, min_value=1, max_value=128)
    disk = forms.IntegerField(label='Disk (GB)', initial=10, min_value=10, max_value=512)

    storage_name = forms.CharField(label='Name', required=False)
    storage_mountpoint = forms.CharField(label='Mount point', required=False)

LabelsFormSet = formset_factory(LabelForm)
EnvVarsFormSet = formset_factory(EnvVarForm)
ArtifactsFormSet = formset_factory(ArtifactForm)


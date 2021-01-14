from django import forms
from django.forms.formsets import formset_factory

from .models import Compute, Storage

class StorageForm(forms.ModelForm):
    class Meta:
        model = Storage
        fields = (
            'name',
            'storage_type',
            'hostname',
            'space',
            'username',
            'password',
            'token'
        )

class ComputeForm(forms.ModelForm):
    class Meta:
        model = Compute
        fields = (
            'name',
            'image_name',
            'resource_type',
            'ost_host',
            'ost_username',
            'ost_password',
            'ost_tenant',
            'ost_domain',
            'ost_auth_version',
            'ost_service_region',
            'ost_tenant_domain_id',
            'gcp_sa_email',
            'gcp_sa_private_key',
            'gcp_project',
            'gcp_regions'
        )

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

class InputFileForm(forms.Form):
    input_file = forms.FileField()

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
    walltime = forms.IntegerField(label='Walltime (hours)', initial=12, min_value=1, max_value=100800)

    storage_name = forms.ModelChoiceField(queryset=Storage.objects.none(), empty_label="", required=False)
    storage_mountpoint = forms.CharField(label='Mount point', required=False)

    policy_task_maxretries = forms.IntegerField(label='Task retries', initial=0, min_value=0, max_value=6)
    policy_job_maxretries = forms.IntegerField(label='Job retries', initial=0, min_value=0, max_value=6)
    policy_leave_job_in_queue = forms.BooleanField(required=False)
    policy_sites = forms.CharField(label='Sites', required=False)
    policy_regions = forms.CharField(label='Regions', required=False)

    notify_email_job_finished = forms.BooleanField(required=False)

    def __init__(self, user, *args, **kwargs):
        super(JobForm, self).__init__(*args, **kwargs)
        self.fields['storage_name'].queryset = Storage.objects.filter(user=user)

LabelsFormSet = formset_factory(LabelForm)
EnvVarsFormSet = formset_factory(EnvVarForm)
InputFilesFormSet = formset_factory(InputFileForm)
ArtifactsFormSet = formset_factory(ArtifactForm)

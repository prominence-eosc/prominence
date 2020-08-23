from django import forms
from .models import Storage
from crispy_forms.bootstrap import Tab, TabHolder
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Submit, Layout

class StorageForm(forms.ModelForm):
    class Meta:
        model = Storage
        fields = ('name', 'storage_type', 'hostname', 'username', 'password')

CONTAINER_RUNTIMES = (
    ('singularity', 'Singularity'),
    ('udocker', 'udocker'),
)

class NoFormTagCrispyFormMixin(object):
    @property
    def helper(self):
        if not hasattr(self, '_helper'):
            self._helper = FormHelper()
            self._helper.form_tag = False
        return self._helper

class JobForm(forms.Form):
    name = forms.CharField(required=False)
    container_image = forms.CharField()
    container_runtime = forms.ChoiceField(choices=CONTAINER_RUNTIMES)
    command = forms.CharField(required=False)

    cpus = forms.IntegerField(label='CPUs', initial=1, min_value=1, max_value=128)
    memory = forms.IntegerField(label='Memory (GB)', initial=1, min_value=1, max_value=128)
    disk = forms.IntegerField(label='Disk (GB)', initial=10, min_value=10, max_value=512)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.form_tag = False
        self.helper.layout = Layout(
            TabHolder(
                Tab('Basic',
                    'name',
                    'container_image',
                    'container_runtime',
                    'command'
                ),
                Tab('Resources',
                    'cpus',
                    'memory',
                    'disk'
                )
            )
        )
        #self.helper.layout.append(Submit('submit', 'Submit'))

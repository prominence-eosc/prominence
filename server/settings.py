from django.conf import settings

CONFIG = {}
CONFIG['SANDBOX_PATH'] = getattr(settings, 'PROMINENCE_SANDBOX_PATH', '/var/spool/prominence/sandboxes')
CONFIG['DEFAULT_MAX_RUNTIME'] = getattr(settings, 'PROMINENCE_DEFAULT_MAX_RUNTIME', 43200)
CONFIG['DEFAULT_DISK_GB'] = getattr(settings, 'PROMINENCE_DEFAULT_DISK_GB', 10)
CONFIG['PROMLET_FILE'] = '/usr/local/libexec/promlet.py'
CONFIG['WORKFLOW_MAX_IDLE'] = getattr(settings, 'PROMINENCE_WORKFLOW_MAX_IDLE', 20)
CONFIG['EXEC_TIMEOUT'] = getattr(settings, 'PROMINENCE_EXEC_TIMEOUT', 10)
CONFIG['ENABLE_EXEC'] = getattr(settings, 'PROMINENCE_ENABLE_EXEC', False)
CONFIG['ENABLE_SNAPSHOTS'] = getattr(settings, 'PROMINENCE_ENABLE_SNAPSHOTS', False)
CONFIG['ENABLE_DATA'] =  getattr(settings, 'PROMINENCE_ENABLE_DATA', False)
CONFIG['S3_URL'] = getattr(settings, 'PROMINENCE_S3_URL', 'none')
CONFIG['S3_ACCESS_KEY_ID'] = getattr(settings, 'PROMINENCE_S3_ACCESS_KEY_ID', 'none')
CONFIG['S3_BUCKET'] = getattr(settings, 'PROMINENCE_S3_BUCKET', 'none')
CONFIG['MAX_CPUS_PER_JOB'] = getattr(settings, 'PROMINENCE_MAX_CPUS_PER_JOB', 4)
CONFIG['MAX_MEMORY_PER_JOB'] = getattr(settings, 'PROMINENCE_MAX_MEMORY_PER_JOB', 4)
CONFIG['USE_PENDING_STATE'] = getattr(settings, 'PROMINENCE_USE_PENDING_STATE', True)
CONFIG['ELASTICSEARCH_HOST'] = getattr(settings, 'PROMINENCE_ELASTICSEARCH_HOST', 'localhost')
CONFIG['ELASTICSEARCH_PORT'] = getattr(settings, 'PROMINENCE_ELASTICSEARCH_PORT', 9200)
CONFIG['ELASTICSEARCH_INDEX'] = getattr(settings, 'PROMINENCE_ELASTICSEARCH_INDEX', 'prominence')
CONFIG['REQUIRED_ENTITLEMENTS'] = [{"urn:mace:egi.eu:aai.egi.eu:vm_operator@vo.access.egi.eu", "urn:mace:egi.eu:aai.egi.eu:member@vo.access.egi.eu"}, {"urn:mace:egi.eu:aai.egi.eu:vm_operator@fusion", "urn:mace:egi.eu:aai.egi.eu:member@fusion"}]
CONFIG['IMC_URL'] = getattr(settings, 'PROMINENCE_IMC_URL', '')
CONFIG['IMC_USERNAME'] = getattr(settings, 'PROMINENCE_IMC_USERNAME', '')
CONFIG['IMC_PASSWORD'] = getattr(settings, 'PROMINENCE_IMC_PASSWORD', '')
CONFIG['IMC_SSL_CERT'] = getattr(settings, 'PROMINENCE_IMC_SSL_CERT', '')
CONFIG['IMC_SSL_KEY'] = getattr(settings, 'PROMINENCE_IMC_SSL_KEY', '')
CONFIG['INFLUXDB_URL'] = getattr(settings, 'PROMINENCE_INFLUXDB_URL', '')
CONFIG['INFLUXDB_TOKEN'] = getattr(settings, 'PROMINENCE_INFLUXDB_TOKEN', '')
CONFIG['INFLUXDB_ORG'] = getattr(settings, 'PROMINENCE_INFLUXDB_ORG', '')
CONFIG['INFLUXDB_BUCKET'] = getattr(settings, 'PROMINENCE_INFLUXDB_BUCKET', '')


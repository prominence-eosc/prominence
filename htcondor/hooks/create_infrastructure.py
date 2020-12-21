#!/usr/bin/python3
from __future__ import print_function
import base64
import configparser
import json
import re
from string import Template
import subprocess
import time
import uuid
import requests
import requests.packages.urllib3
from requests.auth import HTTPBasicAuth
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend

requests.packages.urllib3.disable_warnings()

CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

def get_job_json(filename, sandbox_dir):
    """
    """
    filename_str = filename.replace(sandbox_dir, '')
    match = re.search(r'/([\w\-]+)/([\w\-\_]+)/\d\d/job.json', filename_str)
    if match:
        return '%s/%s/%s/job.json' % (sandbox_dir, match.group(1), match.group(2))
    return filename

def create_ssh_keypair():
    """
    Create an ssh keypair
    """
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=4096
    )
    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption())
    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH,
        crypto_serialization.PublicFormat.OpenSSH
    )

    return (private_key, public_key)

def create_infrastructure_with_retries(uid, data):
    """
    Create infrastructure with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    success = None
    while count < max_retries and success is None:
        success = create_infrastructure(uid, data)
        count += 1
        time.sleep(count/2)
    return success

def create_infrastructure(uid, data):
    """
    Create infrastructure
    """
    headers = {}
    headers['Idempotency-Key'] = uid
    try:
        response = requests.post('%s' % CONFIG.get('imc', 'url'),
                                 auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                    CONFIG.get('imc', 'password')),
                                 cert=(CONFIG.get('imc', 'ssl-cert'),
                                       CONFIG.get('imc', 'ssl-key')),
                                 verify=CONFIG.get('imc', 'ssl-cert'),
                                 json=data,
                                 headers=headers,
                                 timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return None
    except requests.exceptions.RequestException:
        return None
    if response.status_code == 201 or response.status_code == 200:
        return response.json()['id']
    return None

def prepare_credential_content(filename, itype=False, use_file=True):
    """
    Format strings for inclusion in radl templates
    """
    if use_file:
        with open(filename) as file_in:
            content = file_in.readlines()
    else:
        content = []
        for line in filename.split(b'\n'):
            content.append('%s\n' % line.decode('utf-8'))

    if itype:
        content = ['          %s' % line for line in content]
    else:
        content = ['%s' % line for line in content]
    return ''.join(content)

def create_worker_credentials(itype):
    """
    Create worker node credentials
    """
    # Create token for HTCondor auth
    token_duration = 30*24*60*60
    run = subprocess.run(["sudo",
                          "condor_token_create",
                          "-identity",
                          "worker@cloud",
                          "-key",
                          "token_key",
                          "-lifetime",
                          "%s" % token_duration],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
    if run.returncode == 0:
        token = run.stdout.strip()
    else:
        raise Exception('condor_token_create failed with invalid return code')

    # Create ssh keys
    (private_ssh_key_1, public_ssh_key_1) = create_ssh_keypair()
    (private_ssh_key_2, public_ssh_key_2) = create_ssh_keypair()

    return (prepare_credential_content(token, itype, False),
            prepare_credential_content(private_ssh_key_1, itype, False),
            prepare_credential_content(public_ssh_key_1, itype, False),
            prepare_credential_content(private_ssh_key_2, itype, False),
            prepare_credential_content(public_ssh_key_2, itype, False))

def deploy(identity, my_groups, iwd, cluster_id, dag_node_name, dag_job_id, uid, factory_id, subdir=None, cpu_scaling=1):
    """
    Deploy infrastructure
    """
    job_id = '%s.0' % cluster_id
    uid = "%s-%d" % (uid, factory_id)

    # Open JSON job description
    if subdir:
        filename = '%s/%s/job.json' % (iwd, subdir)
    else:
        filename = '%s/job.json' % iwd
    filename = get_job_json(filename, '/var/spool/prominence/sandboxes')
    with open(filename, 'r') as json_file:
        job_json = json.load(json_file)

    # Create infrastructure ID
    uid_infra = str(uuid.uuid4())

    # Current time
    epoch = int(time.time())

    # Get appropriate RADL template depending on job type
    spacing_type = False
    want_mpi = False
    if job_json['resources']['nodes'] > 1:
        radl_file = CONFIG.get('templates', 'multi-node-mpi')
        spacing_type = True
        want_mpi = True
    else:
        radl_file = CONFIG.get('templates', 'single-node')

    # Create credentials for the worker nodes
    (token,
     private_ssh_key_1,
     public_ssh_key_1,
     private_ssh_key_2,
     public_ssh_key_2) = create_worker_credentials(spacing_type)

    # Calculate total cores and number of worker nodes
    num_total_cores = job_json['resources']['nodes']*job_json['resources']['cpus']
    num_worker_nodes = job_json['resources']['nodes'] - 1

    # POSIX mounts
    b2drop_app_username = None
    b2drop_app_password = None
    storage_mountpoint = None
    onedata_provider = None
    onedata_token = None
    webdav_username = None
    webdav_password = None
    webdav_url = None
    add_mounts = ''

    if 'storage' in job_json:
        if 'mountpoint' in job_json['storage']:
            storage_mountpoint = job_json['storage']['mountpoint']
        if 'type' in job_json['storage']:
            if job_json['storage']['type'] == 'b2drop':
                if 'b2drop' in job_json['storage']:
                    if 'app-username' in job_json['storage']['b2drop']:
                        b2drop_app_username = job_json['storage']['b2drop']['app-username']
                    if 'app-password' in job_json['storage']['b2drop']:
                        b2drop_app_password = job_json['storage']['b2drop']['app-password']
            elif job_json['storage']['type'] == 'webdav':
                if 'webdav' in job_json['storage']:
                    if 'username' in job_json['storage']['webdav']:
                        webdav_username = job_json['storage']['webdav']['username']
                    if 'password' in job_json['storage']['webdav']:
                        webdav_password = job_json['storage']['webdav']['password']
                    if 'url' in job_json['storage']['webdav']:
                        webdav_url = job_json['storage']['webdav']['url']

    if storage_mountpoint:
        add_mounts = '-v /mnt%s:/home/user%s' % (storage_mountpoint, storage_mountpoint)

    with open(radl_file) as data:
        radl_template = Template(data.read())

    use_hostname = '%s-%d' % (uid_infra, epoch)
    use_uid = use_hostname

    if dag_job_id:
        dag_job_id = "-e DAG_JOB_ID=%s" % dag_job_id
    else:
        dag_job_id = "-e DAG_JOB_ID=0"

    # Scale resource requirements if necessary
    cpus_req = job_json['resources']['cpus']
    memory_req = job_json['resources']['memory']
    disk_req = job_json['resources']['disk']

    if cpu_scaling > 1:
        cpus_req = cpus_req*cpu_scaling
        memory_req = memory_req*cpu_scaling
        disk_req = disk_req*cpu_scaling

    # Generate RADL based on existing template
    radl_contents = radl_template.substitute(cores_per_node=cpus_req,
                                             job_cores_per_node=job_json['resources']['cpus'],
                                             memory_per_node=memory_req*1000,
                                             job_memory_per_node=job_json['resources']['memory']*1000,
                                             num_nodes=job_json['resources']['nodes'],
                                             num_worker_nodes=num_worker_nodes,
                                             num_total_cores=num_total_cores,
                                             cluster=use_uid,
                                             dag_node_name=dag_node_name,
                                             dag_job_id=dag_job_id,
                                             use_hostname=use_hostname,
                                             disk_size=disk_req,
                                             job_id=cluster_id,
                                             uid_infra=uid_infra,
                                             condor_host=CONFIG.get('htcondor', 'manager'),
                                             token=token,
                                             private_ssh_key_1=private_ssh_key_1,
                                             public_ssh_key_1=public_ssh_key_1,
                                             private_ssh_key_2=private_ssh_key_2,
                                             public_ssh_key_2=public_ssh_key_2,
                                             b2drop_app_username=b2drop_app_username,
                                             b2drop_app_password=b2drop_app_password,
                                             webdav_username=webdav_username,
                                             webdav_password=webdav_password,
                                             webdav_url=webdav_url,
                                             storage_mount_point=storage_mountpoint,
                                             onedata_provider=onedata_provider,
                                             onedata_token=onedata_token,
                                             storage_mounts=add_mounts)

    # Generate JSON document to provide to IMC
    data = {}
    data['requirements'] = {}

    data['requirements']['image'] = {}
    data['requirements']['image']['distribution'] = CONFIG.get('vm', 'image-dist')
    data['requirements']['image']['version'] = CONFIG.get('vm', 'image-version')
    data['requirements']['image']['type'] = CONFIG.get('vm', 'image-type')
    data['requirements']['image']['architecture'] = CONFIG.get('vm', 'image-arch')

    data['requirements']['resources'] = {}
    data['requirements']['resources']['cores'] = cpus_req
    data['requirements']['resources']['memory'] = memory_req
    data['requirements']['resources']['disk'] = disk_req
    data['requirements']['regions'] = CONFIG.get('deployment', 'req-regions').split(',')
    if CONFIG.get('deployment', 'req-sites'):
        data['requirements']['sites'] = CONFIG.get('deployment', 'req-sites').split(',')
    data['requirements']['groups'] = my_groups

    data['preferences'] = {}
    data['preferences']['regions'] = CONFIG.get('deployment', 'pref-regions').split(',')
    if CONFIG.get('deployment', 'pref-sites'):
        data['preferences']['sites'] = CONFIG.get('deployment', 'pref-sites').split(',')

    # If job contains placement policy, use this instead of the default
    if 'policies' in job_json:
        if 'placement' in job_json['policies']:
            if 'requirements' in job_json['policies']['placement']:
                if 'sites' in job_json['policies']['placement']['requirements']:
                    data['requirements']['sites'] = job_json['policies']['placement']['requirements']['sites']
                    data['requirements']['regions'] = []
                    data['preferences']['sites'] = []
                    data['preferences']['regions'] = []
                if 'regions' in job_json['policies']['placement']['requirements']:
                    data['requirements']['regions'] = job_json['policies']['placement']['requirements']['regions']
                    data['preferences']['regions'] = []

    if 'policies' in job_json:
        if 'placement' in job_json['policies']:
            if 'preferences' in job_json['policies']['placement']:
                if 'sites' in job_json['policies']['placement']['preferences']:
                    data['preferences']['sites'] = job_json['policies']['placement']['preferences']['sites']
                if 'regions' in job_json['policies']['placement']['preferences']:
                    data['preferences']['regions'] = job_json['policies']['placement']['preferences']['regions']

    if want_mpi:
        data['requirements']['tags'] = {}
        data['requirements']['tags']['multi-node-jobs'] = 'true'

    data['radl'] = base64.b64encode(radl_contents.encode('utf8')).decode()
    data['identifier'] = job_id
    data['identity'] = identity
    data['want'] = use_uid

    # Create infrastructure
    infra_id = create_infrastructure_with_retries(uid_infra, data)
    return (use_uid, infra_id)

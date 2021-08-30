#!/usr/bin/python3
from __future__ import print_function
import base64
import calendar
import configparser
import json
import logging
from logging.handlers import RotatingFileHandler
import re
from string import Template
import sys
import time
import uuid
import requests
import requests.packages.urllib3
from requests.auth import HTTPBasicAuth
import subprocess
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import classad

import update_presigned_urls
import create_job_token

requests.packages.urllib3.disable_warnings()

def get_from_classad(name, class_ad, default=None):
    """
    Get the value of the specified item from a job ClassAd
    """
    value = default
    if name in class_ad:
        value = class_ad[name]
    return value

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
        if not success:
            logging.warning('Infrastructe create request failed')
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
                                 #cert=(CONFIG.get('imc', 'ssl-cert'),
                                 #      CONFIG.get('imc', 'ssl-key')),
                                 #verify=CONFIG.get('imc', 'ssl-cert'),
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

def create_worker_credentials(itype, max_run_time):
    """
    Create worker node credentials
    """
    root_ca = prepare_credential_content(CONFIG.get('credentials', 'root-ca'), itype)
    signing_policy = prepare_credential_content(CONFIG.get('credentials', 'signing-policy'), itype)
    mapfile = prepare_credential_content(CONFIG.get('credentials', 'mapfile'), itype)

    # Create token for HTCondor auth
    token_duration = 3*24*60*60
    if max_run_time > 0:
        token_duration = int(max_run_time*60)

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

    token = prepare_credential_content(token, itype, False)

    # Prepare ssh keys
    (private_ssh_key_1, public_ssh_key_1) = create_ssh_keypair()
    (private_ssh_key_2, public_ssh_key_2) = create_ssh_keypair()

    private_ssh_key_1 = prepare_credential_content(private_ssh_key_1, itype, False)
    public_ssh_key_1 = prepare_credential_content(public_ssh_key_1, itype, False)
    private_ssh_key_2 = prepare_credential_content(private_ssh_key_2, itype, False)
    public_ssh_key_2 = prepare_credential_content(public_ssh_key_2, itype, False)

    return (token,
            private_ssh_key_1,
            public_ssh_key_1,
            private_ssh_key_2,
            public_ssh_key_2)

def translate_classad():
    """
    Deploy infrastructure for a job
    """
    route = ''
    condor_host = CONFIG.get('htcondor', 'manager')

    classad_in = sys.stdin.read().split('------')

    # Get route name
    match_obj = re.search(r'name = "([\w\-]+)"', classad_in[0])
    if match_obj:
        route = match_obj.group(1)

    job_ad = classad.parseOne(classad_in[1], parser=classad.Parser.Old)
    classad_new = job_ad

    iwd = get_from_classad('Iwd', job_ad)
    dag_node_name = get_from_classad('DAGNodeName', job_ad)
    cluster_id = int(get_from_classad('ClusterId', job_ad, -1))
    proc_id = int(get_from_classad('ProcId', job_ad, 0))
    job_status = int(get_from_classad('JobStatus', job_ad, 0))
    identity = get_from_classad('ProminenceIdentity', job_ad)
    uid = get_from_classad('ProminenceJobUniqueIdentifier', job_ad)
    my_groups = get_from_classad('ProminenceGroup', job_ad).split(',')
    groups = get_from_classad('ProminenceGroup', job_ad)
    factory_id = int(get_from_classad('ProminenceFactoryId', job_ad, 0))
    want_mpi = get_from_classad('ProminenceWantMPI', job_ad)
    existing_route_name = get_from_classad('RouteName', job_ad)
    args = get_from_classad('Args', job_ad)
    max_run_time = int(get_from_classad('ProminenceMaxRunTime', job_ad, -1))

    if want_mpi:
        want_mpi = True
    else:
        want_mpi = False

    job_id = '%s.%s' % (cluster_id, proc_id)
    uid_raw = uid
    uid = "%s-%d" % (uid, factory_id)

    logger.info('[%s] Starting cloud_hook_translate_job', job_id)

    # Open JSON job description
    try:
        filename = '%s/.job.json' % iwd
        with open(filename, 'r') as json_file:
            job_json = json.load(json_file)
    except Exception as err:
        logger.error('[%s] Unable to open JSON job description due to: %s', job_id, err)
        sys.exit(1)

    if 'batch' in route:
        # Write out updated ClassAd to stdout
        classad_new['InfrastructureSite'] = route
        print(classad_new.printOld())
        logger.info('[%s] Exiting cloud_hook_translate_job in batch mode for route %s', job_id, route)
        sys.exit(0)
    elif job_status == 1:
        logger.info('[%s] Attempting to create cloud infrastructure', job_id)

        # Create infrastructure ID
        uid_infra = str(uuid.uuid4())

        # Handle jobs submitted directly to HTCondor
        if existing_route_name:
            classad_new['TransferOutput'] = "promlet.0.log,promlet.0.json"

        # Current time
        epoch = int(time.time())
        classad_new['ProminenceLastRouted'] = epoch
        classad_new['ProminenceInfrastructureEnteredCurrentStatus'] = epoch

        # Get appropriate RADL template depending on job type
        spacing_type = False
        if want_mpi and job_json['resources']['nodes'] > 1:
            radl_file = CONFIG.get('templates', 'multi-node-mpi')
            spacing_type = True
        else:
            radl_file = CONFIG.get('templates', 'single-node')

        # Create credentials for the worker nodes
        (token,
         private_ssh_key_1,
         public_ssh_key_1,
         private_ssh_key_2,
         public_ssh_key_2) = create_worker_credentials(spacing_type, max_run_time)

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
             
        logger.info('[%s] Using mounts="%s"', job_id, add_mounts)

        try:
            with open(radl_file) as data:
                radl_template = Template(data.read())
        except IOError as e:
            logger.critical('[%s] Exiting due to IO error opening RADL template: %s', job_id, e)
            exit(1)
        except Exception as e:
            logger.critical('[%s] Exiting due to unexpected error opening RADL template: %s', job_id, e)
            exit(1)

        use_hostname = '%s-%d' % (uid_infra, epoch)
        use_uid = use_hostname

        # Generate RADL based on existing template
        try:
            radl_contents = radl_template.substitute(cores_per_node=job_json['resources']['cpus'],
                                                     memory_per_node=job_json['resources']['memory'],
                                                     num_nodes=job_json['resources']['nodes'],
                                                     num_worker_nodes=num_worker_nodes,
                                                     num_total_cores=num_total_cores,
                                                     cluster=use_uid,
                                                     use_hostname=use_hostname,
                                                     uid_infra=uid_infra,
                                                     disk_size=job_json['resources']['disk'],
                                                     job_id=cluster_id,
                                                     condor_host=condor_host,
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
        except KeyError as e:
            logger.critical('[%s] Exiting due to KeyError creating RADL template: %s', job_id, e)
            exit(1)
        except ValueError as e:
            logger.critical('[%s] Exiting due to ValueError creating RADL template: %s', job_id, e)
            exit(1)

        # Generate JSON document to provide to IMC
        data = {}
        data['requirements'] = {}

        data['requirements']['image'] = {}
        data['requirements']['image']['distribution'] = CONFIG.get('vm', 'image-dist')
        data['requirements']['image']['version'] = CONFIG.get('vm', 'image-version')
        data['requirements']['image']['type'] = CONFIG.get('vm', 'image-type')
        data['requirements']['image']['architecture'] = CONFIG.get('vm', 'image-arch')

        data['requirements']['resources'] = {}
        data['requirements']['resources']['cores'] = job_json['resources']['cpus']
        data['requirements']['resources']['memory'] = job_json['resources']['memory']
        data['requirements']['resources']['disk'] = job_json['resources']['disk']
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
                        data['preferences']['sites'] = {}
                        data['preferences']['regions'] = {}
                    if 'regions' in job_json['policies']['placement']['requirements']:
                        data['requirements']['regions'] = job_json['policies']['placement']['requirements']['regions']
                        data['preferences']['regions'] = {}

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

        #data['radl'] = base64.b64encode(radl_contents.encode('utf8'))
        data['radl'] = base64.b64encode(radl_contents.encode('utf8')).decode()
        data['identifier'] = job_id
        data['identity'] = identity
        data['want'] = use_uid

        if identity and groups and max_run_time > -1:
            lifetime = max_run_time*60 + 3600
            job_token = create_job_token.create_job_token(identity, groups, lifetime, uid_raw)
            classad_new['ProminenceJobToken'] = str('%s' % job_token.decode('utf-8'))
            classad_new['ProminenceURL'] = str('%s' % CONFIG.get('url', 'restapi'))

        # Create infrastructure
        logger.info('[%s] About to create infrastructure with Idempotency-Key "%s"', job_id, uid_infra)
        infra_id = create_infrastructure_with_retries(uid_infra, data)

        if infra_id is None:
            classad_new['ProminenceInfrastructureState'] = 'failed'
            logger.info('[%s] Deployment onto cloud failed', job_id)
        else:
            classad_new['ProminenceInfrastructureId'] = str('%s' % infra_id)
            classad_new['ProminenceInfrastructureState'] = 'deployment-init'
            classad_new['ProminenceWantCluster'] = use_uid
            classad_new['Requirements'] = classad.ExprTree('MY.ProminenceInfrastructureState =?= "configured"')
            classad_new['ProminenceProcId'] = str('%d' % proc_id)

            logger.info('[%s] Initiated infrastructure deployment with id "%s"', job_id, infra_id)

            new_args = update_presigned_urls.update_presigned_urls(args, '%s/.job.mapped.json' % iwd)
            if new_args:
                classad_new['Args'] = str('%s' % new_args)

    # Write out updated ClassAd to stdout
    print(classad_new.printOld())

    # Write status file
    filename = '%s/status' % iwd
    try:
        with open(filename, 'w') as status_file:
            status_file.write('deploying')
    except Exception:
        logger.critical('[%s] Unable to write status file', job_id)

    logger.info('[%s] Exiting cloud_hook_translate_job', job_id)

if __name__ == "__main__":
    # Read config file
    CONFIG = configparser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    handler = RotatingFileHandler(CONFIG.get('logs', 'translate'),
                                  maxBytes=int(CONFIG.get('logs', 'max_bytes')),
                                  backupCount=int(CONFIG.get('logs', 'num')))
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger('cloud_hook_translate_job')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Create infrastructure
    translate_classad()

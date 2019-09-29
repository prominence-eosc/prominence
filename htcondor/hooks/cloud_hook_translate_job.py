#!/usr/bin/python
from __future__ import print_function
import base64
import calendar
import ConfigParser
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend
import json
import logging
import M2Crypto
import re
from string import Template
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
import classad

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

def emptyCallback1(p1):
   return

def emptyCallback2(p1, p2):
   return

def makeX509Proxy(certPath, keyPath, expirationTime, isLegacyProxy=False, cn=None):
   """
   Return a PEM-encoded limited proxy as a string in either Globus Legacy
   or RFC 3820 format. Checks that the existing cert/proxy expires after
   the given expirationTime, but no other checks are done.
   """

   # First get the existing priviate key

   try:
     oldKey = M2Crypto.RSA.load_key(keyPath, emptyCallback1)
   except Exception as e:
     raise IOError('Failed to get private key from ' + keyPath + ' (' + str(e) + ')')

   # Get the chain of certificates (just one if a usercert or hostcert file)

   try:
     certBIO = M2Crypto.BIO.File(open(certPath))
   except Exception as e:
     raise IOError('Failed to open certificate file ' + certPath + ' (' + str(e) + ')')

   oldCerts = []

   while True:
     try:
       oldCerts.append(M2Crypto.X509.load_cert_bio(certBIO))
     except:
       certBIO.close()
       break

   if len(oldCerts) == 0:
     raise IOError('Failed get certificate from ' + certPath)

   # Check the expirationTime

   if int(calendar.timegm(time.strptime(str(oldCerts[0].get_not_after()), "%b %d %H:%M:%S %Y %Z"))) < expirationTime:
     raise IOError('Cert/proxy ' + certPath + ' expires before given expiration time ' + str(expirationTime))

   # Create the public/private keypair for the new proxy

   newKey = M2Crypto.EVP.PKey()
   newKey.assign_rsa(M2Crypto.RSA.gen_key(1024, 65537, emptyCallback2))

   # Start filling in the new certificate object

   newCert = M2Crypto.X509.X509()
   newCert.set_pubkey(newKey)
   newCert.set_serial_number(int(time.time() * 100))
   newCert.set_issuer_name(oldCerts[0].get_subject())
   newCert.set_version(2) # "2" is X.509 for "v3" ...

   # Construct the legacy or RFC style subject

   newSubject = oldCerts[0].get_subject()

   if isLegacyProxy:
     # Globus legacy proxy
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = 'limited proxy',
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)
   elif cn:
     # RFC proxy, probably with machinetypeName as proxy CN
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = cn,
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)
   else:
     # RFC proxy, with Unix time as CN
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = str(int(time.time() * 100)),
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)

   newCert.set_subject_name(newSubject)

   # Set start and finish times

   newNotBefore = M2Crypto.ASN1.ASN1_UTCTIME()
   newNotBefore.set_time(int(time.time()))
   newCert.set_not_before(newNotBefore)

   newNotAfter = M2Crypto.ASN1.ASN1_UTCTIME()
   newNotAfter.set_time(expirationTime)
   newCert.set_not_after(newNotAfter)

   # Add extensions, possibly including RFC-style proxyCertInfo

   newCert.add_ext(M2Crypto.X509.new_extension("keyUsage", "Digital Signature, Key Encipherment, Key Agreement", 1))

   if not isLegacyProxy:
     newCert.add_ext(M2Crypto.X509.new_extension("proxyCertInfo", "critical, language:1.3.6.1.4.1.3536.1.1.1.9", 1, 0))

   # Sign the certificate with the old private key
   oldKeyEVP = M2Crypto.EVP.PKey()
   oldKeyEVP.assign_rsa(oldKey)
   newCert.sign(oldKeyEVP, 'sha256')

   # Return proxy as a string of PEM blocks

   proxyString = newCert.as_pem() + newKey.as_pem(cipher = None)

   for oneOldCert in oldCerts:
     proxyString += oneOldCert.as_pem()

   return proxyString

def create_infrastructure_with_retries(data):
    """
    Create infrastructure with retries & backoff
    """
    max_retries = int(CONFIG.get('imc', 'retries'))
    count = 0
    success = None
    while count < max_retries and success is None:
        success = create_infrastructure(data)
        count += 1
        time.sleep(count/2)
    return success

def create_infrastructure(data):
    """
    Create infrastructure
    """
    try:
        response = requests.post('%s' % CONFIG.get('imc', 'url'),
                                 auth=HTTPBasicAuth(CONFIG.get('imc', 'username'),
                                                    CONFIG.get('imc', 'password')),
                                 json=data,
                                 timeout=int(CONFIG.get('imc', 'timeout')))
    except requests.exceptions.Timeout:
        return None
    except requests.exceptions.RequestException:
        return None
    if response.status_code == 201:
        return response.json()['id']
    return None

def prepare_credential_content(filename, itype=False, file=True):
    """
    Format strings for inclusion in radl templates
    """
    if file:
        with open(filename) as file_in:
            content = file_in.readlines()
    else:
        content = []
        for line in filename.split('\n'):
            content.append('%s\n' % line)

    if itype:
        content = ['          %s' % line for line in content]
    else:
        content = ['%s' % line for line in content]
    return ''.join(content)

def create_worker_credentials(itype=False):
    """
    Create worker node credentials
    """
    root_ca = prepare_credential_content(CONFIG.get('credentials', 'root-ca'), itype)
    signing_policy = prepare_credential_content(CONFIG.get('credentials', 'signing-policy'), itype)
    mapfile = prepare_credential_content(CONFIG.get('credentials', 'mapfile'), itype)

    # Proxy for HTCondor auth
    expiry_time = int(time.time()) + 7*24*60*60
    proxy = makeX509Proxy('/etc/prominence/credentials/hostcert.pem',
                          '/etc/prominence/credentials/hostkey.pem',
                          expiry_time,
                          isLegacyProxy=False,
                          cn=None)

    proxy = prepare_credential_content(proxy, itype, file=False)

    # Prepare ssh keys
    (private_ssh_key_1, public_ssh_key_1) = create_ssh_keypair()
    (private_ssh_key_2, public_ssh_key_2) = create_ssh_keypair()

    private_ssh_key_1 = prepare_credential_content(private_ssh_key_1, itype, file=False)
    public_ssh_key_1 = prepare_credential_content(public_ssh_key_1, itype, file=False)
    private_ssh_key_2 = prepare_credential_content(private_ssh_key_2, itype, file=False)
    public_ssh_key_2 = prepare_credential_content(public_ssh_key_2, itype, file=False)

    return (root_ca,
            proxy,
            signing_policy,
            mapfile,
            private_ssh_key_1,
            public_ssh_key_1,
            private_ssh_key_2,
            public_ssh_key_2)

def translate_classad():
    """
    Deploy infrastructure for a job
    """
    route = ''
    num_nodes = 0
    cores_per_node = 0
    memory_per_node = 0
    disk_size = 0
    want_mpi = False
    condor_host = CONFIG.get('htcondor', 'manager')
    proc_id = 0
    job_status = 0
    my_groups = []
    dag_node_name = None
    identity = None
    iwd = None

    classad_in = sys.stdin.read().split('------')

    # Get route name
    match_obj = re.search(r'name = "([\w\-]+)"', classad_in[0])
    if match_obj:
        route = match_obj.group(1)

    job_ad = classad.parseOne(classad_in[1], parser=classad.Parser.Old)
    classad_new = job_ad

    if 'Iwd' in job_ad:
        iwd = job_ad['Iwd']
    if 'DAGNodeName' in job_ad:
        dag_node_name = job_ad['DAGNodeName']
    if 'ClusterId' in job_ad:
        cluster_id = int(job_ad['ClusterId'])
    if 'ProcId' in job_ad:
        proc_id = int(job_ad['ProcId'])
    if 'JobStatus' in job_ad:
        job_status = int(job_ad['JobStatus'])
    if 'ProminenceIdentity' in job_ad:
        identity = job_ad['ProminenceIdentity']
    if 'ProminenceWantMPI' in job_ad:
        if job_ad['ProminenceWantMPI']:
            want_mpi = True
    if 'ProminenceJobUniqueIdentifier' in job_ad:
        uid = job_ad['ProminenceJobUniqueIdentifier']
        uid_raw = uid
    if 'ProminenceGroup' in job_ad:
        my_groups = job_ad['ProminenceGroup'].split(',')

    job_id = '%s.%s' % (cluster_id, proc_id)
    uid = "%s-%d" % (uid, proc_id)

    logging.info('[%s] Starting cloud_hook_translate_job', job_id)

    # Open JSON job description
    try:
        filename = '%s/.job.json' % iwd
        with open(filename, 'r') as json_file:
            job_json = json.load(json_file)
    except Exception as err:
        logging.error('[%s] Unable to open JSON job description due to: %s', job_id, err)
        sys.exit(1)

    if 'batch' in route:
        # Write out updated ClassAd to stdout
        classad_new['InfrastructureSite'] = route
        print(classad_new.printOld())
        logging.info('[%s] Exiting cloud_hook_translate_job in batch mode for route %s', job_id, route)
        sys.exit(0)
    elif job_status == 1:
        logging.info('[%s] Attempting to create cloud infrastructure', job_id)

        # Current time
        epoch = int(time.time())
        classad_new['ProminenceLastRouted'] = epoch

        # Current time
        epoch = int(time.time())
        classad_new['ProminenceInfrastructureEnteredCurrentStatus'] = epoch

        # Get appropriate RADL template depending on job type
        spacing_type = False
        if want_mpi:
            radl_file = CONFIG.get('templates', 'multi-node-mpi')
            spacing_type = True
        else:
            radl_file = CONFIG.get('templates', 'single-node')

        # Create credentials for the worker nodes
        (root_ca,
         proxy,
         signing_policy,
         mapfile,
         private_ssh_key_1,
         public_ssh_key_1,
         private_ssh_key_2,
         public_ssh_key_2) = create_worker_credentials(spacing_type)

        # Generate RADL based on existing templates
        num_total_cores = job_json['resources']['nodes']*job_json['resources']['cpus']
        num_worker_nodes = job_json['resources']['nodes'] - 1

        # POSIX mounts
        b2drop_app_username = None
        b2drop_app_password = None
        storage_mountpoint = None
        onedata_provider = None
        onedata_token = None
        add_mounts = ''
        if storage_mountpoint is not None:
            add_mounts = '-v /mnt%s:%s' % (storage_mountpoint, storage_mountpoint)
        logging.info('[%s] Using mounts="%s"', job_id, add_mounts)

        try:
            with open(radl_file) as data:
                radl_template = Template(data.read())
        except IOError as e: 
            logging.critical('[%s] Exiting due to IO error opening RADL template: %s', job_id, e)
            exit(1)
        except Exception as e:
            logging.critical('[%s] Exiting due to unexpected error opening RADL template: %s', job_id, e)
            exit(1)

        use_hostname = '%s-%d' % (uid, epoch)
    
        try:
            radl_contents = radl_template.substitute(cores_per_node=job_json['resources']['cpus'],
                                                     memory_per_node=job_json['resources']['memory'],
                                                     num_nodes=job_json['resources']['nodes'],
                                                     num_worker_nodes=num_worker_nodes,
                                                     num_total_cores=num_total_cores,
                                                     cluster=uid,
                                                     use_hostname=use_hostname,
                                                     disk_size=job_json['resources']['disk'],
                                                     job_id=cluster_id,
                                                     condor_host=condor_host,
                                                     root_ca=root_ca,
                                                     proxy=proxy,
                                                     signing_policy=signing_policy,
                                                     mapfile=mapfile,
                                                     private_ssh_key_1=private_ssh_key_1,
                                                     public_ssh_key_1=public_ssh_key_1,
                                                     private_ssh_key_2=private_ssh_key_2,
                                                     public_ssh_key_2=public_ssh_key_2,
                                                     b2drop_app_username=b2drop_app_username,
                                                     b2drop_app_password=b2drop_app_password,
                                                     storage_mount_point=storage_mountpoint,
                                                     onedata_provider=onedata_provider,
                                                     onedata_token=onedata_token,
                                                     storage_mounts=add_mounts)
        except KeyError as e:
            logging.critical('[%s] Exiting due to KeyError creating RADL template: %s', job_id, e)
            exit(1)
        except ValueError as e:
            logging.critical('[%s] Exiting due to ValueError creating RADL template: %s', job_id, e)
            exit(1)

        try:
            with open('/tmp/job-%s-%d.radl' % (job_id, epoch), 'w') as radl_write:
                radl_write.write(radl_contents)
        except IOError as e:
            logging.warning('[%s] Exiting due to IO error writing RADL template: %s', job_id, e)
        except Exception as e:
            logging.warning('[%s] Exiting due to unexpected error writing RADL template: %s', job_id, e)

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
        data['requirements']['regions'] = CONFIG.get('deployment', 'req-regions').split(',')
        data['requirements']['sites'] = CONFIG.get('deployment', 'req-sites').split(',')
        data['requirements']['groups'] = my_groups

        data['preferences'] = {}
        data['preferences']['regions'] = CONFIG.get('deployment', 'pref-regions').split(',')
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

        data['radl'] = base64.b64encode(radl_contents)
        data['identifier'] = job_id

        # Create infrastructure
        infra_id = create_infrastructure_with_retries(data)

        if infra_id is None:
            classad_new['ProminenceInfrastructureState'] = 'failed'
            logging.info('[%s] Deployment onto cloud failed', job_id)
        else:
            classad_new['ProminenceInfrastructureId'] = str('%s' % infra_id)
            classad_new['ProminenceInfrastructureState'] = 'deployment-init'
            classad_new['ProminenceWantCluster'] = uid
            classad_new['Requirements'] = classad.ExprTree('MY.ProminenceInfrastructureState =?= "configured"')
            classad_new['ProminenceProcId'] = str('%d' % proc_id)

            logging.info('[%s] Initiated infrastructure deployment with id "%s"', job_id, infra_id)

    # Write out updated ClassAd to stdout
    print(classad_new.printOld())

    # Write status file
    filename = '%s/status' % iwd
    try:
        with open(filename, 'w') as status_file:
            status_file.write('deploying')
    except Exception:
        logging.critical('[%s] Unable to write status file', job_id)

    logging.info('[%s] Exiting cloud_hook_translate_job', job_id)

if __name__ == "__main__":
    # Read config file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read('/etc/prominence/prominence.ini')

    # Logging
    logging.basicConfig(filename=CONFIG.get('logs', 'translate'),
                        level=logging.INFO,
                        format='%(asctime)s %(message)s')

    # Create infrastructure
    translate_classad()

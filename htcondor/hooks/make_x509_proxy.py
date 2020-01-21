import calendar
import time
import M2Crypto

def empty_callback1(p1):
    return

def empty_callback2(p1, p2):
    return

def make_x509_proxy(cert_path, key_path, expiration_time, is_legacy_proxy=False, cn=None):
    """
    Return a PEM-encoded limited proxy as a string in either Globus Legacy
    or RFC 3820 format. Checks that the existing cert/proxy expires after
    the given expiration_time, but no other checks are done.
    """
 
    # First get the existing priviate key
    try:
        old_key = M2Crypto.RSA.load_key(key_path, empty_callback1)
    except Exception as e:
        raise IOError('Failed to get private key from ' + key_path + ' (' + str(e) + ')')
 
    # Get the chain of certificates (just one if a usercert or hostcert file)
    try:
        cert_BIO = M2Crypto.BIO.File(open(cert_path))
    except Exception as e:
        raise IOError('Failed to open certificate file ' + cert_path + ' (' + str(e) + ')')
 
    old_certs = []
 
    while True:
        try:
            old_certs.append(M2Crypto.X509.load_cert_bio(cert_BIO))
        except:
            cert_BIO.close()
            break
 
    if len(old_certs) == 0:
        raise IOError('Failed get certificate from ' + cert_path)
 
    # Check the expiration_time
    if int(calendar.timegm(time.strptime(str(old_certs[0].get_not_after()), "%b %d %H:%M:%S %Y %Z"))) < expiration_time:
        raise IOError('Cert/proxy ' + cert_path + ' expires before given expiration time ' + str(expiration_time))
 
    # Create the public/private keypair for the new proxy
    new_key = M2Crypto.EVP.PKey()
    new_key.assign_rsa(M2Crypto.RSA.gen_key(1024, 65537, empty_callback2))
 
    # Start filling in the new certificate object
    new_cert = M2Crypto.X509.X509()
    new_cert.set_pubkey(new_key)
    new_cert.set_serial_number(int(time.time() * 100))
    new_cert.set_issuer_name(old_certs[0].get_subject())
    new_cert.set_version(2) # "2" is X.509 for "v3" ...
 
    # Construct the legacy or RFC style subject
    new_subject = old_certs[0].get_subject()
 
    if is_legacy_proxy:
        # Globus legacy proxy
        new_subject.add_entry_by_txt(field="CN",
                                     type=0x1001,
                                     entry='limited proxy',
                                     len=-1,
                                     loc=-1,
                                     set=0)
    elif cn:
        # RFC proxy, probably with machinetypeName as proxy CN
        new_subject.add_entry_by_txt(field="CN",
                                     type=0x1001,
                                     entry=cn,
                                     len=-1,
                                     loc=-1,
                                     set=0)
    else:
        # RFC proxy, with Unix time as CN
        new_subject.add_entry_by_txt(field="CN",
                                     type=0x1001,
                                     entry=str(int(time.time() * 100)),
                                     len=-1,
                                     loc=-1,
                                     set=0)
 
    new_cert.set_subject_name(new_subject)
 
    # Set start and finish times
    new_not_before = M2Crypto.ASN1.ASN1_UTCTIME()
    new_not_before.set_time(int(time.time()))
    new_cert.set_not_before(new_not_before)
 
    new_not_after = M2Crypto.ASN1.ASN1_UTCTIME()
    new_not_after.set_time(expiration_time)
    new_cert.set_not_after(new_not_after)
 
    # Add extensions, possibly including RFC-style proxyCertInfo
    new_cert.add_ext(M2Crypto.X509.new_extension("keyUsage", "Digital Signature, Key Encipherment, Key Agreement", 1))
 
    if not is_legacy_proxy:
        new_cert.add_ext(M2Crypto.X509.new_extension("proxyCertInfo", "critical, language:1.3.6.1.4.1.3536.1.1.1.9", 1, 0))

    # Sign the certificate with the old private key
    old_key_evp = M2Crypto.EVP.PKey()
    old_key_evp.assign_rsa(old_key)
    new_cert.sign(old_key_evp, 'sha256')
 
    # Return proxy as a string of PEM blocks
    proxy_string = new_cert.as_pem() + new_key.as_pem(cipher=None)
 
    for one_old_cert in old_certs:
        proxy_string += one_old_cert.as_pem()
 
    return proxy_string

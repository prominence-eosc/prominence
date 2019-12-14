def redact_storage_creds(storage):
    """
    Redact storage credentials
    """
    if 'b2drop' in storage:
        if 'app-username' in storage['b2drop']:
            storage['b2drop']['app-username'] = '***'
        if 'app-password' in storage['b2drop']:
            storage['b2drop']['app-password'] = '***'
    elif 'onedata' in storage:
        if 'provider' in storage['onedata']:
            storage['onedata']['provider'] = '***'
        if 'token' in storage['onedata']:
            storage['onedata']['token'] = '***'
    return storage

def condor_str(str_in):
    """
    Returns a double-quoted string
    """
    return str('"%s"' % str_in)

"""Miscellaneous utilities"""
def get_remote_addr(req):
    """
    Returns the remote IP address of a user
    """
    return req.environ.get('HTTP_X_REAL_IP', req.remote_addr)

def object_access_allowed(groups, path):
    """
    Decide if a user is allowed to access a path
    """
    for group in groups.split(','):
        if path.startswith(group):
            return True
    return False

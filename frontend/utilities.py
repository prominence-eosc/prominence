"""Miscellaneous utilities"""
def get_remote_addr(req):
    """
    Returns the remote IP address of a user
    """
    return req.environ.get('HTTP_X_REAL_IP', req.remote_addr)

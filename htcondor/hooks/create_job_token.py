import configparser
import time
import jwt

CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/prominence/prominence.ini')

def create_job_token(username, groups, lifetime, ui=''):
    """
    Create a jwt job token
    """
    return jwt.encode({"username": username, "groups": groups, "job": ui, "exp": int(time.time() + lifetime)},
                      CONFIG.get('credentials', 'job_token_secret'),
                      algorithm="HS256")

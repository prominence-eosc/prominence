from functools import wraps
import time

def retry(tries=4, delay=3, backoff=2):
    """
    Retry calling the decorated function using an exponential backoff
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                rv = f(*args, **kwargs)
                if not rv:
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                else:
                    return rv
            return f(*args, **kwargs)

        return f_retry

    return deco_retry

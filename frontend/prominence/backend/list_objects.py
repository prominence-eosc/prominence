import boto3

def get_matching_s3_objects(url, access_key_id, secret_access_key, bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket filtered by a prefix and/or suffix
    """
    s3 = boto3.client('s3',
                      endpoint_url=url,
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key)
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj

def list_objects(self, user, groups, path=None):
    """
    List objects in S3 storage
    """
    if path is None:
        prefix = 'uploads/%s' % user
        prefix_to_remove = ['uploads', user]
    else:
        prefix = 'uploads/%s' % path
        prefix_to_remove = ['uploads']

    objects = []

    try:
        keys = get_matching_s3_objects(self._config['S3_URL'],
                                       self._config['S3_ACCESS_KEY_ID'],
                                       self._config['S3_SECRET_ACCESS_KEY'],
                                       self._config['S3_BUCKET'],
                                       prefix=prefix)
    except Exception:
        return objects

    for key in keys:
        name = key['Key']
        pieces = name.split('/')
        for item in prefix_to_remove:
            pieces.remove(item)
        obj = {}
        obj['name'] = '/'.join(pieces)
        obj['size'] = key['Size']
        obj['lastModified'] = key['LastModified']
        objects.append(obj)

    return objects

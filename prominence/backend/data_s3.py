import boto3

def get_object_size(self, object_name):
    """
    Get the size of an object
    """
    s3_client = boto3.client('s3',
                             verify=False,
                             endpoint_url=self._config['S3_URL'],
                             aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                             aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])

    try:
        response = s3_client.head_object(Bucket=self._config['S3_BUCKET'], Key=object_name)
    except Exception:
        return None

    if 'ContentLength' in response:
        return response['ContentLength']

    return None

def create_presigned_url(self, method, object_name, duration_in_seconds=600):
    """
    Create presigned S3 URL
    """
    s3_client = boto3.client('s3',
                             verify=False,
                             endpoint_url=self._config['S3_URL'],
                             aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                             aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])
    if method == 'get':
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': self._config['S3_BUCKET'], 'Key': object_name},
                                                        ExpiresIn=duration_in_seconds)
        except Exception:
            return None
    else:
        try:
            response = s3_client.generate_presigned_url('put_object',
                                                        Params={'Bucket': self._config['S3_BUCKET'], 'Key':object_name},
                                                        ExpiresIn=duration_in_seconds,
                                                        HttpMethod='PUT')
        except Exception:
            return None

    return response

def get_matching_s3_objects(url, access_key_id, secret_access_key, bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket filtered by a prefix and/or suffix
    """
    s3 = boto3.client('s3',
                      verify=False,
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
        return None

    if keys is None:
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

def delete_object(self, username, group, obj):
    """
    Delete object from object storage
    """
    if '/' in obj:
        key = 'uploads/%s' % obj
    else:
        key = 'uploads/%s/%s' % (username, obj)

    try:
        s3_client = boto3.client('s3',
                                 verify=False,
                                 endpoint_url=self._config['S3_URL'],
                                 aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                                 aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])
        response = s3_client.delete_object(Bucket=self._config['S3_BUCKET'], Key=key)
    except Exception:
        return False

    return True

import boto3

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
                                 endpoint_url=self._config['S3_URL'],
                                 aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                                 aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])
        response = s3_client.delete_object(Bucket=self._config['S3_BUCKET'], Key=key)
    except Exception:
        return False

    return True

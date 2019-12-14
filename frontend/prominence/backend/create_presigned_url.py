import boto3

def create_presigned_url(self, method, bucket_name, object_name, duration_in_seconds=600):
    """
    Create presigned S3 URL
    """
    s3_client = boto3.client('s3',
                             endpoint_url=self._config['S3_URL'],
                             aws_access_key_id=self._config['S3_ACCESS_KEY_ID'],
                             aws_secret_access_key=self._config['S3_SECRET_ACCESS_KEY'])
    if method == 'get':
        try:
            response = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket_name, 'Key': object_name},
                                                        ExpiresIn=duration_in_seconds)
        except Exception:
            return None
    else:
        try:
            response = s3_client.generate_presigned_url('put_object',
                                                        Params={'Bucket':bucket_name, 'Key':object_name},
                                                        ExpiresIn=duration_in_seconds,
                                                        HttpMethod='PUT')
        except Exception:
            return None

    return response

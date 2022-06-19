try:
    from prominence.backend.data_s3 import get_object_size as s3_get_object_size, create_presigned_url as s3_create_presigned_url, list_objects as s3_list_objects, delete_object as s3_delete_object
except:
    from .data_s3 import get_object_size as s3_get_object_size, create_presigned_url as s3_create_presigned_url, list_objects as s3_list_objects, delete_object as s3_delete_object

try:
    from prominence.backend.data_azure import get_object_size as azure_get_object_size, create_presigned_url as azure_create_presigned_url, list_objects as azure_list_objects, delete_object as azure_delete_object
except:
    from .data_azure import get_object_size as azure_get_object_size, create_presigned_url as azure_create_presigned_url, list_objects as azure_list_objects, delete_object as azure_delete_object

def get_object_size(self, object_name):
    """
    Get the size of an object
    """
    if self._config['DEFAULT_STORAGE'] == 's3':
        return s3_get_object_size(self, object_name)
    elif self._config['DEFAULT_STORAGE'] == 'azure':
        return azure_get_object_size(self, object_name)

    return None, None

def create_presigned_url(self, method, object_name, duration_in_seconds=600, checksum=None):
    """
    Create presigned S3 URL
    """
    if self._config['DEFAULT_STORAGE'] == 's3':
        return s3_create_presigned_url(self, method, object_name, duration_in_seconds, checksum=checksum)
    elif self._config['DEFAULT_STORAGE'] == 'azure':
        return azure_create_presigned_url(self, method, object_name, duration_in_seconds, checksum=checksum)

    return None

def list_objects(self, user, groups, path=None):
    """
    List objects in S3 storage
    """
    if self._config['DEFAULT_STORAGE'] == 's3':
        return s3_list_objects(self, user, groups, path)
    elif self._config['DEFAULT_STORAGE'] == 'azure':
        return azure_list_objects(self, user, groups, path)

    return None

def delete_object(self, username, group, obj):
    """
    Delete object from object storage
    """
    if self._config['DEFAULT_STORAGE'] == 's3':
        return s3_delete_object(self, username, group, obj)
    elif self._config['DEFAULT_STORAGE'] == 'azure':
        return azure_delete_object(self, username, group, obj)

    return None

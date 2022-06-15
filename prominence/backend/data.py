import data_s3
import data_azure

def get_object_size(self, object_name):
    """
    Get the size of an object
    """
    if self._config['DEFAULT_STORAGE'] == 'azure':
        return data_s3.get_object_size(self, object_name)
    elif self._config['DEFAULT_STORAGE'] == 's3':
        return data_azure.get_object_size(self, object_name)

    return None

def create_presigned_url(self, method, object_name, duration_in_seconds=600):
    """
    Create presigned S3 URL
    """
    if self._config['DEFAULT_STORAGE'] == 'azure':
        return data_s3.create_presigned_url(self, method, object_name, duration_in_seconds)
    elif self._config['DEFAULT_STORAGE'] == 's3':
        return data_azure.create_presigned_url(self, method, object_name, duration_in_seconds)

    return None

def list_objects(self, user, groups, path=None):
    """
    List objects in S3 storage
    """
    if self._config['DEFAULT_STORAGE'] == 'azure':
        return data_s3.list_objects(self, user, groups, path)
    elif self._config['DEFAULT_STORAGE'] == 's3':
        return data_azure.list_objects(self, user, groups, path)

    return None

def delete_object(self, username, group, obj):
    """
    Delete object from object storage
    """
    if self._config['DEFAULT_STORAGE'] == 'azure':
        return data_s3.delete_object(self, username, group, obj)
    elif self._config['DEFAULT_STORAGE'] == 's3':
        return data_azure.delete_object(self, username, group, obj)

    return None

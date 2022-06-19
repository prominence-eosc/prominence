from datetime import datetime, timedelta
import time
from azure.storage.blob import ContainerClient, generate_blob_sas, BlobSasPermissions, BlobServiceClient

def get_object_size(self, object_name):
    """
    Get the size of an object
    """
    try:
        blob_service_client = BlobServiceClient(account_url=self._config['AZURE_ACCOUNT_NAME'],
                                                credential=self._config['AZURE_CREDENTIAL'])

        blob_client = blob_service_client.get_blob_client(container=self._config['AZURE_CONTAINER_NAME'],
                                                          blob=object_name)

        properties = blob_client.get_blob_properties()
    except:
        return None

    return properties.size, None

def create_presigned_url(self, method, object_name, duration_in_seconds=600, checksum=None):
    """
    Create presigned URL
    """
    if method == 'get':
        permission = BlobSasPermissions(read=True)
    else:
        permission = BlobSasPermissions(read=True, write=True, create=True)

    try:
        sas_blob = generate_blob_sas(account_name=self._config['AZURE_ACCOUNT_NAME'],
                                     container_name=self._config['AZURE_CONTAINER_NAME'],
                                     blob_name=object_name,
                                     account_key=self._config['AZURE_CREDENTIAL'],
                                     permission=permission,
                                     expiry=datetime.utcnow() + timedelta(seconds=duration_in_seconds))
    except Exception as err:
        print(err)
        return None

    return 'https://%s.blob.core.windows.net/%s/%s?%s' % (self._config['AZURE_ACCOUNT_NAME'],
                                                          self._config['AZURE_CONTAINER_NAME'],
                                                          object_name,
                                                          sas_blob)

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

    container = ContainerClient(account_url="https://%s.blob.core.windows.net" % self._config['AZURE_ACCOUNT_NAME'],
                                credential=self._config['AZURE_CREDENTIAL'],
                                container_name=self._config['AZURE_CONTAINER_NAME'])

    objects = []
    for blob in container.list_blobs(name_starts_with=prefix):
        pieces = blob.name.split('/')
        for item in prefix_to_remove:
            pieces.remove(item)

        objects.append({'name': '/'.join(pieces),
                        'size': blob.size,
                        'lastModified': blob.last_modified})

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
        container = ContainerClient(account_url="https://%s.blob.core.windows.net" % self._config['AZURE_ACCOUNT_NAME'],
                                    credential=self._config['AZURE_CREDENTIAL'],
                                    container_name=self._config['AZURE_CONTAINER_NAME'])

        container.delete_blob(key)
    except:
        return False

    return True

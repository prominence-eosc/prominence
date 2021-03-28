"""
API view for data management
"""
from rest_framework import status, views
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from rest_framework import status, permissions, views

from frontend.api.authentication import TokenAuthentication

from server.backend import ProminenceBackend
from server.set_groups import set_groups
import server.settings

def object_access_allowed(groups, path):
    """
    Decide if a user is allowed to access a path
    """
    for group in groups:
        if path.startswith(group):
            return True
    return False

class DataView(views.APIView):
    """
    API view for data management
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [JSONRenderer]

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request, path=None):
        """
        List objects
        """
        if not server.settings.CONFIG['ENABLE_DATA']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        groups = set_groups(request)

        if not path:
            objects = self._backend.list_objects(request.user.username)
            return Response(objects, status=status.HTTP_200_OK)
        else:
            path = str(path)

        if not object_access_allowed(groups, path):
            return Response({'error':'Not authorised to access this path'},
                            status=status.HTTP_403_FORBIDDEN)

        objects = self._backend.list_objects(request.user.username, path)
        return Response(objects, status=status.HTTP_200_OK)

    def post(self, request, path=None):
        """
        Upload objects
        """
        if not server.settings.CONFIG['ENABLE_DATA']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        if 'name' in request.query_params:
            object_name = request.args.get('name')
        elif 'filename' in request.data:
            object_name = request.data['filename']
        else:
            return Response({'error':'An object name must be specified'}, status=status.HTTP_400_BAD_REQUEST)

        groups = set_groups(request)

        if '/' in object_name:
            pieces = object_name.split('/')
            object_name_only = pieces[len(pieces) - 1]
            pieces.remove(object_name_only)
            file_group = '/'.join(pieces)
            if file_group not in groups:
                return Response({'error':'Not authorized to access upload with this path'},
                                status=status.HTTP_403_FORBIDDEN)
            url = self._backend.create_presigned_url('put',
                                                     server.settings.CONFIG['S3_BUCKET'],
                                                     'uploads/%s' % object_name)
        else:
            url = self._backend.create_presigned_url('put',
                                                     server.settings.CONFIG['S3_BUCKET'],
                                                     'uploads/%s/%s' % (request.user.username, object_name))

        return Response({'url':url}, status=status.HTTP_201_CREATED)

    def delete(self, request, obj=None):
        """
        Delete an object
        """
        if not server.settings.CONFIG['ENABLE_DATA']:
            return Response({'error': 'Functionality disabled by admin'},
                            status=status.HTTP_400_BAD_REQUEST)

        groups = set_groups(request)

        obj = str(obj)
        if '/' in obj:
            if not object_access_allowed(groups, obj):
                return Response({'error': 'Not authorized to access this object'},
                                 status=status.HTTP_403_FORBIDDEN)

        success = self._backend.delete_object(request.user.username, obj)
        return Response({}, status=status.HTTP_204_NO_CONTENT)


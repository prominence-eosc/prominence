"""
API view for health status
"""
from rest_framework import status, views
from rest_framework.response import Response

from server.backend import ProminenceBackend
import server.settings

class HealthView(views.APIView):
    """
    API view for getting health status information
    """

    def __init__(self, *args, **kwargs):
        self._backend = ProminenceBackend(server.settings.CONFIG)
        super().__init__(*args, **kwargs)

    def get(self, request):
        """
        Get health status
        """
        if not self._backend.get_health():
            return Response({}, status=status.HTTP_409_CONFLICT)

        return Response({}, status=status.HTTP_204_NO_CONTENT)

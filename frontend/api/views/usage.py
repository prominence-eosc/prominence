"""
API view for getting usage data
"""
from rest_framework import status, views
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer
from rest_framework import status, permissions, views

from frontend.api.authentication import TokenAuthentication

import server.settings
from frontend.metrics import ResourceUsage

class UsageView(views.APIView):
    """
    API view for providing usage data
    """
    authentication_classes = [TokenAuthentication]
    permission_classes = [permissions.IsAuthenticated]
    renderer_classes = [JSONRenderer]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, request):
        """
        Get usage data
        """
        show_users = True
        show_groups = False
        show_all_users = False

        if 'by_group' in request.query_params:
            if request.query_params.get('by_group') == 'true':
                show_groups = True
                show_users = False

        if 'show_all_users' in request.query_params:
            if request.query_params.get('show_all_users') == 'true':
                show_all_users = True

        if 'start' in request.query_params:
            start_date = request.query_params.get('start')
        else:
            return Response({'error': 'Start date missing'}, status=status.HTTP_400_BAD_REQUEST)

        if 'end' in request.query_params:
            end_date = request.query_params.get('end')
        else:
            return Response({'error': 'End date missing'}, status=status.HTTP_400_BAD_REQUEST)

        usage = ResourceUsage(server.settings.CONFIG)
        data = usage.get(request.user.username, group, start_date, end_date, show_users, show_all_users, show_groups)

        if data:
            return Response(data, status=status.HTTP_200_OK)

        return Response({'error': 'Error getting usage data'}, status=status.HTTP_400_BAD_REQUEST)

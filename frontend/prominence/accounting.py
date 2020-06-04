"""Route for obtaining usage data"""
from flask import Blueprint, jsonify, request
from flask import current_app as app

from auth import requires_auth
import errors
import usage
from utilities import get_remote_addr

accounting = Blueprint('accounting', __name__)

@accounting.route("/prominence/v1/accounting", methods=['GET'])
@requires_auth
def get_accounting(username, group, email):
    """
    Return usage data
    """
    app.logger.info('%s GetAccounting user:%s group:%s' % (get_remote_addr(request), username, group))

    show_users = True
    show_groups = False
    show_all_users = False

    if 'by_group' in request.args:
        if request.args.get('by_group') == 'true':
            show_groups = True
            show_users = False

    if 'show_all_users' in request.args:
        if request.args.get('show_all_users') == 'true':
            show_all_users = True

    if 'start' in request.args:
        start_date = request.args.get('start')
    else:
        return errors.start_date_missing()

    if 'end' in request.args:
        end_date = request.args.get('end')
    else:
        return errors.end_date_missing()

    data = usage.get_usage(username, group, start_date, end_date, show_users, show_all_users, show_groups, app.config)
    if data:
        return jsonify(data), 200

    return errors.usage_data_error()

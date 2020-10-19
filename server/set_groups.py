import re

def set_groups(request):
    groups = []
    for entitlement in request.user.entitlements.split(','):
        match = re.match(r'urn:mace:egi.eu:group:(\w+):role=member#aai.egi.eu', entitlement)
        if match:
            if match.group(1) not in groups:
                groups.append(match.group(1))

    # TODO: FOR TESTING ONLY
    groups = ["vo.access.egi.eu"]

    # If user not a member of any VOs, assign to a default group
    if not groups:
        groups.append('default')

    return groups

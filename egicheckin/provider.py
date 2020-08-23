from allauth.socialaccount.providers.base import ProviderAccount
from allauth.socialaccount.providers.oauth2.provider import OAuth2Provider


class EGICheckInAccount(ProviderAccount):
    def to_str(self):
        dflt = super(EGICheckInAccount, self).to_str()
        return self.account.extra_data.get('name', dflt)


class EGICheckInProvider(OAuth2Provider):
    id = 'egicheckin'
    name = 'EGICheckIn'
    account_class = EGICheckInAccount

    def get_auth_params(self, request, action):
        data = super(EGICheckInProvider, self).get_auth_params(request, action)
        return data

    def extract_uid(self, data):
        return str(data.get('id'))

    def extract_common_fields(self, data):
        return dict(
            email=data.get('email'),
            username=data.get('sub'),
            first_name=data.get('given_name'),
            last_name=data.get('family_name'),
            name=data.get('name')
        )


provider_classes = [EGICheckInProvider]

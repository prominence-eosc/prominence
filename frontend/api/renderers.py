from rest_framework import renderers

class PlainTextRenderer(renderers.BaseRenderer):
    media_type = 'text/plain'
    format = 'text'

    def render(self, data, media_type=None, renderer_context=None):
        return data.encode(self.charset)

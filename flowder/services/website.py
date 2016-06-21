import socket

from twisted.web import resource
from twisted.web.static import File
from twisted.application.service import IServiceCollection

from pygear.system.loading import load_object


class Root(resource.Resource):
    def __init__(self, app, config):
        resource.Resource.__init__(self)
        self.app = app
        self.IApp = IServiceCollection(app, app)
        self.debug = config.getboolean('debug', False)
        self.nodename = config.get('node_name', socket.gethostname())
        static_serve_path = config.get('static_serve_path', 'files')
        storage_path = config.get('storage_path')

        self.putChild('', Home(self))
        self.putChild('jobs', Jobs(self))
        self.putChild(static_serve_path, File(storage_path))

        services = config.items('services', ())
        for servName, servClsName in services:
            servCls = load_object(servClsName)
            self.putChild(servName, servCls(self))

    def request_permitted(self, request):
        # @TODO log not permitted requests
        return request.getClientIP() in self.app.trusted_clients.keys()

    @property
    def launcher(self):
        return self.IApp.getServiceNamed('launcher')

    @property
    def scheduler(self):
        return self.IApp.getServiceNamed('scheduler')

    @property
    def poller(self):
        app = IServiceCollection(self.app, self.app)
        return app.getServiceNamed('poller')


class Home(resource.Resource):
    def __init__(self, root):
        resource.Resource.__init__(self)
        self.root = root

    def render_GET(self, txrequest):
        raise NotImplementedError()


class Jobs(resource.Resource):
    def __init__(self, root):
        resource.Resource.__init__(self)
        self.root = root

    def render(self, txrequest):
        raise NotImplementedError()

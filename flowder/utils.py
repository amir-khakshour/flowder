import csv
import re
import netifaces as ni

from twisted.internet import defer
from twisted.names import client

from pygear.logging import log
from pygear.core.six.moves.urllib.parse import urlparse

from .interfaces import ITaskStorage

csv.register_dialect('pipes', delimiter='|')
client_callback_schemes = ['http', 'https']
default_scheme = 'http'

client_scheme_re = re.compile(r'^(%s)' % '|'.join(client_callback_schemes))
ip_re = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
ip_scheme_re = re.compile(r"^(%s)://(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})" % '|'.join(client_callback_schemes))


def get_tasks_db(config, app):
    return app.getComponent(ITaskStorage)


def prepare_url(url):
    if not client_scheme_re.match(url):
        url = default_scheme + url
    return url


def get_interface_ip(eth):
    eth = ni.ifaddresses(eth)
    return eth[2][0]['addr']


@defer.inlineCallbacks
def parse_clients_list(file_path):
    trusted_clients = None
    # @TODO create a service to read trusted clients from DB
    try:
        trusted_clients = open(file_path, 'r').readlines()
        trusted_clients = map(lambda c: c.replace('\n', ''), trusted_clients)
    except IOError:
        _clients = []
        log.warn("Trusted clinets list not found.")

    clients_list = {}
    if trusted_clients:
        for row in csv.reader(trusted_clients, dialect='pipes', quotechar='!'):
            _host, _user, _pass = row
            if ip_re.match(_host):
                _ip = _host
            else:
                _host = prepare_url(_host)
                parsed_url = urlparse.urlparse(_host)
                _ip = yield client.getHostByName(parsed_url.netloc)

            clients_list[_ip] = {'host': _host, 'user': _user, 'pass': _pass}
        defer.returnValue(clients_list)


def get_callback_auth_details(url, trusted_clients):
    match = ip_scheme_re.match(url)
    if not match or len(match.groups()) < 2:
        ip = client.getHostByName(url)
    else:
        scheme, ip = match.groups()

    for client_ip, details in trusted_clients.iteritems():
        if ip == client_ip:
            return details['user'], details['pass']
    return None


def get_serve_uri(config):
    rest_port = config.getint('rest_port', 4000)
    eth = config.get('eth', 'eth1')  # private IP
    rest_host = 'http://%s' % (get_interface_ip(eth))
    files_static_serve_path = config.get('static_serve_path', 'files')

    if rest_host.endswith('/'):
        rest_host = rest_host[:-1]

    base_url = '{0}:{1}/'.format(rest_host, rest_port)

    if not files_static_serve_path.endswith('/'):
        files_static_serve_path += '/'

    return urlparse.urljoin(base_url, files_static_serve_path)


def get_file_path(filename, base_path):
    pass
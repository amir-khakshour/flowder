from twisted.application import service

from scrapy.http.request import Request
from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.project import get_project_settings
from scrapy.core.downloader.handlers.http import HTTPDownloadHandler

from pygear.logging import log
from pygear.net.http import get_proxy
from pygear.core.six.moves.urllib.request import getproxies, proxy_bypass


class FetcherService(service.Service):
    name = 'fetcher'

    def __init__(self, config):
        self.settings = get_project_settings()
        self.settings.set('DOWNLOAD_MAXSIZE', config.get('max_file_size', 1024 * 1024 * 2))

        self.downloader = HTTPDownloadHandler(self.settings)
        self.proxies = {}
        self.valid_extensions = config.getlist('file_valid_extensions', "jpg, png")
        _proxies = config.items('proxy', ())
        for proxy_type, proxy in _proxies:
            self.proxies[proxy_type] = get_proxy(proxy, proxy_type)

    def startService(self):
        log.msg("Starting Fetcher service ...")

    def fetch(self, url):
        log.debug("Fetch URL %s" % url)
        request = Request(url=url)
        self.process_request(request)
        return mustbe_deferred(self.downloader.download_request, request, None)

    def process_request(self, request):
        request.meta['download_timeout'] = 60

        parsed = urlparse_cached(request)
        scheme = parsed.scheme
        # 'no_proxy' is only supported by http schemes
        if scheme in ('http', 'https') and proxy_bypass(parsed.hostname):
            return
        if scheme in self.proxies:
            self._set_proxy(request, scheme)

    def _set_proxy(self, request, scheme):
        creds, proxy = self.proxies[scheme]
        request.meta['proxy'] = proxy
        if creds:
            request.headers['Proxy-Authorization'] = 'Basic ' + creds
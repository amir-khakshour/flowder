import os
import time
import signal
from multiprocessing import cpu_count

from twisted.internet.defer import CancelledError
from twisted.internet import reactor, defer, threads
from twisted.application.service import Service, IService, IServiceCollection
from twisted.web._newclient import ResponseNeverReceived, ResponseFailed
from twisted.internet.error import TimeoutError, ConnectionRefusedError

from pygear.logging import log
from pygear.text.encoding import stringify_dict
from pygear.twisted.reactor import CallLaterOnce
from pygear.system.magic import get_buffer_extension
from pygear.core.six.moves.urllib.parse import urlparse
from pygear.twisted.signal import install_shutdown_handlers, signal_names

from .exceptions import NoResponseContent, InvalidResponseRetry
from .utils import get_serve_uri
from .amqp import amqp_message_decode
from flowder import __version__


class Launcher(Service):
    """
    Launching scheduled jobs
    """
    name = 'launcher'
    VALID_RESPONSE_EXT = ['.png', '.gif', '.jpeg', '.jpg', '.svg']

    def __init__(self, app, config):
        self.app = app
        self.threads = {}
        self.finished = []
        self.job_results = {}
        self.task_slots = {}
        self.max_proc = self._get_max_proc(config)
        self.storage_path = config.get('storage_path', '/tmp')
        self.serve_uri = get_serve_uri(config)
        self.max_retry = 10
        self.retry_counter = {}
        install_shutdown_handlers(self._signal_shutdown)
        self.all_threads_killed = CallLaterOnce(self._all_threads_killed)
        self.all_threads_killed.delay = 0
        self.default_callback_field = config.get('callback_field', 'price_img')

    def check_storage_path(self):
        if not os.path.exists(self.storage_path):
            try:
                os.makedirs(self.storage_path)
            except OSError as e:
                if e.errno == 13:
                    log.err("Can't create storage directory: access denied")
                else:
                    raise e

                reactor.callFromThread(self.stop)

    def startService(self):
        app = IServiceCollection(self.app, self.app)
        self.poller = app.getServiceNamed('poller')
        self.fetcher = app.getServiceNamed('fetcher')
        self.amqp = app.getServiceNamed('amqp')
        self.task_storage = app.getServiceNamed('task_storage')
        self.check_storage_path()

        for slot in range(self.max_proc):
            self._wait_for_project(slot)
        log.msg(format='Flowder %(version)s started: max_proc=%(max_proc)r',
                version=__version__, max_proc=self.max_proc, system='Launcher')

    def _wait_for_project(self, slot):
        self.poller.next().addCallback(self._spawn_thread, slot)

    def _spawn_thread(self, task_info, slot):
        task_info = stringify_dict(task_info, keys_only=False)
        job_id = task_info['job_id']
        self.task_slots[job_id] = slot
        self.run_task(slot, task_info)

    def fetch_if_new(self, result, task_info):
        job_id = task_info['job_id']
        if result:
            log.debug("Task Result already exists: %s" % job_id)
            file_name = result['result_url']
            self.task_storage.set_jobid_result_url(job_id, file_name)
            dfd = defer.maybeDeferred(self.publish_result, file_name, task_info)
        else:
            dfd = defer.maybeDeferred(self.fetcher.fetch, task_info['fetch_uri'])

            # get file response body
            dfd.addCallbacks(self.parse_response, self.failed,
                             callbackArgs=(job_id,), errbackArgs=(job_id,))

            # Save File
            dfd.addCallbacks(self.save_file_content, self.failed,
                             callbackArgs=(job_id,), errbackArgs=(job_id,))

            # Callback to URI
            dfd.addCallbacks(self.publish_result, self.failed,
                             callbackArgs=(task_info,), errbackArgs=(job_id,))

    def run_task(self, slot, task_info):
        job_id = task_info['job_id']

        log.debug("Running task: %s" % task_info)
        self.poller.set_task_running(job_id)

        dfd = self.poller.check_url_already_fetched(task_info['fetch_uri'])
        self.threads[slot] = dfd
        dfd.addCallback(self.fetch_if_new, task_info)
        dfd.addErrback(self.failed, job_id)

        dfd.addBoth(self._thread_finished, job_id)

    def publish_result(self, file_name, task_info):
        # Build result message to be published on AMQP
        message = dict()
        settings = amqp_message_decode(task_info['settings'])
        message['settings'] = settings
        message['timestamp'] = time.time()
        message['file_uri'] = urlparse.urljoin(self.serve_uri, file_name)
        self.amqp.publish(message)

    def save_file_content(self, content, job_id):
        # @TODO add new service to call periodically failed requests
        # to the callback_uri
        if not content:
            raise NoResponseContent("Response has no body!")
        ext = get_buffer_extension(content)
        if ext not in self.VALID_RESPONSE_EXT:
            raise InvalidResponseRetry("Invalid content type, retry!")

        file_name = '{name}{ext}'.format(name=job_id, ext=ext)
        log.debug("Save file: %s" % file_name)

        save_path = os.path.join(self.storage_path, file_name)
        with open(save_path, 'wb+') as file:
            file.write(content)

        # Save jobID result URL
        self.task_storage.set_jobid_result_url(job_id, file_name)
        return file_name

    def parse_response(self, response, job_id):
        if not response.body:
            raise NoResponseContent("Response has no body!")
        return response.body

    def job_failed(self, message, job_id):
        self.job_results[job_id] = ('FAILED', message)
        log.err(message)

    def job_failed_retry(self, message, job_id):
        # TODO add retry max number to prevent dogpile effect
        self.job_results[job_id] = ('RETRY', message)

        if job_id not in self.retry_counter:
            self.retry_counter[job_id] = 0
        else:
            self.retry_counter[job_id] += 1

        log.err(message)

    def failed(self, failure, job_id):
        if failure.check(CancelledError):
            self.job_failed("Response max size exceeded! job id: %s!" % job_id, job_id)

        elif failure.check(InvalidResponseRetry):
            ex = failure.value
            if job_id in self.retry_counter and self.retry_counter[job_id] == self.max_retry:
                self.job_failed("Max retry has been reached! job id: %s!" % job_id, job_id)
            else:
                self.job_failed_retry(ex.message, job_id)

        elif failure.check(ResponseNeverReceived):
            self.job_failed("No response from the server! job id: %s!" % job_id, job_id)

        elif failure.check(ResponseFailed):
            # @TODO add retry
            self.job_failed("Connection to server failed, retry .... %s!" % job_id, job_id)

        elif failure.check(NoResponseContent):
            self.job_failed("Response has no content .... %s!" % job_id, job_id)

        elif failure.check(TimeoutError):
            if job_id in self.retry_counter and self.retry_counter[job_id] == self.max_retry:
                self.job_failed("Max retry has been reached! job id: %s!" % job_id, job_id)
            else:
                self.job_failed_retry("Request timeout .... %s!" % job_id, job_id)
        elif failure.check(ConnectionRefusedError):
            if job_id in self.retry_counter and self.retry_counter[job_id] == self.max_retry:
                self.job_failed("Max retry has been reached! job id: %s!" % job_id, job_id)
            else:
                self.job_failed_retry("Connection refused .... %s!" % job_id, job_id)

        else:
            ex = failure.value
            self.job_failed("No proper failure found: %s, \n %s!" % (job_id, ex.message), job_id)
            failure.printTraceback()

    def _thread_finished(self, _, job_id):
        slot = self.task_slots[job_id]
        """
        When a Crawl process finishes her job,
        :param _:
        :param slot:
        :return:
        """
        log.debug("Task %s finished!" % job_id)

        def _do_cleanup(_, slot):
            thread = self.threads.pop(slot)
            self.finished.append(thread)
            # In case of shutdown
            self._wait_for_project(slot)  # add another

        if job_id in self.job_results.keys():
            result_type, result_message = self.job_results.pop(job_id)

            if result_type == 'FAILED':
                d = defer.maybeDeferred(self.poller.set_task_failed, job_id, result_message)
                if job_id in self.retry_counter:
                    del self.retry_counter[job_id]

            elif result_type == 'RETRY':
                d = defer.maybeDeferred(self.poller.set_task_retry, job_id, result_message)
            del self.job_results[job_id]
        else:
            d = defer.maybeDeferred(self.poller.set_task_succesfull, job_id, 'task finished successfully!')

        d.addBoth(_do_cleanup, slot)


    def _get_max_proc(self, config):
        max_proc = config.getint('max_proc', 1)
        if not max_proc:
            try:
                cpus = cpu_count()
            except NotImplementedError:
                cpus = 1
            max_proc = cpus * config.getint('max_proc_per_cpu', 4)
        return max_proc

    def _signal_shutdown(self, signum, _):
        install_shutdown_handlers(self._signal_kill)
        signame = signal_names[signum]
        log.msg(format="Received %(signame)s, shutting down gracefully. Send again to force ",
                level=log.INFO, signame=signame)
        reactor.callFromThread(self.stop)

    def _signal_kill(self, signum, _):
        install_shutdown_handlers(signal.SIG_IGN)
        signame = signal_names[signum]
        log.msg(format='Received %(signame)s twice, forcing unclean shutdown',
                level=log.INFO, signame=signame)
        reactor.callFromThread(self._stop_reactor)

    def all_services_stoped(self, result):
        for dfd in self.threads.values():
            dfd.cancel()
        self.all_threads_killed.schedule()

    def stop(self):
        self.poller.reset_all_tasks()
        d = IService(self.app).stopService()
        d.addCallback(self.all_services_stoped)

    def _all_threads_killed(self):
        if self.threads:
            return self.all_threads_killed.schedule()
        reactor.stop()

    @staticmethod
    def _stop_reactor(_=None):
        try:
            reactor.stop()
        except RuntimeError:  # raised if already stopped or in shutdown stage
            pass


import time
import uuid
import threading

from twisted.application import service
from twisted.internet import defer, reactor, protocol, task
from twisted.application.service import IServiceCollection
from twisted.internet.error import ConnectionRefusedError

from pika.adapters import twisted_connection
from pika.connection import ConnectionParameters
from pika import credentials as pika_credentials, BasicProperties
from pika.exceptions import AMQPError, ChannelClosed, ConnectionClosed

from pygear.logging import log
from pygear.twisted.signal import get_signal_manager

from flowder import signals
from flowder.amqp import amqp_message_encode, amqp_message_decode, amqp_message_update_meta
from flowder.exceptions import InvalidAMQPMessage, EncodingError


class AmqpService(service.Service):
    """
    Simple L{service.IService} which consumes an AMQP queue.
    :param settings an instance of miutils.config.BaseConfig
    :param consume_callback when got an item send the message to this callback
    """

    CONSUME_INTERVAL = 10
    name = 'amqp'

    def __init__(self, app, settings):
        self.IApp = IServiceCollection(app, app)
        self.signal_manager = get_signal_manager(app)

        self.settings = settings

        self._conn = None
        self.conn_parameters = None
        self._client = None
        self._channel = None
        self._stopping = None
        self._consumer_tag = None
        self._closing = False
        self._running = False
        self._publish_meta = None
        self._message_number_in = 0
        self._message_number_out = 0
        self._queue_in = None
        self._queue_out = None

        self._lock = threading.Lock()
        self._in_retry = dict()
        self._cached_messages = []
        self.conn_retry_interval = 0

        self.app_id = settings.get('app_id', 'fw0')
        self.consume_interval = settings.getint("consume_interval", 2, section='amqp')
        self.exchange_name = settings.get("exchange_name", 'flowder-ex', section='amqp')
        self.queue_in_name = settings.get("queue_in_name", 'flowder-in-queue', section='amqp')
        self.queue_in_routing_key = settings.get("queue_in_routing_key", 'flowder.in', section='amqp')
        self.queue_out_name = settings.get("queue_out_name", 'flowder-out-queue', section='amqp')
        self.queue_out_routing_key = settings.get("queue_out_routing_key", 'flowder.out', section='amqp')

    @property
    def scheduler(self):
        return self.IApp.getServiceNamed('scheduler')

    def startService(self):
        params = {}
        if self.settings.get("username", None, section='amqp') \
                and self.settings.get("pass", None, section='amqp'):
            params['credentials'] = pika_credentials.PlainCredentials(
                self.settings.get("username", None, section='amqp'),
                self.settings.get("pass", None, section='amqp')
            )

        if self.settings.getdict("params", dict(), section='amqp'):
            params.update(self.settings.getdict("params", dict(), section='amqp'))

        if self.settings.get("amqp_vhost", '/'):
            params.update({'virtual_host': self.settings.get("vhost", '/', section='amqp')})
        parameters = ConnectionParameters(**params)

        self._client = protocol.ClientCreator(
            reactor,
            twisted_connection.TwistedProtocolConnection,
            parameters)

        self.do_connect()

    def do_connect(self):
        d = self._client.connectTCP(
            self.settings.get("host", "localhost", section='amqp'),
            self.settings.getint("port", 5672, section='amqp')
        )
        d.addCallbacks(lambda protocol: protocol.ready, self.failed)
        d.addCallbacks(self.ready, self.failed)
        return d

    def failed(self, failure):
        if failure.check(ChannelClosed):
            self.retry_connect()
        elif failure.check(ConnectionClosed) or failure.check(ConnectionRefusedError):
            self.retry_connect()
        else:
            log.err("Unhandled failure in Amqp Service....")
            failure.printTraceback()
            reactor.stop()

    @defer.inlineCallbacks
    def ready(self, connection):
        self._in_retry['connection'] = False
        self._in_retry['channel'] = False
        if not connection:
            raise ConnectionClosed
        self._conn = connection
        log.msg("AMQP Connection created")

        if self._channel:
            yield self.check_connection()
        self._channel = yield self._conn.channel()

        self.conn_retry_interval = 0
        log.msg("Setting up exchange and queue")

        # Exchange
        yield self._channel.exchange_declare(
            exchange=self.exchange_name,
            type=self.settings.get("exchange_type", 'topic', section='amqp'),
            passive=True,
        )

        # Queue
        self._queue_in = yield self._channel.queue_declare(
            queue=self.queue_in_name,
            auto_delete=False,
            exclusive=False,
            durable=True,
        )
        self._queue_out = yield self._channel.queue_declare(
            queue=self.queue_out_name,
            auto_delete=False,
            exclusive=False,
            durable=True,
        )

        # Queue-in > Exchange
        yield self._channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_in_name,
            routing_key=self.queue_in_routing_key)

        yield self._channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_out_name,
            routing_key=self.queue_out_routing_key)

        yield self._channel.basic_qos(prefetch_count=1)

        self._running = True

        log.msg("Start Consuming %s" % self.queue_in_name)
        log.msg("Start Producing %s" % self.queue_out_name)

        # Consume queue_in queue
        queue_obj, consumer_tag = yield self._channel.basic_consume(queue=self.queue_in_name, no_ack=False)
        l = task.LoopingCall(self.read, queue_obj)
        l.start(0.01)

    @defer.inlineCallbacks
    def read(self, queue_obj):
        ch, method, properties, msg = yield queue_obj.get()

        msg = amqp_message_decode(msg)
        log.debug("Consuming msg %s" % msg)
        self._message_number_in += 1
        self.process_in_message(msg)
        time.sleep(self.consume_interval)
        yield ch.basic_ack(delivery_tag=method.delivery_tag)
        log.debug('Acknowledging message #%s' % self._message_number_in)

    @defer.inlineCallbacks
    def publish(self, message):
        self._message_number_out += 1

        amqp_message_update_meta(message, self.get_meta())
        amqp_msg = amqp_message_encode(message)
        log.debug("Publish message #%s, AMQP message: %s" % (self._message_number_out, amqp_msg))
        properties = BasicProperties(
            app_id=self.app_id,
            content_type='application/json',
            content_encoding='utf-8',
            delivery_mode=2,  # persistent
        )
        try:
            yield self._channel.basic_publish(
                self.exchange_name,
                self.queue_out_routing_key,
                amqp_msg,
                properties=properties,
            )
        except ChannelClosed:
            self.retry_channel()
            self._cached_messages.append(message)
        except AMQPError:
            self.retry_connect()
            self._cached_messages.append(message)

    def retry_connect(self):
        with self._lock:
            if 'connection' not in self._in_retry or not self._in_retry['connection']:
                self.conn_retry_interval += 2
                log.err("Connection Closed! retry connecting in %s seconds..." % self.conn_retry_interval)
                self._in_retry['connection'] = True
                d = task.deferLater(reactor, self.conn_retry_interval, self.do_connect)
                d.addErrback(self.failed)

    def retry_channel(self):
        with self._lock:
            if 'channel' not in self._in_retry or not self._in_retry['channel']:
                log.err("Channel Closed! retry creating it ...")
                self._in_retry['channel'] = True
                d = defer.maybeDeferred(self.ready, self._conn)
                d.addErrback(self.failed)

    def check_connection(self):
        d = defer.maybeDeferred(self.get_queue_size)
        d.addErrback(self.failed)

    @defer.inlineCallbacks
    def get_queue_size(self):
        queue = yield self._channel.queue_declare(
            queue=self.queue_out_name,
            passive=True
        )
        defer.returnValue(queue)

    def process_in_message(self, message):
        if 'fetch_uri' not in message:
            raise InvalidAMQPMessage('Given message has no fetch_uri value.')

        message_copy = message.copy()
        jobid = uuid.uuid1().hex
        _message = dict()
        _message['job_id'] = jobid
        _message['fetch_uri'] = message_copy.pop('fetch_uri')

        # Encode additional fields to send back to client on fetch
        try:
            _message['settings'] = amqp_message_encode(message_copy)
        except:
            # @TODO define a more specific exception handling here
            raise EncodingError("Can't encode message info. %s" % message)

        self.scheduler.schedule(_message)
        self.signal_manager.send_catch_log(signal=signals.request_received, jobid=jobid)

    def stopService(self):
        self._stopping = True
        if not self._cached_messages:
            return self.really_stop_service(None)
        log.warn("Cached messages found, try to publish them before shut down!")
        dfds = []
        for slot, c in enumerate(self._cached_messages):
            dfd = defer.maybeDeferred(self._publish, c, slot)
            dfds.append(dfd)

        dfd_list = defer.DeferredList(dfds)
        dfd_list.addBoth(self.really_stop_service)
        return dfd_list

    def really_stop_service(self, _):
        service.Service.stopService(self)

    def get_meta(self):
        if not self._publish_meta:
            self._publish_meta = {
                'app_id': self.app_id,
                'timestamp': time.time(),
            }
        return self._publish_meta
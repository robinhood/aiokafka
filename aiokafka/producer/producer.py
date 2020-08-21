import abc
import asyncio
import logging
import sys
import traceback
import warnings
from typing import Mapping, MutableMapping

from kafka.partitioner.default import DefaultPartitioner
from kafka.codec import has_gzip, has_snappy, has_lz4

from aiokafka.client import AIOKafkaClient
from aiokafka.errors import (
    MessageSizeTooLargeError, UnsupportedVersionError, IllegalOperation)
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.structs import TopicPartition
from aiokafka.util import (
    INTEGER_MAX_VALUE, PY_36, commit_structure_validate, get_running_loop
)

from .message_accumulator import MessageAccumulator
from .sender import Sender
from .transaction_manager import TransactionManager

log = logging.getLogger(__name__)

_missing = object()


class BaseProducer(abc.ABC):

    _PRODUCER_CLIENT_ID_SEQUENCE = 0

    _COMPRESSORS = {
        'gzip': (has_gzip, LegacyRecordBatchBuilder.CODEC_GZIP),
        'snappy': (has_snappy, LegacyRecordBatchBuilder.CODEC_SNAPPY),
        'lz4': (has_lz4, LegacyRecordBatchBuilder.CODEC_LZ4),
    }

    _closed = None  # Serves as an uninitialized flag for __del__
    _source_traceback = None

    def __init__(self, *, loop=None, bootstrap_servers='localhost',
                 client_id=None,
                 metadata_max_age_ms=300000, request_timeout_ms=40000,
                 api_version='auto', acks=_missing,
                 key_serializer=None, value_serializer=None,
                 compression_type=None, max_batch_size=16384,
                 partitioner=DefaultPartitioner(), max_request_size=1048576,
                 linger_ms=0, send_backoff_ms=100,
                 retry_backoff_ms=100, security_protocol="PLAINTEXT",
                 ssl_context=None, connections_max_idle_ms=540000,
                 on_irrecoverable_error=None,
                 enable_idempotence=False, transactional_id=None,
                 transaction_timeout_ms=60000, sasl_mechanism="PLAIN",
                 sasl_plain_password=None, sasl_plain_username=None,
                 sasl_kerberos_service_name='kafka',
                 sasl_kerberos_domain_name=None,
                 sasl_oauth_token_provider=None):
        if loop is None:
            loop = get_running_loop()

        if acks not in (0, 1, -1, 'all', _missing):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ('gzip', 'snappy', 'lz4', None):
            raise ValueError("Invalid compression type!")
        if compression_type:
            checker, compression_attrs = self._COMPRESSORS[compression_type]
            if not checker():
                raise RuntimeError("Compression library for {} not found"
                                   .format(compression_type))
        else:
            compression_attrs = 0
        self._compression_attrs = compression_attrs

        if acks is _missing:
            acks = 1
        elif acks == 'all':
            acks = -1

        AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE += 1
        if client_id is None:
            client_id = 'aiokafka-producer-%s' % \
                AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._metadata_max_age_ms = metadata_max_age_ms
        self._request_timeout_ms = request_timeout_ms
        self._api_version = api_version
        self._acks = acks
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._compression_type = compression_type
        self._max_batch_size = max_batch_size
        self._partitioner = partitioner
        self._max_request_size = max_request_size
        self._linger_ms = linger_ms
        self._send_backoff_ms = send_backoff_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._security_protocol = security_protocol
        self._ssl_context = ssl_context
        self._connections_max_idle_ms = connections_max_idle_ms
        self._transaction_timeout_ms = transaction_timeout_ms
        self._transaction_timeout_ms = transaction_timeout_ms
        self._on_irrecoverable_error = on_irrecoverable_error
        self._sasl_mechanism = sasl_mechanism
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._sasl_kerberos_service_name = sasl_kerberos_service_name
        self._sasl_kerberos_domain_name = sasl_kerberos_domain_name

        self.client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version, security_protocol=security_protocol,
            ssl_context=ssl_context,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider)
        self._metadata = self.client.cluster
        self._loop = loop
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False

    # Warn if producer was not closed properly
    # We don't attempt to close the Consumer, as __del__ is synchronous
    def __del__(self, _warnings=warnings):
        if self._closed is False:
            if PY_36:
                kwargs = {'source': self}
            else:
                kwargs = {}
            _warnings.warn("Unclosed AIOKafkaProducer {!r}".format(self),
                           ResourceWarning,
                           **kwargs)
            context = {'producer': self,
                       'message': 'Unclosed AIOKafkaProducer'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)

    @abc.abstractmethod
    def _on_set_api_version(self, api_version):
        ...

    @abc.abstractmethod
    def _message_accumulator_for(self, transactional_id, tp):
        ...

    @abc.abstractmethod
    def _transactional_id_or_default(self, transactional_id):
        ...

    @abc.abstractmethod
    def _verify_txn_started(self, transactional_id):
        ...

    @abc.abstractmethod
    def _wait_for_sender(self):
        ...

    @abc.abstractmethod
    def _ensure_transactional(self):
        ...

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, type, value, traceback):
        await self.stop()

    async def start(self):
        """Connect to Kafka cluster and check server version"""
        log.debug("Starting the Kafka producer")  # trace
        await self.client.bootstrap()
        if self._closed:
            return
        api_version = self.client.api_version

        self._verify_api_version(api_version)
        await self._start_sender()
        self._on_set_api_version(api_version)

        self._producer_magic = 0 if api_version < (0, 10) else 1
        log.debug("Kafka producer started")

    def _verify_api_version(self, api_version):
        if self._compression_type == 'lz4':
            assert self.client.api_version >= (0, 8, 2), \
                'LZ4 Requires >= Kafka 0.8.2 Brokers'

    async def stop(self):
        """Flush all pending data and close all connections to kafka cluster"""
        if self._closed:
            return
        self._closed = True
        self.client.set_close()

        await self._wait_for_sender()

        await self.client.close()
        log.debug("The Kafka producer has closed.")

    async def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        return (await self.client._wait_on_metadata(topic))

    def _serialize(self, topic, key, value):
        if self._key_serializer:
            serialized_key = self._key_serializer(key)
        else:
            serialized_key = key
        if self._value_serializer:
            serialized_value = self._value_serializer(value)
        else:
            serialized_value = value

        message_size = LegacyRecordBatchBuilder.record_overhead(
            self._producer_magic)
        if serialized_key is not None:
            message_size += len(serialized_key)
        if serialized_value is not None:
            message_size += len(serialized_value)
        if message_size > self._max_request_size:
            raise MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % message_size)

        return serialized_key, serialized_value

    def _partition(self, topic, partition, key, value,
                   serialized_key, serialized_value):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(topic), \
                'Unrecognized partition'
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self._partitioner(
            serialized_key, all_partitions, available)

    async def send(
        self, topic, value=None, key=None, partition=None,
        timestamp_ms=None, headers=None, transactional_id=None
    ):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
                See kafka compaction documentation for more details:
                http://kafka.apache.org/documentation.html#compaction
                (compaction requires kafka >= 0.8.1)
            partition (int, optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                'partitioner'.
            key (optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is None (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is None, partition is chosen randomly).
                Must be type bytes, or be serializable to bytes via configured
                key_serializer.
            timestamp_ms (int, optional): epoch milliseconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.

        Returns:
            asyncio.Future: object that will be set when message is
            processed

        Raises:
            kafka.KafkaTimeoutError: if we can't schedule this record (
                pending buffer is full) in up to `request_timeout_ms`
                milliseconds.

        Note:
            The returned future will wait based on `request_timeout_ms`
            setting. Cancelling the returned future **will not** stop event
            from being sent, but cancelling the ``send`` coroutine itself
            **will**.
        """
        assert value is not None or self.client.api_version >= (0, 8, 1), (
            'Null messages require kafka >= 0.8.1')
        assert not (value is None and key is None), \
            'Need at least one: key or value'
        transactional_id = self._transactional_id_or_default(transactional_id)

        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)

        # Ensure transaction is started and not committing
        self._verify_txn_started(transactional_id)

        if headers is not None:
            if self.client.api_version < (0, 11):
                raise UnsupportedVersionError(
                    "Headers not supported before Kafka 0.11")
        else:
            # Record parser/builder support only list type, no explicit None
            headers = []

        key_bytes, value_bytes = self._serialize(topic, key, value)
        partition = self._partition(topic, partition, key, value,
                                    key_bytes, value_bytes)

        tp = TopicPartition(topic, partition)
        log.debug("Sending (key=%s value=%s) to %s", key, value, tp)

        message_accumulator = self._message_accumulator_for(
            transactional_id, tp)
        fut = await message_accumulator.add_message(
            tp, key_bytes, value_bytes, self._request_timeout_ms / 1000,
            timestamp_ms=timestamp_ms, headers=headers)
        return fut

    async def send_and_wait(
        self, topic, value=None, key=None, partition=None,
        timestamp_ms=None, headers=None
    ):
        """Publish a message to a topic and wait the result"""
        future = await self.send(
            topic, value, key, partition, timestamp_ms, headers)
        return (await future)


class AIOKafkaProducer(BaseProducer):
    """A Kafka client that publishes records to the Kafka cluster.

    The producer consists of a pool of buffer space that holds records that
    haven't yet been transmitted to the server as well as a background task
    that is responsible for turning these records into requests and
    transmitting them to the cluster.

    The send() method is asynchronous. When called it adds the record to a
    buffer of pending record sends and immediately returns. This allows the
    producer to batch together individual records for efficiency.

    The 'acks' config controls the criteria under which requests are considered
    complete. The "all" setting will result in waiting for all replicas to
    respond, the slowest but most durable setting.

    The key_serializer and value_serializer instruct how to turn the key and
    value objects the user provides into bytes.

    Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the producer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client.
            Default: 'aiokafka-producer-#' (appended with a unique number
            per instance)
        key_serializer (callable): used to convert user-supplied keys to bytes
            If not None, called as f(key), should return bytes. Default: None.
        value_serializer (callable): used to convert user-supplied message
            values to bytes. If not None, called as f(value), should return
            bytes. Default: None.
        acks (0, 1, 'all'): The number of acknowledgments the producer requires
            the leader to have received before considering a request complete.
            This controls the durability of records that are sent. The
            following settings are common:

            0: Producer will not wait for any acknowledgment from the server
                at all. The message will immediately be added to the socket
                buffer and considered sent. No guarantee can be made that the
                server has received the record in this case, and the retries
                configuration will not take effect (as the client won't
                generally know of any failures). The offset given back for each
                record will always be set to -1.
            1: The broker leader will write the record to its local log but
                will respond without awaiting full acknowledgement from all
                followers. In this case should the leader fail immediately
                after acknowledging the record but before the followers have
                replicated it then the record will be lost.
            all: The broker leader will wait for the full set of in-sync
                replicas to acknowledge the record. This guarantees that the
                record will not be lost as long as at least one in-sync replica
                remains alive. This is the strongest available guarantee.

            If unset, defaults to *acks=1*. If ``enable_idempotence`` is
            ``True`` defaults to *acks=all*
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are 'gzip', 'snappy', 'lz4', or None.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: None.
        max_batch_size (int): Maximum size of buffered data per partition.
            After this amount `send` coroutine will block until batch is
            drained.
            Default: 16384
        linger_ms (int): The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, if first request is processed faster,
            than `linger_ms`, producer will wait `linger_ms - process_time`.
            This setting defaults to 0 (i.e. no delay).
        partitioner (callable): Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            partitioner(key_bytes, all_partitions, available_partitions).
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the Java client so that
            messages with the same key are assigned to the same partition.
            When a key is None, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        request_timeout_ms (int): Produce request timeout in milliseconds.
            As it's sent as part of ProduceRequest (it's a blocking call),
            maximum waiting time can be up to 2 * request_timeout_ms.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        api_version (str): specify which kafka API version to use.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. Directly passed into asyncio's
            `create_connection`_. For more information see :ref:`ssl_auth`.
            Default: None.
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9 minutes).
        enable_idempotence (bool): When set to ``True``, the producer will
            ensure that exactly one copy of each message is written in the
            stream. If ``False``, producer retries due to broker failures,
            etc., may write duplicates of the retried message in the stream.
            Note that enabling idempotence acks to set to 'all'. If it is not
            explicitly set by the user it will be chosen. If incompatible
            values are set, a ``ValueError`` will be thrown.
            New in version 0.5.0.
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
            PLAIN, GSSAPI. Default: PLAIN
        sasl_plain_username (str): username for sasl PLAIN authentication.
            Default: None
        sasl_plain_password (str): password for sasl PLAIN authentication.
            Default: None
        sasl_oauth_token_provider (kafka.oauth.abstract.AbstractTokenProvider):
            OAuthBearer token provider instance. (See kafka.oauth.abstract).
            Default: None

    Note:
        Many configuration parameters are taken from the Java client:
        https://kafka.apache.org/documentation.html#producerconfigs
    """

    def __init__(self, *,
                 loop,
                 bootstrap_servers='localhost',
                 acks=_missing,
                 enable_idempotence=False,
                 transactional_id=None,
                 transaction_timeout_ms=60000,
                 **kwargs):
        if transactional_id is not None:
            enable_idempotence = True
        else:
            transaction_timeout_ms = INTEGER_MAX_VALUE

        if enable_idempotence:
            if acks is _missing:
                acks = -1
            elif acks not in ('all', -1):
                raise ValueError(
                    "acks={} not supported if enable_idempotence=True"
                    .format(acks))
            self._txn_manager = TransactionManager(
                transactional_id, transaction_timeout_ms, loop=loop)
        else:
            self._txn_manager = None

        super().__init__(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            transaction_timeout_ms=transaction_timeout_ms,
            **kwargs,
        )

        self._message_accumulator = MessageAccumulator(
            self._metadata, self._max_batch_size, self._compression_attrs,
            self._request_timeout_ms / 1000, txn_manager=self._txn_manager,
            loop=self._loop)

        self._sender = Sender(
            self.client,
            acks=self._acks,
            txn_manager=self._txn_manager,
            retry_backoff_ms=self._retry_backoff_ms,
            linger_ms=self._linger_ms,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=self._request_timeout_ms,
            on_irrecoverable_error=self._on_irrecoverable_error,
            loop=self._loop,
        )

    def _on_set_api_version(self, api_version):
        self._message_accumulator.set_api_version(api_version)

    def _message_accumulator_for(self, transactional_id, tp):
        return self._message_accumulator

    def _transactional_id_or_default(self, transactional_id):
        if not transactional_id and self._txn_manager is not None:
            return self._txn_manager.transactional_id

    def _verify_txn_started(self, transactional_id):
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if txn_manager.transactional_id is not None and \
                    not self._txn_manager.is_in_transaction():
                raise IllegalOperation(
                    "Can't send messages while not in transaction")

    def _verify_api_version(self, api_version):
        super()._verify_api_version(api_version)
        if self._txn_manager is not None and self.client.api_version < (0, 11):
            raise UnsupportedVersionError(
                "Idempotent producer available only for Broker version 0.11"
                " and above")

    async def flush(self):
        """Wait untill all batches are Delivered and futures resolved"""
        await self._message_accumulator.flush()

    async def _wait_for_sender(self):
        # If the sender task is down there is no way for accumulator to flush
        if self._sender is not None and self._sender.sender_task is not None:
            await asyncio.wait([
                self._message_accumulator.close(),
                self._sender.sender_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self._loop)
        await self._sender.close()

    def _ensure_transactional(self):
        if self._txn_manager is None or \
                self._txn_manager.transactional_id is None:
            raise IllegalOperation(
                "You need to configure transaction_id to use transactions")

    async def _start_sender(self):
        await self._sender.start()

    async def begin_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Beginning a new transaction for id %s",
            self._txn_manager.transactional_id)
        await asyncio.shield(
            self._txn_manager.wait_for_pid(),
            loop=self._loop,
        )
        self._txn_manager.begin_transaction()

    async def commit_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Committing transaction for id %s",
            self._txn_manager.transactional_id)
        self._txn_manager.committing_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
            loop=self._loop,
        )

    async def abort_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Aborting transaction for id %s",
            self._txn_manager.transactional_id)
        self._txn_manager.aborting_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
            loop=self._loop,
        )

    def transaction(self):
        return TransactionContext(self)

    async def send_offsets_to_transaction(self, offsets, group_id):
        self._ensure_transactional()

        if not self._txn_manager.is_in_transaction():
            raise IllegalOperation("Not in the middle of a transaction")

        if not group_id or not isinstance(group_id, str):
            raise ValueError(group_id)

        # validate `offsets` structure
        formatted_offsets = commit_structure_validate(offsets)

        log.debug(
            "Begin adding offsets %s for consumer group %s to transaction",
            formatted_offsets, group_id)
        fut = self._txn_manager.add_offsets_to_txn(formatted_offsets, group_id)
        await asyncio.shield(fut, loop=self._loop)

    def create_batch(self):
        """Create and return an empty BatchBuilder.

        The batch is not queued for send until submission to ``send_batch``.

        Returns:
            BatchBuilder: empty batch to be filled and submitted by the caller.
        """
        return self._message_accumulator.create_builder()

    async def send_batch(self, batch, topic, *, partition):
        """Submit a BatchBuilder for publication.

        Arguments:
            batch (BatchBuilder): batch object to be published.
            topic (str): topic where the batch will be published.
            partition (int): partition where this batch will be published.

        Returns:
            asyncio.Future: object that will be set when the batch is
                delivered.
        """
        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)
        # We only validate we have the partition in the metadata here
        partition = self._partition(topic, partition, None, None, None, None)

        # Ensure transaction is started and not committing
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if txn_manager.transactional_id is not None and \
                    not self._txn_manager.is_in_transaction():
                raise IllegalOperation(
                    "Can't send messages while not in transaction")

        tp = TopicPartition(topic, partition)
        log.debug("Sending batch to %s", tp)
        future = await self._message_accumulator.add_batch(
            batch, tp, self._request_timeout_ms / 1000)
        return future


class TransactionContext:

    def __init__(self, producer):
        self._producer = producer

    async def __aenter__(self):
        await self._producer.begin_transaction()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            # If called directly we want the API to raise a InvalidState error,
            # but when exiting a context manager we should just let it out.
            if self._producer._txn_manager.is_fatal_error():
                return
            await self._producer.abort_transaction()
        else:
            await self._producer.commit_transaction()


class MultiTXNProducer(BaseProducer):

    _transactions: MutableMapping[str, TransactionManager]
    _accumulators: MutableMapping[str, MessageAccumulator]
    _senders: MutableMapping[str, Sender]
    _received_api_version: str = None

    def __init__(self, *,
                 loop,
                 bootstrap_servers='localhost',
                 acks=_missing,
                 enable_idempotence=False,
                 transactional_id=None,
                 transaction_timeout_ms=60000,
                 **kwargs):
        if acks is _missing:
            acks = -1
        elif acks not in ('all', -1):
            raise ValueError(
                "acks={} not supported with MultiTXNProducer"
                .format(acks))

        super().__init__(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            transaction_timeout_ms=transaction_timeout_ms,
            **kwargs,
        )
        self._transactions = {}
        self._accumulators = {}
        self._senders = {}

        self._message_accumulator = MessageAccumulator(
            self._metadata, self._max_batch_size, self._compression_attrs,
            self._request_timeout_ms / 1000,
            txn_manager=None,
            loop=self._loop)

        self._sender = Sender(
            self.client,
            txn_manager=None,
            acks=self._acks,
            retry_backoff_ms=self._retry_backoff_ms,
            linger_ms=self._linger_ms,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=self._request_timeout_ms,
            on_irrecoverable_error=self._on_irrecoverable_error,
            loop=self._loop,
        )

    async def commit(
        self,
        tid_to_offset_map: Mapping[str, Mapping[TopicPartition, int]],
        group_id: str,
        start_new_transaction: bool = True
    ) -> None:
        for transactional_id, offsets in tid_to_offset_map.items():
            log.debug('+COMMIT %r %r' % (transactional_id, offsets))
            await self._commit(
                transactional_id, offsets, group_id,
                start_new_transaction=start_new_transaction,
            )
            log.debug('-COMMIT %r %r' % (transactional_id, offsets))

    async def _commit(
        self, transactional_id, offsets: Mapping[TopicPartition, int],
        group_id: str,
        start_new_transaction: bool = True
    ) -> None:
        log.debug('+send offsets to transaction %r' % (transactional_id,))
        await self.send_offsets_to_transaction(
            transactional_id, offsets, group_id)
        log.debug('-send offsets to transaction %r' % (transactional_id,))
        log.debug('+commit transaction %r' % (transactional_id,))
        await self.commit_transaction(transactional_id)
        log.debug('-commit transaction %r' % (transactional_id,))
        if start_new_transaction:
            log.debug('+start new transaction %r' % (transactional_id,))
            await self.begin_transaction(transactional_id)
            log.debug('-start new transaction %r' % (transactional_id,))

    def _on_set_api_version(self, api_version):
        self._received_api_version = api_version
        for accumulator in self._accumulators.values():
            accumulator.set_api_version(api_version)
        self._message_accumulator.set_api_version(api_version)

    def _message_accumulator_for(self, transactional_id, tp):
        if transactional_id is None:
            return self._message_accumulator
        return self._accumulators[transactional_id]

    def _transactional_id_or_default(self, transactional_id):
        return None

    def _verify_txn_started(self, transactional_id):
        try:
            txn_manager = self._transactions[transactional_id]
        except KeyError:
            pass
        else:
            assert txn_manager.transactional_id == transactional_id
            if not txn_manager.is_in_transaction():
                raise IllegalOperation(
                    "Can't send messages while not in transaction")

    def _verify_api_version(self, api_version):
        super()._verify_api_version(api_version)
        if self.client.api_version < (0, 11):
            raise UnsupportedVersionError(
                "MultiTXNProducer available only for Broker version 0.11"
                " and above")

    async def flush(self):
        """Wait untill all batches are Delivered and futures resolved"""
        await asyncio.gather(
            self._message_accumulator.flush(),
            *[acc.flush() for acc in self._accumulators.values()],
        )

    async def _wait_for_sender(self):
        senders = self._senders
        accumulators = self._accumulators
        await asyncio.gather(
            self._wait_for_sender1(self._sender, self._message_accumulator),
            *[self._wait_for_sender1(senders[transactional_id], accumulator)
              for transactional_id, accumulator in accumulators.items()],
        )

    async def _wait_for_sender1(self, sender, accumulator):
        # If the sender task is down there is no way for accumulator to flush
        if sender is not None:
            if sender.sender_task is not None:
                futs = [sender.sender_task]
                if accumulator is not None:
                    futs.append(accumulator.close())
                await asyncio.wait(
                    futs,
                    return_when=asyncio.FIRST_COMPLETED,
                    loop=self._loop)
            await sender.close()

    def _ensure_transactional(self):
        ...

    async def _start_sender(self):
        await self._sender.start()

    async def _init_transaction(self, tid):
        txn_manager = self._transactions[tid] = TransactionManager(
            tid, self._transaction_timeout_ms, loop=self._loop,
        )
        accumulator = self._accumulators[tid] = MessageAccumulator(
            self._metadata,
            self._max_batch_size,
            self._compression_attrs,
            self._request_timeout_ms / 1000,
            txn_manager=txn_manager,
            loop=self._loop,
        )
        accumulator.set_api_version(self._received_api_version)
        sender = self._senders[tid] = Sender(
            self.client,
            acks=self._acks,
            txn_manager=txn_manager,
            retry_backoff_ms=self._retry_backoff_ms,
            linger_ms=self._linger_ms,
            message_accumulator=accumulator,
            request_timeout_ms=self._request_timeout_ms,
            on_irrecoverable_error=self._on_irrecoverable_error,
            loop=self._loop,
        )
        await sender.start()
        return txn_manager

    async def begin_transaction(self, transactional_id):
        log.debug(
            "Beginning a new transaction for id %s", transactional_id)
        txn_manager = self._transactions.get(transactional_id)
        if txn_manager is None:
            txn_manager = await self._init_transaction(transactional_id)

        await asyncio.shield(
            txn_manager.wait_for_pid(),
            loop=self._loop,
        )
        txn_manager.begin_transaction()

    async def commit_transaction(self, transactional_id):
        log.debug(
            "Committing transaction for id %s", transactional_id)
        txn_manager = self._transactions[transactional_id]
        txn_manager.committing_transaction()
        await asyncio.shield(
            txn_manager.wait_for_transaction_end(),
            loop=self._loop,
        )

    async def abort_transaction(self, transactional_id):
        log.debug(
            "Aborting transaction for id %s", transactional_id)
        txn_manager = self._transactions[transactional_id]
        txn_manager.aborting_transaction()
        await asyncio.shield(
            txn_manager.wait_for_transaction_end(),
            loop=self._loop,
        )

    async def stop_transaction(self, transactional_id):
        txn_manager = self._transactions.pop(transactional_id, None)
        accumulator = self._accumulators.pop(transactional_id, None)
        sender = self._senders.pop(transactional_id, None)
        if txn_manager is not None:
            if txn_manager.is_in_transaction():
                txn_manager.aborting_transaction()
                await asyncio.shield(
                    txn_manager.wait_for_transaction_end(),
                    loop=self._loop,
                )
        await self._wait_for_sender1(sender, accumulator)

    async def maybe_begin_transaction(self, transactional_id):
        txn_manager = self._transactions.get(transactional_id)
        if txn_manager is None:
            txn_manager = await self._init_transaction(transactional_id)
        else:
            if txn_manager.is_in_transaction():
                return
        log.debug(
            "Beginning a new transaction for id %s", transactional_id)
        await asyncio.shield(
            txn_manager.wait_for_pid(),
            loop=self._loop,
        )
        txn_manager.begin_transaction()

    async def send_offsets_to_transaction(
        self, transactional_id, offsets, group_id
    ):
        txn_manager = self._transactions[transactional_id]
        if not txn_manager.is_in_transaction():
            raise IllegalOperation("Not in the middle of a transaction")

        if not group_id or not isinstance(group_id, str):
            raise ValueError(group_id)

        # validate `offsets` structure
        formatted_offsets = commit_structure_validate(offsets)

        log.debug(
            "Begin adding offsets %s for consumer group %s to transaction",
            formatted_offsets, group_id)
        fut = txn_manager.add_offsets_to_txn(formatted_offsets, group_id)
        log.debug('+WAIT FOR RESPONSE OR ERROR %r' % (fut,))
        await asyncio.shield(fut, loop=self._loop)
        log.debug('-WAIT FOR RESPONSE OR ERROR %r' % (fut,))

from kafka.conn import collect_hosts as collect_hosts
from kafka.future import Future as Future
from kafka.structs import BrokerMetadata as BrokerMetadata, PartitionMetadata as PartitionMetadata, TopicPartition as TopicPartition
from typing import Any

log: Any

class ClusterMetadata:
    DEFAULT_CONFIG: Any = ...
    need_all_topic_metadata: bool = ...
    unauthorized_topics: Any = ...
    internal_topics: Any = ...
    controller: Any = ...
    config: Any = ...
    def __init__(self, **configs: Any) -> None: ...
    def is_bootstrap(self, node_id: Any): ...
    def brokers(self): ...
    def broker_metadata(self, broker_id: Any): ...
    def partitions_for_topic(self, topic: Any): ...
    def available_partitions_for_topic(self, topic: Any): ...
    def leader_for_partition(self, partition: Any): ...
    def partitions_for_broker(self, broker_id: Any): ...
    def coordinator_for_group(self, group: Any): ...
    def ttl(self): ...
    def refresh_backoff(self): ...
    def request_update(self): ...
    def topics(self, exclude_internal_topics: bool = ...): ...
    def failed_update(self, exception: Any) -> None: ...
    def update_metadata(self, metadata: Any): ...
    def add_listener(self, listener: Any) -> None: ...
    def remove_listener(self, listener: Any) -> None: ...
    def add_group_coordinator(self, group: Any, response: Any): ...
    def with_partitions(self, partitions_to_add: Any): ...

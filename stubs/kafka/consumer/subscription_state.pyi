import abc
from kafka.errors import IllegalStateError as IllegalStateError
from kafka.protocol.offset import OffsetResetStrategy as OffsetResetStrategy
from kafka.structs import OffsetAndMetadata as OffsetAndMetadata
from typing import Any, Optional

log: Any

class SubscriptionState:
    subscription: Any = ...
    subscribed_pattern: Any = ...
    assignment: Any = ...
    listener: Any = ...
    needs_fetch_committed_offsets: bool = ...
    def __init__(self, offset_reset_strategy: str = ...) -> None: ...
    def subscribe(self, topics: Any = ..., pattern: Optional[Any] = ..., listener: Optional[Any] = ...) -> None: ...
    def change_subscription(self, topics: Any) -> None: ...
    def group_subscribe(self, topics: Any) -> None: ...
    def reset_group_subscription(self) -> None: ...
    def assign_from_user(self, partitions: Any) -> None: ...
    def assign_from_subscribed(self, assignments: Any) -> None: ...
    def unsubscribe(self) -> None: ...
    def group_subscription(self): ...
    def seek(self, partition: Any, offset: Any) -> None: ...
    def assigned_partitions(self): ...
    def paused_partitions(self): ...
    def fetchable_partitions(self): ...
    def partitions_auto_assigned(self): ...
    def all_consumed_offsets(self): ...
    def need_offset_reset(self, partition: Any, offset_reset_strategy: Optional[Any] = ...) -> None: ...
    def has_default_offset_reset_policy(self): ...
    def is_offset_reset_needed(self, partition: Any): ...
    def has_all_fetch_positions(self): ...
    def missing_fetch_positions(self): ...
    def is_assigned(self, partition: Any): ...
    def is_paused(self, partition: Any): ...
    def is_fetchable(self, partition: Any): ...
    def pause(self, partition: Any) -> None: ...
    def resume(self, partition: Any) -> None: ...

class TopicPartitionState:
    committed: Any = ...
    has_valid_position: bool = ...
    paused: bool = ...
    awaiting_reset: bool = ...
    reset_strategy: Any = ...
    highwater: Any = ...
    drop_pending_message_set: bool = ...
    last_offset_from_message_batch: Any = ...
    def __init__(self) -> None: ...
    position: Any = ...
    def await_reset(self, strategy: Any) -> None: ...
    def seek(self, offset: Any) -> None: ...
    def pause(self) -> None: ...
    def resume(self) -> None: ...
    def is_fetchable(self): ...

class ConsumerRebalanceListener(metaclass=abc.ABCMeta):
    __metaclass__: Any = ...
    @abc.abstractmethod
    def on_partitions_revoked(self, revoked: Any) -> Any: ...
    @abc.abstractmethod
    def on_partitions_assigned(self, assigned: Any) -> Any: ...

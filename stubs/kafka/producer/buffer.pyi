from kafka.metrics.stats import Rate as Rate
from typing import Any, Optional

class SimpleBufferPool:
    wait_time: Any = ...
    def __init__(self, memory: Any, poolable_size: Any, metrics: Optional[Any] = ..., metric_group_prefix: str = ...) -> None: ...
    def allocate(self, size: Any, max_time_to_block_ms: Any): ...
    def deallocate(self, buf: Any) -> None: ...
    def queued(self): ...

import abc
from kafka.protocol.api import Request as Request, Response as Response
from kafka.protocol.types import Array as Array, Bytes as Bytes, Int16 as Int16, Int32 as Int32, Int64 as Int64, Schema as Schema, String as String
from typing import Any

class ProduceResponse_v0(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceResponse_v1(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceResponse_v2(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceResponse_v3(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceResponse_v4(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceResponse_v5(Response):
    API_KEY: int = ...
    API_VERSION: int = ...
    SCHEMA: Any = ...

class ProduceRequest(Request, metaclass=abc.ABCMeta):
    API_KEY: int = ...
    def expect_response(self): ...

class ProduceRequest_v0(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

class ProduceRequest_v1(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

class ProduceRequest_v2(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

class ProduceRequest_v3(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

class ProduceRequest_v4(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

class ProduceRequest_v5(ProduceRequest):
    API_VERSION: int = ...
    RESPONSE_TYPE: Any = ...
    SCHEMA: Any = ...

ProduceResponse: Any

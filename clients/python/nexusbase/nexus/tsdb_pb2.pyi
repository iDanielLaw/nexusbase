from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PutRequest(_message.Message):
    __slots__ = ("metric", "tags", "timestamp", "fields")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    fields: _struct_pb2.Struct
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[int] = ..., fields: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PutBatchRequest(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[PutRequest]
    def __init__(self, points: _Optional[_Iterable[_Union[PutRequest, _Mapping]]] = ...) -> None: ...

class PutBatchResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("metric", "tags", "timestamp")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("fields",)
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _struct_pb2.Struct
    def __init__(self, fields: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("metric", "tags", "timestamp")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteSeriesRequest(_message.Message):
    __slots__ = ("metric", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class DeleteSeriesResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeletesByTimeRangeRequest(_message.Message):
    __slots__ = ("metric", "tags", "start_time", "end_time")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    start_time: int
    end_time: int
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., start_time: _Optional[int] = ..., end_time: _Optional[int] = ...) -> None: ...

class DeletesByTimeRangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryRequest(_message.Message):
    __slots__ = ("metric", "start_time", "end_time", "tags", "aggregation_specs", "downsample_interval", "emit_empty_windows", "limit")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    AGGREGATION_SPECS_FIELD_NUMBER: _ClassVar[int]
    DOWNSAMPLE_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    EMIT_EMPTY_WINDOWS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    metric: str
    start_time: int
    end_time: int
    tags: _containers.ScalarMap[str, str]
    aggregation_specs: _containers.RepeatedCompositeFieldContainer[AggregationSpec]
    downsample_interval: str
    emit_empty_windows: bool
    limit: int
    def __init__(self, metric: _Optional[str] = ..., start_time: _Optional[int] = ..., end_time: _Optional[int] = ..., tags: _Optional[_Mapping[str, str]] = ..., aggregation_specs: _Optional[_Iterable[_Union[AggregationSpec, _Mapping]]] = ..., downsample_interval: _Optional[str] = ..., emit_empty_windows: bool = ..., limit: _Optional[int] = ...) -> None: ...

class AggregationSpec(_message.Message):
    __slots__ = ("function", "field")
    class AggregationFunc(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NONE: _ClassVar[AggregationSpec.AggregationFunc]
        SUM: _ClassVar[AggregationSpec.AggregationFunc]
        COUNT: _ClassVar[AggregationSpec.AggregationFunc]
        AVERAGE: _ClassVar[AggregationSpec.AggregationFunc]
        MIN: _ClassVar[AggregationSpec.AggregationFunc]
        MAX: _ClassVar[AggregationSpec.AggregationFunc]
        FIRST: _ClassVar[AggregationSpec.AggregationFunc]
        LAST: _ClassVar[AggregationSpec.AggregationFunc]
    NONE: AggregationSpec.AggregationFunc
    SUM: AggregationSpec.AggregationFunc
    COUNT: AggregationSpec.AggregationFunc
    AVERAGE: AggregationSpec.AggregationFunc
    MIN: AggregationSpec.AggregationFunc
    MAX: AggregationSpec.AggregationFunc
    FIRST: AggregationSpec.AggregationFunc
    LAST: AggregationSpec.AggregationFunc
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    function: AggregationSpec.AggregationFunc
    field: str
    def __init__(self, function: _Optional[_Union[AggregationSpec.AggregationFunc, str]] = ..., field: _Optional[str] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ("metric", "tags", "timestamp", "fields", "window_start_time", "window_end_time", "aggregated_values", "is_aggregated")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class AggregatedValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_START_TIME_FIELD_NUMBER: _ClassVar[int]
    WINDOW_END_TIME_FIELD_NUMBER: _ClassVar[int]
    AGGREGATED_VALUES_FIELD_NUMBER: _ClassVar[int]
    IS_AGGREGATED_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    fields: _struct_pb2.Struct
    window_start_time: int
    window_end_time: int
    aggregated_values: _containers.ScalarMap[str, float]
    is_aggregated: bool
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[int] = ..., fields: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., window_start_time: _Optional[int] = ..., window_end_time: _Optional[int] = ..., aggregated_values: _Optional[_Mapping[str, float]] = ..., is_aggregated: bool = ...) -> None: ...

class CreateSnapshotRequest(_message.Message):
    __slots__ = ("snapshot_dir",)
    SNAPSHOT_DIR_FIELD_NUMBER: _ClassVar[int]
    snapshot_dir: str
    def __init__(self, snapshot_dir: _Optional[str] = ...) -> None: ...

class CreateSnapshotResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetSeriesByTagsRequest(_message.Message):
    __slots__ = ("metric", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class GetSeriesByTagsResponse(_message.Message):
    __slots__ = ("series_keys",)
    SERIES_KEYS_FIELD_NUMBER: _ClassVar[int]
    series_keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, series_keys: _Optional[_Iterable[str]] = ...) -> None: ...

class SubscribeRequest(_message.Message):
    __slots__ = ("metric", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    metric: str
    tags: _containers.ScalarMap[str, str]
    def __init__(self, metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class DataPointUpdate(_message.Message):
    __slots__ = ("update_type", "metric", "tags", "timestamp", "value")
    class UpdateType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PUT: _ClassVar[DataPointUpdate.UpdateType]
        DELETE: _ClassVar[DataPointUpdate.UpdateType]
        DELETE_SERIES: _ClassVar[DataPointUpdate.UpdateType]
        DELETE_RANGE: _ClassVar[DataPointUpdate.UpdateType]
    PUT: DataPointUpdate.UpdateType
    DELETE: DataPointUpdate.UpdateType
    DELETE_SERIES: DataPointUpdate.UpdateType
    DELETE_RANGE: DataPointUpdate.UpdateType
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    UPDATE_TYPE_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    update_type: DataPointUpdate.UpdateType
    metric: str
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    value: float
    def __init__(self, update_type: _Optional[_Union[DataPointUpdate.UpdateType, str]] = ..., metric: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., timestamp: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...

class ForceFlushRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ForceFlushResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

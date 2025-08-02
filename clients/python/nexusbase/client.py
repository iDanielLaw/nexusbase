import grpc
import grpc.aio
from typing import Any, Callable
from datetime import datetime, timezone
from google.protobuf.struct_pb2 import Struct

# Import โค้ดที่ generate มา
from .nexus import tsdb_pb2, tsdb_pb2_grpc

def _datetime_to_nanos(dt: datetime) -> int:
    """แปลง datetime object เป็น nanoseconds since epoch."""
    return int(dt.timestamp() * 1_000_000_000)

class _AsyncAuthTokenInterceptor(grpc.aio.ClientInterceptor):
    """Interceptor to add an authentication token to every gRPC call."""
    def __init__(self, token: str):
        self._token = f'Bearer {token}'

    async def _intercept(self,
                         continuation: Callable,
                         client_call_details: grpc.aio.ClientCallDetails,
                         request_or_iterator: Any) -> grpc.aio.Call:
        """Helper to add auth metadata to a call."""
        metadata: list[tuple[str, str]] = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        metadata.append(('authorization', self._token))
        new_details = client_call_details._replace(metadata=tuple(metadata))

        return await continuation(new_details, request_or_iterator)

    async def intercept_unary_unary(self, continuation: Callable, client_call_details: grpc.aio.ClientCallDetails, request: Any) -> grpc.aio.Call:
        return await self._intercept(continuation, client_call_details, request)

    async def intercept_unary_stream(self, continuation: Callable, client_call_details: grpc.aio.ClientCallDetails, request: Any) -> grpc.aio.Call:
        return await self._intercept(continuation, client_call_details, request)

class NexusBaseClient:
    """
    Client Library สำหรับเชื่อมต่อกับ NexusBase TSDB ผ่าน gRPC
    """
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 50051,
                 use_tls: bool = False,
                 ca_cert_path: str = None,
                 server_hostname: str = 'localhost',
                 auth_token: str = None):
        """
        สร้างการเชื่อมต่อ gRPC ไปยังเซิร์ฟเวอร์
        :param host: Host ของเซิร์ฟเวอร์
        :param port: Port ของเซิร์ฟเวอร์
        :param use_tls: ตั้งค่าเป็น True เพื่อใช้การเชื่อมต่อแบบปลอดภัย (TLS)
        :param ca_cert_path: (จำเป็นเมื่อ use_tls=True) ตำแหน่งของไฟล์ CA certificate (.crt)
        :param server_hostname: (ทางเลือก) ชื่อโฮสต์ของเซิร์ฟเวอร์ที่ใช้ในการตรวจสอบใบรับรอง
        :param auth_token: (ทางเลือก) Token สำหรับการยืนยันตัวตน (e.g., JWT) ที่จะถูกส่งไปใน Authorization header
        """
        self.target = f'{host}:{port}'

        channel = None
        if use_tls:
            if not ca_cert_path:
                raise ValueError("ต้องระบุ 'ca_cert_path' เมื่อเปิดใช้งาน TLS")
            try:
                with open(ca_cert_path, 'rb') as f:
                    root_certs = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(f"ไม่พบไฟล์ CA certificate ที่: {ca_cert_path}")

            credentials = grpc.ssl_channel_credentials(root_certificates=root_certs)
            # Override the target name for certificate validation if a specific hostname is provided
            options = (('grpc.ssl_target_name_override', server_hostname),)
            channel = grpc.aio.secure_channel(self.target, credentials, options=options)
            print(f"กำลังเชื่อมต่อไปยัง NexusBase ที่ {self.target} ผ่าน TLS...")
        else:
            channel = grpc.aio.insecure_channel(self.target)
            print(f"กำลังเชื่อมต่อไปยัง NexusBase ที่ {self.target} (insecure)...")

        if auth_token:
            interceptor = _AsyncAuthTokenInterceptor(auth_token)
            self.channel = grpc.aio.intercept_channel(channel, interceptor)
            print("เปิดใช้งานการยืนยันตัวตนด้วย Token")
        else:
            self.channel = channel

        self.stub = tsdb_pb2_grpc.TSDBServiceStub(self.channel)

    async def put(self, metric: str, fields: dict, tags: dict = None, ts: datetime = None):
        """
        เขียนข้อมูลหนึ่งจุดลงในฐานข้อมูล

        :param metric: ชื่อของเมตริก (e.g., 'cpu.usage')
        :param fields: Dictionary ของข้อมูล (e.g., {'value': 85.5, 'core': '1'})
        :param tags: Dictionary ของแท็ก (e.g., {'host': 'server-1'})
        :param ts: เวลาของข้อมูล (datetime object), ถ้าไม่ระบุจะใช้เวลาปัจจุบัน
        """
        if ts is None:
            ts = datetime.now(timezone.utc)
        
        # แปลง Python dict เป็น Protobuf Struct
        fields_struct = Struct()
        fields_struct.update(fields)

        request = tsdb_pb2.PutRequest(
            metric=metric,
            tags=tags or {},
            timestamp=_datetime_to_nanos(ts),
            fields=fields_struct
        )
        try:
            await self.stub.Put(request)
        except grpc.RpcError as e:
            raise

    async def query(self, metric: str, start_time: datetime, end_time: datetime, tags: dict = None, limit: int = 0):
        """
        ค้นหาข้อมูลดิบ (Raw Data) จากฐานข้อมูล

        :param metric: ชื่อของเมตริก
        :param start_time: เวลาเริ่มต้น (datetime object)
        :param end_time: เวลาสิ้นสุด (datetime object)
        :param tags: Dictionary ของแท็กสำหรับกรองข้อมูล
        :param limit: จำนวนผลลัพธ์สูงสุดที่ต้องการ
        :return: Generator ที่ yield ผลลัพธ์เป็น dictionary
        """
        request = tsdb_pb2.QueryRequest(
            metric=metric,
            tags=tags or {},
            start_time=_datetime_to_nanos(start_time),
            end_time=_datetime_to_nanos(end_time),
            limit=limit
        )
        try:
            async for response in self.stub.Query(request):
                result = {
                    'metric': response.metric,
                    'tags': dict(response.tags),
                }
                if response.is_aggregated:
                    # Handle aggregated point
                    result.update({
                        'type': 'aggregated_point',
                        'window_start_time': datetime.fromtimestamp(response.window_start_time / 1_000_000_000, tz=timezone.utc),
                        'window_end_time': datetime.fromtimestamp(response.window_end_time / 1_000_000_000, tz=timezone.utc),
                        'aggregated_values': dict(response.aggregated_values)
                    })
                else:
                    # Handle raw data point
                    point_fields = {k: v for k, v in response.fields.items()}
                    result.update({
                        'type': 'point',
                        'timestamp': datetime.fromtimestamp(response.timestamp / 1_000_000_000, tz=timezone.utc),
                        'fields': point_fields
                    })
                yield result
        except grpc.RpcError as e:
            raise

    async def get_series_by_tags(self, metric: str = None, tags: dict = None) -> list[str]:
        """
        ค้นหารายชื่อ Series ทั้งหมดที่ตรงตามเงื่อนไข

        :param metric: (ทางเลือก) ชื่อเมตริกสำหรับกรอง
        :param tags: (ทางเลือก) Dictionary ของแท็กสำหรับกรอง
        :return: List ของ series keys
        """
        request = tsdb_pb2.GetSeriesByTagsRequest(
            metric=metric or "",
            tags=tags or {}
        )
        try:
            response = await self.stub.GetSeriesByTags(request)
            return list(response.series_keys)
        except grpc.RpcError as e:
            raise

    async def close(self):
        """ปิดการเชื่อมต่อ gRPC"""
        if self.channel:
            await self.channel.close()
            print("ปิดการเชื่อมต่อแล้ว")

import asyncio
from datetime import datetime, timedelta, timezone
from nexusbase.client import NexusBaseClient # Assuming the refactored client is in client.py
import grpc

async def run_async_example():
    # 1. สร้าง Client และเชื่อมต่อ
    # การสร้าง client ไม่ใช่ async, แต่การใช้งาน method ต่างๆ เป็น async
    client = NexusBaseClient(host="10.1.1.1")

    try:
        # 2. เขียนข้อมูล (Put)
        print("\n--- กำลังเขียนข้อมูล (async) ---")
        metric_name = "server.performance.async"
        
        # สร้าง tasks สำหรับการเขียนข้อมูลพร้อมกัน
        put_task1 = client.put(
            metric=metric_name,
            tags={'host': 'web-01-async', 'region': 'us-east'},
            fields={'cpu_usage': 80.1, 'memory_mb': 2048},
            ts=datetime.now(timezone.utc) - timedelta(minutes=2)
        )
        
        put_task2 = client.put(
            metric=metric_name,
            tags={'host': 'db-01-async', 'region': 'eu-west'},
            fields={'cpu_usage': 45.5, 'memory_mb': 8192},
            ts=datetime.now(timezone.utc) - timedelta(minutes=1)
        )

        # รัน task พร้อมกัน
        await asyncio.gather(put_task1, put_task2)
        print(f"เขียนข้อมูลสำหรับ {metric_name} สำเร็จ")

        # 3. ค้นหา Series ที่มีอยู่
        print("\n--- ค้นหา Series ที่มี region=us-east (async) ---")
        series = await client.get_series_by_tags(tags={'region': 'us-east'})
        for s in series:
            print(f"  พบ Series: {s}")

        # 4. ค้นหาข้อมูล (Query)
        print("\n--- ค้นหาข้อมูลทั้งหมดในช่วง 5 นาทีที่ผ่านมา (async) ---")
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)
        
        # Query เป็น async generator
        async for point in client.query(
            metric=metric_name,
            start_time=start,
            end_time=now,
        ):
            print(f"  - เวลา: {point['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}, "
                  f"Tags: {point['tags']}, "
                  f"Fields: {point['fields']}")

    except grpc.aio.AioRpcError as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ gRPC: {e.code()} - {e.details()}")

    finally:
        # 5. ปิดการเชื่อมต่อ
        await client.close()

if __name__ == '__main__':
    # รัน event loop ของ asyncio
    asyncio.run(run_async_example())
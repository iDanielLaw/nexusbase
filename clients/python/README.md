# NexusBase Python Client Library

ไลบรารี Client สำหรับเชื่อมต่อกับ NexusBase TSDB ผ่าน gRPC โดยรองรับการทำงานแบบ Asynchronous (asyncio)

## คุณสมบัติ (Features)

*   เชื่อมต่อแบบ Async/await เต็มรูปแบบ
*   รองรับการเชื่อมต่อทั้งแบบปลอดภัย (TLS) และไม่ปลอดภัย (insecure)
*   รองรับการยืนยันตัวตนผ่าน Token (Bearer Token)
*   เมธอดสำหรับ `Put`, `Query`, และ `GetSeriesByTags`

## ข้อกำหนดเบื้องต้น (Requirements)

*   Python 3.8+
*   `grpcio >= 1.60.0`
*   `protobuf >= 4.25.0`

## การติดตั้ง (Installation)

คุณสามารถติดตั้ง Library ได้โดยตรงจาก Git repository:

```bash
# ติดตั้งจาก Git (แนะนำสำหรับเวอร์ชันล่าสุด)
pip install git+https://github.com/INLOpen/nexusbase.git#subdirectory=clients/python
```

หรือหากคุณโคลนโปรเจกต์ลงมาแล้ว สามารถติดตั้งในโหมด Editable ได้จากไดเรกทอรี `clients/python`:

```bash
# จากภายในโปรเจกต์ที่โคลนมา
cd clients/python
pip install -e .
```

## การใช้งาน (Usage)

### การเชื่อมต่อ

```python
import asyncio
from nexusdb import NexusBaseClient

async def main():
    # เชื่อมต่อแบบไม่ปลอดภัย (สำหรับทดสอบในเครื่อง)
    client = NexusBaseClient(host='localhost', port=50051)

    # ตัวอย่างการเชื่อมต่อแบบปลอดภัย (TLS) และมีการยืนยันตัวตน
    # client = NexusBaseClient(
    #     host='your.nexusbase.server',
    #     port=50051,
    #     use_tls=True,
    #     ca_cert_path='/path/to/your/ca.crt', # (ถ้าใช้ Self-signed certificate)
    #     auth_token='your-secret-token'
    # )

    print("เชื่อมต่อสำเร็จ!")
    # ... ใช้งาน client ...

    await client.close()
    print("ปิดการเชื่อมต่อแล้ว")

if __name__ == "__main__":
    asyncio.run(main())
```

### การเขียนข้อมูล (`put`)

```python
from datetime import datetime, timezone

# (โค้ดเชื่อมต่อจากตัวอย่างด้านบน)

await client.put(
    metric="cpu.usage",
    tags={"host": "server-1", "region": "us-east"},
    fields={"value_percent": 85.5, "core": 1},
    ts=datetime.now(timezone.utc)
)
print("เขียนข้อมูลสำเร็จ")
```

### การดึงข้อมูล (`query`)

`query` จะคืนค่าเป็น Async Generator ซึ่งคุณสามารถวนลูปเพื่อดึงข้อมูลแต่ละจุดได้

```python
from datetime import datetime, timedelta, timezone

# (โค้dเชื่อมต่อจากตัวอย่างด้านบน)

start_time = datetime.now(timezone.utc) - timedelta(hours=1)
end_time = datetime.now(timezone.utc)

print(f"กำลังดึงข้อมูล 'cpu.usage' จาก {start_time} ถึง {end_time}")

async for point in client.query(metric="cpu.usage", start_time=start_time, end_time=end_time):
    print(f"  - พบข้อมูล: {point}")
```

## การรันเทส (Running Tests)

จากไดเรกทอรี `clients/python` รันคำสั่ง:

```bash
python -m unittest discover tests
```
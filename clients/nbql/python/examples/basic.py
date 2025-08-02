import time
import sys
import os
import json

# Add the parent directory to the Python path to allow importing the 'nbql' module.
# This is necessary for running the example script directly from the command line.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbql import Client, APIError, ConnectionError

def run_example(host='10.1.1.1', port=50052):
    """
    Demonstrates basic usage of the NBQL Python client.
    (สาธิตการใช้งานพื้นฐานของ NBQL Python client)
    """
    print(f"--- Connecting to NBQL server at {host}:{port} ---")

    try:
        # Using a 'with' statement ensures the connection is properly closed.
        # การใช้ 'with' จะช่วยให้มั่นใจว่าการเชื่อมต่อจะถูกปิดอย่างถูกต้อง
        with Client(host=host, port=port) as client:

            # 1. Push a single data point
            # (ส่งข้อมูล 1 จุด)
            # ---------------------------
            print("\n1. Pushing a single data point...")
            metric_name = "system.cpu.usage"
            tags = {"host": "app-server-01", "region": "us-east-1"}
            value = 55.6

            response = client.push(metric=metric_name, value=value, tags=tags)
            print(f"   Server response: {response}")
            print("   Push successful!")

            # 2. Push multiple data points in bulk
            # (ส่งข้อมูลหลายจุดในครั้งเดียว)
            # ------------------------------------
            print("\n2. Pushing multiple data points in bulk...")
            points = [
                {'metric': 'system.memory.used', 'fields': {'value': 4.2}, 'tags': {'host': 'app-server-01'}, 'timestamp': int(time.time()) - 120},
                {'metric': 'system.memory.used', 'fields': {'value': 8.1}, 'tags': {'host': 'db-server-01'}, 'timestamp': int(time.time()) - 60},
                {'metric': 'api.requests.count', 'fields': {'value': 150}, 'tags': {'endpoint': '/login', 'method': 'POST'}, 'timestamp': int(time.time())}
            ]

            response = client.push_bulk(points)
            print(f"   Server response: {response}")
            print("   Bulk push successful!")

            # 2a. Bulk push with chunking (for very large datasets)
            # (ส่งข้อมูลจำนวนมากแบบแบ่งกลุ่ม เพื่อลดภาระเซิร์ฟเวอร์)
            # ---------------------------------------------------------
            print("\n2a. Pushing large dataset with chunking...")
            # Imagine we have 10,000 points to push
            # (สมมติว่าเรามีข้อมูล 10,000 จุดที่ต้องส่ง)
            large_points_list = [
                {'metric': 'iot.sensor.reading', 'fields': {'value': i % 100}, 'tags': {'sensor_id': f'sensor-{i % 50}'}}
                for i in range(10000)
            ]
            # We send them in chunks of 1000 to avoid server memory issues.
            # (เราจะส่งข้อมูลทีละ 1000 จุด เพื่อเลี่ยงปัญหาหน่วยความจำของเซิร์ฟเวอร์)
            response = client.push_bulk(large_points_list, chunk_size=1000)
            print(f"   Final server response from chunking: {response}")
            print("   Chunked bulk push successful!")

            # 3. Querying the data (using parameterized query for safety)
            # (ดึงข้อมูลที่เพิ่งส่งไป โดยใช้พารามิเตอร์เพื่อความปลอดภัย)
            # -------------------------------------------------------------
            print("\n3. Querying the data we just pushed (using parameters)...")
            # Using '?' as a placeholder for values prevents NBQL injection.
            # The client library will handle quoting and escaping for identifiers (like metric names)
            # and values (like tag values).
            # (การใช้ '?' เป็นตัวยึดตำแหน่งสำหรับค่าต่างๆ จะช่วยป้องกัน NBQL injection
            #  โดย client library จะจัดการเรื่องการใส่เครื่องหมายคำพูดและการ escape ให้เอง
            #  ซึ่งใช้ได้ทั้งกับส่วนที่เป็นชื่อ (เช่น ชื่อเมตริก) และส่วนที่เป็นค่า (เช่น ค่าของแท็ก))
            query_template = 'QUERY ? FROM RELATIVE(10m) TAGGED (host=?)'
            host_value = "app-server-01"
            print(f"   Executing query: '{query_template}' with params: ('{metric_name}', '{host_value}')")

            result = client.query(query_template, metric_name, host_value)
            print("   Query result:")
            print(json.dumps(result, indent=2))

    except ConnectionError as e:
        print(f"\n[ERROR] Could not connect to the NBQL server: {e}")
        print("Please ensure the NBQL server is running and accessible.")
        sys.exit(1)
    except APIError as e:
        print(f"\n[ERROR] An API error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")
        sys.exit(1)

    print("\n--- Example script finished successfully! ---")


if __name__ == "__main__":
    # You can change the host and port here if needed
    run_example()

# NBQL Python Client

A Python client library for interacting with a Nexusbase (NBQL) database.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Library

```python
from nbql import Client, APIError, ConnectionError

try:
    with Client(host='127.0.0.1', port=50052) as client:
        # Push a data point
        client.push(
            metric="cpu.usage",
            value=99.9,
            tags={"host": "server-1", "region": "us-west"}
        )

        # Bulk push data points
        points = [
            {'metric': 'mem.usage', 'fields': {'value': 45.1}, 'tags': {'host': 'server-1'}},
            {'metric': 'mem.usage', 'fields': {'value': 60.8}, 'tags': {'host': 'server-2'}, 'timestamp': 1678886400}
        ]
        # For a large number of points, use chunk_size to avoid server memory issues.
        # (สำหรับข้อมูลจำนวนมาก, ใช้ chunk_size เพื่อเลี่ยงปัญหาหน่วยความจำของเซิร์ฟเวอร์)
        client.push_bulk(points, chunk_size=1000)

        # Run a query using parameters to prevent injection attacks.
        # (รัน query โดยใช้พารามิเตอร์เพื่อป้องกันการโจมตีแบบ injection)
        query_template = 'QUERY ? FROM RELATIVE(5m) TAGGED (host=?)'
        metric_to_query = "cpu.usage"
        host_to_query = "server-1"
        
        result = client.query(query_template, metric_to_query, host_to_query)
        print(result)

except (APIError, ConnectionError) as e:
    print(f"An error occurred: {e}")
except Exception as e:
    print(f"A general error occurred: {e}")

```

### Command-Line Interface (CLI)

The client also comes with a simple CLI for direct interaction.

**Executing a raw query (less secure):**
```bash
python nbql/cli.py --host <your-host> "QUERY cpu.usage FROM RELATIVE(5m) TAGGED (host=\"server-1\")"
```

You can also push data in bulk from a JSON file. The file should contain an array of data point objects. + Example data.json: 
```json
[
  {"metric": "disk.usage", "fields": {"value": 78.2}, "tags": {"path": "/", "host": "web-01"}},
  {"metric": "disk.usage", "fields": {"value": 45.9}, "tags": {"path": "/var", "host": "web-01"}}
]
```

Command:
```bash
python cli.py --file data.json --chunk-size 1000
```
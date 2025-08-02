# NBQL Python Client

A Python client library for interacting with a Nexusbase (NBQL) database.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Library

```python
from nbql import Client, APIError

try:
    with Client(host='127.0.0.1', port=9925) as client:
        # Push a data point
        client.push(
            metric="cpu.usage",
            value=99.9,
            tags={"host": "server-1", "region": "us-west"}
        )

        # Run a raw query
        result = client.query("QUERY SERIES FROM cpu.usage TAGGED WITH host=server-1")
        print(result)

except APIError as e:
    print(f"An error occurred: {e}")
except Exception as e:
    print(f"A general error occurred: {e}")

```

### Command-Line Interface (CLI)

The client also comes with a simple CLI for direct interaction.

```bash
python cli.py --host <your-host> "QUERY SERIES FROM cpu.usage"
```
# NexusBase TypeScript Client Library

A TypeScript client library for connecting to NexusBase TSDB via gRPC. This library is designed for Node.js environments.

## Features

*   Fully typed, modern TypeScript API.
*   Handles gRPC connection management.
*   Simple methods for `put` and `query` operations.
*   Supports both insecure and TLS connections (basic setup).

## Requirements

*   Node.js 16+
*   `@grpc/grpc-js`
*   `google-protobuf`

## Installation

```bash
npm install nexusbase-client # Or your published package name
```

Or, if you have cloned the repository:

```bash
# From the root of the tsdb-prototype project
cd clients/typescript
npm install
npm run build
```

### Protocol Buffer Generation

The gRPC and Protobuf code is pre-generated. If you make changes to `proto/tsdb.proto`, you need to regenerate the client code:

```bash
# From within clients/typescript
npm run build:proto
```

## Usage

### Connecting to the Server

```typescript
import { NexusBaseClient } from 'nexusbase-client'; // Adjust import path

async function main() {
  // Connect insecurely (for local development)
  const client = new NexusBaseClient({
    host: 'localhost',
    port: 50051,
  });

  console.log('Successfully connected to NexusBase!');

  // ... use client ...

  client.close();
  console.log('Connection closed.');
}

main().catch(console.error);
```

### Writing Data (`put`)

```typescript
import { NexusBaseClient } from 'nexusbase-client';

async function writeData() {
  const client = new NexusBaseClient();

  try {
    await client.put({
      metric: 'cpu.usage',
      tags: { host: 'server-1', region: 'us-east' },
      fields: {
        value_percent: 85.5,
        core: 1,
        active: true,
      },
      ts: new Date(), // Timestamp is optional, defaults to now
    });
    console.log('Successfully wrote data point.');
  } catch (error) {
    console.error('Failed to write data:', error);
  } finally {
    client.close();
  }
}

writeData();
```

### Querying Data (`query`)

The `query` method returns an `AsyncGenerator` that you can easily loop over.

```typescript
import { NexusBaseClient } from 'nexusbase-client';
import { AggregationSpec } from './src/proto/tsdb_pb'; // Import for enum

async function queryData() {
  const client = new NexusBaseClient();

  try {
    const startTime = new Date(Date.now() - 60 * 60 * 1000); // 1 hour ago
    const endTime = new Date();

    console.log(`Querying 'cpu.usage' from ${startTime.toISOString()} to ${endTime.toISOString()}`);

    const queryStream = client.query({
      metric: 'cpu.usage',
      startTime,
      endTime,
      tags: { host: 'server-1' },
      // Example of aggregation
      aggregations: [
        { func: AggregationSpec.AggregationFunc.AVERAGE, field: 'value_percent' }
      ],
      downsampleInterval: '5m'
    });

    for await (const result of queryStream) {
      console.log('  - Found result:', JSON.stringify(result, null, 2));
    }

  } catch (error) {
    console.error('Query failed:', error);
  } finally {
    client.close();
  }
}

queryData();
```

## Development

To build the project from source:

```bash
# Install dependencies
npm install

# Generate protobuf files (if needed)
npm run build:proto

# Compile TypeScript to JavaScript
npm run build
```
The compiled output will be in the `dist` directory.
import { NexusBaseClient, PutDataPoint, QueryParams } from './client';
import { tsdb } from './proto/api/tsdb/tsdb';
import { google } from './proto/google/protobuf/struct';
import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import * as grpc from '@grpc/grpc-js';

// Mock the gRPC client implementation
const mockGrpcClient = {
  Put: jest.fn() as jest.Mock<(request: tsdb.PutRequest, callback: (error: grpc.ServiceError | null, response?: tsdb.PutResponse) => void) => void>,
  PutBatch: jest.fn() as jest.Mock<(request: tsdb.PutBatchRequest, callback: (error: grpc.ServiceError | null, response?: tsdb.PutBatchResponse) => void) => void>,
  Query: jest.fn() as jest.Mock<(request: tsdb.QueryRequest) => any>,
  close: jest.fn() as jest.Mock<() => void>,
};

// Mock the TSDBServiceClient constructor to return our mock client
// This allows us to intercept calls without needing a running server.
jest.mock('./proto/api/tsdb/tsdb', () => {
  // We need the actual message classes and enums for type checking and instantiation
  const original = jest.requireActual('./proto/api/tsdb/tsdb') as { tsdb: any };
  return {
    tsdb: {
      ...original.tsdb, // Keep original enums, message classes etc.
      TSDBServiceClient: jest.fn().mockImplementation(() => mockGrpcClient),
    },
  };
});

describe('NexusBaseClient', () => {
  let client: NexusBaseClient;

  beforeEach(() => {
    // Reset mocks before each test to ensure isolation
    jest.clearAllMocks();
    client = new NexusBaseClient({ host: 'localhost', port: 50051 });
  });

  describe('put', () => {
    it('should correctly format and send a data point with all fields', async () => {
      const dataPoint: PutDataPoint = {
        metric: 'cpu.usage',
        tags: { host: 'server-1', region: 'us-east' },
        fields: {
          value: 99.5,
          core: 1,
          active: true,
          status: 'ok',
          extra: null,
        },
        ts: new Date('2023-01-01T00:00:00.000Z'),
      };

      // Mock the gRPC callback to simulate a successful call
      mockGrpcClient.Put.mockImplementation((_req, callback) => {
        callback(null, new tsdb.PutResponse());
      });

      await client.put(dataPoint);

      // Verify that the Put method was called once
      expect(mockGrpcClient.Put).toHaveBeenCalledTimes(1);

      // Get the request object passed to the mock Put method
      const request: tsdb.PutRequest = mockGrpcClient.Put.mock.calls[0][0];

      // Assertions on the request object
      expect(request.metric).toBe('cpu.usage');
      expect(request.tags).toEqual(new Map(Object.entries(dataPoint.tags!)));

      // JS Date.getTime() is in ms, we expect nanoseconds (ms * 1,000,000)
      const expectedTimestamp = dataPoint.ts!.getTime() * 1_000_000;
      expect(request.timestamp).toBe(expectedTimestamp);

      // Verify fields conversion to protobuf Struct
      const fields = request.fields!.fields;
      expect(fields.get('value')?.number_value).toBe(99.5);
      expect(fields.get('core')?.number_value).toBe(1);
      expect(fields.get('active')?.bool_value).toBe(true);
      expect(fields.get('status')?.string_value).toBe('ok');
      expect(fields.get('extra')?.null_value).toBe(google.protobuf.NullValue.NULL_VALUE);
    });

    it('should use current time if timestamp is not provided', async () => {
      const now = Date.now();
      // Mock Date.now() to get a predictable timestamp
      const dateNowSpy = jest.spyOn(Date, 'now').mockImplementation(() => now);

      const dataPoint: PutDataPoint = {
        metric: 'memory.usage',
        fields: { value: 512 },
      };

      mockGrpcClient.Put.mockImplementation((_req, callback) => {
        callback(null, new tsdb.PutResponse());
      });

      await client.put(dataPoint);

      const request: tsdb.PutRequest = mockGrpcClient.Put.mock.calls[0][0];
      const expectedTimestamp = now * 1_000_000;
      expect(request.timestamp).toBe(expectedTimestamp);

      dateNowSpy.mockRestore();
    });

    it('should reject the promise on gRPC error', async () => {
      const dataPoint: PutDataPoint = {
        metric: 'test.metric',
        fields: { value: 1 },
      };
      const grpcError = new Error('gRPC connection failed') as grpc.ServiceError;
      grpcError.code = grpc.status.UNAVAILABLE;

      // Mock the gRPC callback to simulate an error
      mockGrpcClient.Put.mockImplementation((_req, callback) => {
        callback(grpcError, undefined);
      });

      await expect(client.put(dataPoint)).rejects.toThrow('gRPC connection failed');
    });
  });

  describe('putBatch', () => {
    it('should correctly format and send a batch of data points', async () => {
      const dataPoints: PutDataPoint[] = [
        {
          metric: 'cpu.usage',
          tags: { host: 'server-1', region: 'us-east' },
          fields: { value: 55.5 },
          ts: new Date('2023-01-01T00:00:00.000Z'),
        },
        {
          metric: 'memory.usage',
          tags: { host: 'server-1' },
          fields: { value: 8192, unit: 'MB' },
          ts: new Date('2023-01-01T00:00:01.000Z'),
        },
      ];

      // Mock the gRPC callback for PutBatch
      mockGrpcClient.PutBatch.mockImplementation((_req, callback) => {
        callback(null, new tsdb.PutBatchResponse());
      });

      await client.putBatch(dataPoints);

      // Verify that PutBatch was called once
      expect(mockGrpcClient.PutBatch).toHaveBeenCalledTimes(1);

      // Get the request object passed to the mock PutBatch method
      const request: tsdb.PutBatchRequest = mockGrpcClient.PutBatch.mock.calls[0][0];

      // Verify the batch request
      expect(request.points).toHaveLength(2);

      // Verify the first point in the batch
      const point1 = request.points[0];
      expect(point1.metric).toBe('cpu.usage');
      expect(point1.tags).toEqual(new Map(Object.entries(dataPoints[0].tags!)));
      expect(point1.timestamp).toBe(dataPoints[0].ts!.getTime() * 1_000_000);
      expect(point1.fields!.fields.get('value')?.number_value).toBe(55.5);

      // Verify the second point in the batch
      const point2 = request.points[1];
      expect(point2.metric).toBe('memory.usage');
      expect(point2.tags).toEqual(new Map(Object.entries(dataPoints[1].tags!)));
      expect(point2.timestamp).toBe(dataPoints[1].ts!.getTime() * 1_000_000);
      expect(point2.fields!.fields.get('value')?.number_value).toBe(8192);
      expect(point2.fields!.fields.get('unit')?.string_value).toBe('MB');
    });

    it('should resolve without error for an empty batch', async () => {
      await expect(client.putBatch([])).resolves.toBeUndefined();
      expect(mockGrpcClient.PutBatch).not.toHaveBeenCalled();
    });

    it('should reject the promise on gRPC error for PutBatch', async () => {
      const dataPoints: PutDataPoint[] = [{ metric: 'test.metric', fields: { value: 1 } }];
      const grpcError = new Error('gRPC batch failed') as grpc.ServiceError;
      grpcError.code = grpc.status.INTERNAL;

      // Mock the gRPC callback to simulate an error
      mockGrpcClient.PutBatch.mockImplementation((_req, callback) => {
        callback(grpcError, undefined);
      });

      await expect(client.putBatch(dataPoints)).rejects.toThrow('gRPC batch failed');
    });
  });

  describe('query', () => {
    it('should correctly format and send a query request', async () => {
      const queryParams: QueryParams = {
        metric: 'cpu.usage',
        startTime: new Date('2023-01-01T00:00:00.000Z'),
        endTime: new Date('2023-01-01T01:00:00.000Z'),
        tags: { host: 'server-1' },
        aggregations: [
          { func: tsdb.AggregationSpec.AggregationFunc.AVERAGE, field: 'value' },
          { func: tsdb.AggregationSpec.AggregationFunc.COUNT, field: 'value' },
        ],
        downsampleInterval: '5m',
        limit: 100,
      };

      // Mock the gRPC stream response
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({ metric: 'cpu.usage', timestamp: 12345 });
        yield new tsdb.QueryResult({ metric: 'cpu.usage', timestamp: 67890 });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      const results: ReturnType<tsdb.QueryResult['toObject']>[] = [];
      for await (const result of client.query(queryParams)) {
        results.push(result);
      }

      expect(mockGrpcClient.Query).toHaveBeenCalledTimes(1);
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];

      expect(request.metric).toBe('cpu.usage');
      expect(request.start_time).toBe(queryParams.startTime!.getTime() * 1_000_000);
      expect(request.end_time).toBe(queryParams.endTime!.getTime() * 1_000_000);
      expect(request.tags).toEqual(new Map(Object.entries(queryParams.tags!)));
      expect(request.downsample_interval).toBe('5m');
      expect(request.limit).toBe(100);
      expect(request.aggregation_specs).toHaveLength(2);
      expect(request.aggregation_specs[0].function).toBe(tsdb.AggregationSpec.AggregationFunc.AVERAGE);
      expect(request.aggregation_specs[0].field).toBe('value');

      expect(results).toHaveLength(2);
      expect(results[0].metric).toBe('cpu.usage');
      expect(results[1].timestamp).toBe(67890);
    });

    it('should handle queries without tags', async () => {
      const queryParams: QueryParams = {
        metric: 'cpu.usage',
        startTime: new Date('2023-01-01T00:00:00.000Z'),
        endTime: new Date('2023-01-01T01:00:00.000Z'),
      };

      // Mock the gRPC stream response
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({ metric: 'cpu.usage' });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      // We just need to trigger the call, so we don't need to loop through the results
      await client.query(queryParams).next();

      expect(mockGrpcClient.Query).toHaveBeenCalledTimes(1);
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];

      // The tags map should be empty, not null or undefined.
      expect(request.tags).toBeInstanceOf(Map);
      expect(request.tags.size).toBe(0);
    });

    it('should handle queries with an empty tags object', async () => {
      const queryParams: QueryParams = {
        metric: 'cpu.usage',
        startTime: new Date('2023-01-01T00:00:00.000Z'),
        endTime: new Date('2023-01-01T01:00:00.000Z'),
        tags: {}, // Explicitly empty tags object
      };

      // Mock the gRPC stream response
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({ metric: 'cpu.usage' });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      // We just need to trigger the call, so we don't need to loop through the results
      await client.query(queryParams).next();

      expect(mockGrpcClient.Query).toHaveBeenCalledTimes(1);
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];

      expect(request.tags).toBeInstanceOf(Map);
      expect(request.tags.size).toBe(0);
    });

    it('should correctly handle a response with multiple aggregated values', async () => {
      const startTime = new Date('2023-01-01T10:00:00.000Z');
      const endTime = new Date('2023-01-01T11:00:00.000Z');

      const queryParams: QueryParams = {
        metric: 'system.load',
        startTime,
        endTime,
        aggregations: [
          { func: tsdb.AggregationSpec.AggregationFunc.SUM, field: 'value' },
          { func: tsdb.AggregationSpec.AggregationFunc.COUNT, field: 'value' },
          { func: tsdb.AggregationSpec.AggregationFunc.MAX, field: 'value' },
          { func: tsdb.AggregationSpec.AggregationFunc.MIN, field: 'value' },
        ],
        // No downsample, so it's a final aggregation over the whole time range
      };

      const mockAggregatedValues = {
        'sum_value': 150.5,
        'count_value': 10,
        'max_value': 25.1,
        'min_value': 5.9,
      };

      // Mock the gRPC stream response
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({
          is_aggregated: true,
          window_start_time: startTime.getTime() * 1_000_000,
          window_end_time: endTime.getTime() * 1_000_000,
          aggregated_values: new Map(Object.entries(mockAggregatedValues)),
        });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      const results: ReturnType<tsdb.QueryResult['toObject']>[] = [];
      for await (const result of client.query(queryParams)) {
        results.push(result);
      }

      // Verify the results
      expect(results).toHaveLength(1);
      const result = results[0];
      expect(result.is_aggregated).toBe(true);
      expect(result.window_start_time).toBe(startTime.getTime() * 1_000_000);
      expect(result.window_end_time).toBe(endTime.getTime() * 1_000_000);
      expect(result.aggregated_values).toEqual(mockAggregatedValues);
    });

    it('should handle downsampling and aggregation together', async () => {
      const startTime = new Date('2023-10-26T10:00:00.000Z');
      const endTime = new Date('2023-10-26T10:10:00.000Z'); // 10 minute range

      const queryParams: QueryParams = {
        metric: 'network.bytes',
        startTime,
        endTime,
        tags: { interface: 'eth0' },
        aggregations: [
          { func: tsdb.AggregationSpec.AggregationFunc.SUM, field: 'rx' },
          { func: tsdb.AggregationSpec.AggregationFunc.SUM, field: 'tx' },
        ],
        downsampleInterval: '5m', // 2 windows in the 10 minute range
      };

      const mockWindow1Values = { sum_rx: 1000, sum_tx: 500 };
      const mockWindow2Values = { sum_rx: 1200, sum_tx: 650 };

      // Mock the gRPC stream to return two downsampled windows
      const mockStream = (async function* () {
        // Window 1: 10:00 to 10:05
        yield new tsdb.QueryResult({
          is_aggregated: true,
          window_start_time: new Date('2023-10-26T10:00:00.000Z').getTime() * 1_000_000,
          aggregated_values: new Map(Object.entries(mockWindow1Values)),
        });
        // Window 2: 10:05 to 10:10
        yield new tsdb.QueryResult({
          is_aggregated: true,
          window_start_time: new Date('2023-10-26T10:05:00.000Z').getTime() * 1_000_000,
          aggregated_values: new Map(Object.entries(mockWindow2Values)),
        });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      const results: ReturnType<tsdb.QueryResult['toObject']>[] = [];
      for await (const result of client.query(queryParams)) {
        results.push(result);
      }

      expect(results).toHaveLength(2);
      expect(results[0].aggregated_values).toEqual(mockWindow1Values);
      expect(results[1].aggregated_values).toEqual(mockWindow2Values);
      expect(results[1].window_start_time).toBe(new Date('2023-10-26T10:05:00.000Z').getTime() * 1_000_000);
    });

    it('should request default aggregations when only downsampling is provided', async () => {
      const startTime = new Date('2023-10-26T12:00:00.000Z');
      const endTime = new Date('2023-10-26T12:05:00.000Z');

      const queryParams: QueryParams = {
        metric: 'system.cpu.idle',
        startTime,
        endTime,
        tags: { core: '0' },
        // No `aggregations` property
        downsampleInterval: '1m',
      };

      // The backend is expected to calculate default aggregations.
      // Let's mock a response that reflects this for a field named 'value'.
      const mockDefaultAggregatedValues = {
        'sum_value': 450.0,
        'count_value': 5,
        'max_value': 95.0,
        'min_value': 85.0,
        'avg_value': 90.0,
      };

      // Mock the gRPC stream to return one downsampled window
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({
          is_aggregated: true,
          window_start_time: new Date('2023-10-26T12:00:00.000Z').getTime() * 1_000_000,
          aggregated_values: new Map(Object.entries(mockDefaultAggregatedValues)),
        });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      const results: ReturnType<tsdb.QueryResult['toObject']>[] = [];
      for await (const result of client.query(queryParams)) {
        results.push(result);
      }

      // Verify the request sent to the gRPC client
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];
      expect(request.aggregation_specs).toHaveLength(0);

      // Verify the results returned to the user
      expect(results).toHaveLength(1);
      expect(results[0].is_aggregated).toBe(true);
      expect(results[0].aggregated_values).toEqual(mockDefaultAggregatedValues);
    });

    it('should handle queries without startTime or endTime', async () => {
      const queryParams: QueryParams = {
        metric: 'system.uptime',
      };

      // Mock the gRPC stream response
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({ metric: 'system.uptime', timestamp: 1 });
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      // We just need to trigger the call, so we don't need to loop through the results
      await client.query(queryParams).next();

      expect(mockGrpcClient.Query).toHaveBeenCalledTimes(1);
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];

      // Verify that start_time and end_time default to 0 if not provided
      // The generated protobuf classes default number fields to 0.
      expect(request.start_time).toBe(0);
      expect(request.end_time).toBe(0);
      expect(request.metric).toBe('system.uptime');
    });

    it('should respect the limit parameter and stop iteration', async () => {
      const queryParams: QueryParams = {
        metric: 'network.traffic',
        startTime: new Date('2023-01-01T00:00:00.000Z'),
        endTime: new Date('2023-01-01T01:00:00.000Z'),
        limit: 2, // We expect only 2 results
      };

      // Mock the gRPC stream to return more items than the limit
      const mockStream = (async function* () {
        yield new tsdb.QueryResult({ timestamp: 1 });
        yield new tsdb.QueryResult({ timestamp: 2 });
        yield new tsdb.QueryResult({ timestamp: 3 }); // This should not be processed
        yield new tsdb.QueryResult({ timestamp: 4 }); // This should not be processed
      })();
      mockGrpcClient.Query.mockReturnValue(mockStream);

      const results: ReturnType<tsdb.QueryResult['toObject']>[] = [];
      for await (const result of client.query(queryParams)) {
        results.push(result);
      }

      // Verify that the Query method was called with the correct limit
      expect(mockGrpcClient.Query).toHaveBeenCalledTimes(1);
      const request: tsdb.QueryRequest = mockGrpcClient.Query.mock.calls[0][0];
      expect(request.limit).toBe(2);

      // Verify that the client only yielded the limited number of results
      expect(results).toHaveLength(2);
      expect(results[0].timestamp).toBe(1);
      expect(results[1].timestamp).toBe(2);
    });
  });

  describe('close', () => {
    it('should call the underlying client close method', () => {
      client.close();
      expect(mockGrpcClient.close).toHaveBeenCalledTimes(1);
    });
  });
});
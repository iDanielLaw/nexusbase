import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta, timezone

import grpc
from google.protobuf.struct_pb2 import Struct

# Import the client we want to test
from nexusbase.client import NexusBaseClient

# Import the generated protobuf files
# Assuming the structure is clients/python/nexus/
from nexusbase.nexus import tsdb_pb2, tsdb_pb2_grpc


class MockAsyncIterator:
    """Helper class to mock an async iterator."""
    def __init__(self, items):
        self._items = items
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index < len(self._items):
            item = self._items[self._index]
            self._index += 1
            return item
        else:
            raise StopAsyncIteration


class TestNexusBaseClient(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the async NexusBaseClient."""

    async def asyncSetUp(self):
        """
        Set up the test environment before each test.
        This method patches the TSDBServiceStub to avoid actual network calls.
        """
        # Create a mock object that mimics the behavior of the gRPC stub.
        # The stub methods are now awaitable, so we use AsyncMock.
        self.mock_stub = MagicMock(spec=tsdb_pb2_grpc.TSDBServiceStub)
        self.mock_stub.Put = AsyncMock()
        self.mock_stub.Query = MagicMock() # Will return an async iterator
        self.mock_stub.GetSeriesByTags = AsyncMock()

        # Use patch to replace the real stub with our mock stub during client initialization.
        # The target string is where the object is looked up, not where it's defined.
        self.stub_patcher = patch('nexusbase.client.tsdb_pb2_grpc.TSDBServiceStub', return_value=self.mock_stub)
        self.stub_patcher.start()

        # Now, when we create a client, it will be initialized with our mock_stub
        self.client = NexusBaseClient(use_tls=False, auth_token=None)

    async def asyncTearDown(self):
        """Clean up after each test by stopping the patcher."""
        self.stub_patcher.stop()
        await self.client.close()

    async def test_put_success(self):
        """Test a successful Put operation."""
        # Arrange: Configure the mock stub to return a simple response for the Put call.
        self.mock_stub.Put.return_value = tsdb_pb2.PutResponse()

        # Act: Call the method on our client.
        metric = "test.metric"
        tags = {"host": "test-host"}
        fields = {"value": 123.45}
        ts = datetime.now(timezone.utc)

        await self.client.put(metric=metric, tags=tags, fields=fields, ts=ts)

        # Assert: Check that the Put method on the stub was called exactly once.
        self.mock_stub.Put.assert_awaited_once()

        # Optional but recommended: Check the arguments it was called with.
        called_with_request = self.mock_stub.Put.call_args[0][0]
        self.assertEqual(called_with_request.metric, metric)
        self.assertEqual(dict(called_with_request.tags), tags)
        self.assertEqual(called_with_request.fields['value'], 123.45)

    async def test_query_success_streaming(self):
        """Test a successful Query operation that returns a stream of data."""
        # Arrange: Configure the mock stub to return an iterator of predefined responses.
        expected_timestamp_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)

        response_fields = Struct()
        response_fields.update({"value": 99.9})

        mock_responses = [
            tsdb_pb2.QueryResult(
                metric="stream.metric",
                tags={"source": "test"},
                timestamp=expected_timestamp_ns,
                fields=response_fields,
                is_aggregated=False
            )
        ]
        self.mock_stub.Query.return_value = MockAsyncIterator(mock_responses)

        # Act: Call the query method and collect the results.
        results = [item async for item in self.client.query(
            metric="stream.metric",
            start_time=datetime.now(timezone.utc) - timedelta(minutes=1),
            end_time=datetime.now(timezone.utc)
        )]

        # Assert: Check that the Query method was called and the results are correct.
        self.mock_stub.Query.assert_called_once()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['metric'], "stream.metric")
        self.assertEqual(results[0]['fields']['value'], 99.9)
        self.assertEqual(int(results[0]['timestamp'].timestamp() * 1_000_000_000), expected_timestamp_ns)

    async def test_get_series_by_tags_success(self):
        """Test a successful GetSeriesByTags operation."""
        # Arrange: Configure the mock to return a response with a list of series.
        expected_keys = ["series_key_1", "series_key_2"]
        self.mock_stub.GetSeriesByTags.return_value = tsdb_pb2.GetSeriesByTagsResponse(series_keys=expected_keys)

        # Act: Call the method.
        series = await self.client.get_series_by_tags(metric="some.metric")

        # Assert: Check the result.
        self.mock_stub.GetSeriesByTags.assert_awaited_once()
        self.assertEqual(series, expected_keys)

    async def test_put_rpc_error_handling(self):
        """Test that the client correctly handles an RpcError from the server."""
        # Arrange: Configure the mock to raise an RpcError when called.
        # The AioRpcError constructor requires initial_metadata and trailing_metadata as positional arguments.
        self.mock_stub.Put.side_effect = grpc.aio.AioRpcError(
            grpc.StatusCode.UNAVAILABLE,
            (),  # initial_metadata
            (),  # trailing_metadata
            details="Server is temporarily down"
        )

        # Act & Assert: Use assertRaises to verify that the expected exception is raised.
        with self.assertRaises(grpc.aio.AioRpcError) as context:
            await self.client.put(metric="fail.metric", fields={"value": 1})

        # Verify the details of the caught exception.
        self.assertEqual(context.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(context.exception.details(), "Server is temporarily down")

if __name__ == '__main__':
    unittest.main()
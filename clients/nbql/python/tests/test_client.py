import unittest
from unittest.mock import patch, MagicMock
import json
import struct
import io
import socket

# Add project root to path to allow direct import of nbql
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbql.client import Client
from nbql.exceptions import ConnectionError, APIError
from nbql import protocol

class TestNBQLClient(unittest.TestCase):

    def setUp(self):
        self.client = Client(host='localhost', port=1234)

    def _create_mock_socket(self, recv_data):
        """
        Helper to create a mock socket that simulates reading from a stream.
        recv_data can be a single bytes object or a list of them (for multiple responses).
        """
        mock_sock = MagicMock(spec=socket.socket)

        # If the test provides a list of byte strings, it means multiple server responses.
        # We concatenate them to simulate a continuous stream the client reads from.
        if isinstance(recv_data, list):
            data_stream = io.BytesIO(b"".join(recv_data))
        else:
            data_stream = io.BytesIO(recv_data)

        def mock_recv(bufsize):
            # Read from the in-memory stream, which behaves like a real socket buffer
            return data_stream.read(bufsize)

        mock_sock.recv.side_effect = mock_recv
        return mock_sock

    def _pack_frame(self, cmd_type, payload):
        """Helper to pack a command and payload into a valid binary frame."""
        header = struct.pack('>BI', cmd_type, len(payload))
        checksum = protocol.crc32c(header + payload)
        return header + payload + struct.pack('>I', checksum)

    def _create_manipulate_response_frame(self, rows_affected, seq_ids=None):
        """Helper to create a packed CMD_MANIPULATE response frame."""
        if seq_ids is None:
            seq_ids = []
        resp_buf = io.BytesIO()
        resp_buf.write(struct.pack('>B', protocol.RESP_OK))
        resp_buf.write(struct.pack('>Q', rows_affected))
        resp_buf.write(struct.pack('>H', len(seq_ids)))
        for seq_id in seq_ids:
            resp_buf.write(struct.pack('>Q', seq_id))
        return self._pack_frame(protocol.CMD_MANIPULATE, resp_buf.getvalue())

    @patch('nbql.client.socket.create_connection')
    def test_query_success_streaming(self, mock_create_connection):
        """Test a successful query with a streaming response."""
        # Frame 1: Query Result Part with 1 point
        part1_buf = io.BytesIO()
        part1_buf.write(struct.pack('>B', protocol.RESP_OK))
        part1_buf.write(struct.pack('>B', 0)) # flags
        protocol._write_string(part1_buf, "") # next_cursor
        part1_buf.write(struct.pack('>I', 1)) # point count
        # point 1 data
        part1_buf.write(struct.pack('>Q', 1)) # seq id
        protocol._write_string(part1_buf, "cpu.usage")
        protocol._write_tags(part1_buf, {'host': 'server1'})
        part1_buf.write(struct.pack('>q', 1000)) # timestamp
        protocol._write_fields(part1_buf, {'value': 99.5})
        frame1 = self._pack_frame(protocol.CMD_QUERY_RESULT_PART, part1_buf.getvalue())

        # Frame 2: Query End
        end_buf = io.BytesIO()
        end_buf.write(struct.pack('>B', protocol.RESP_OK))
        end_buf.write(struct.pack('>Q', 1)) # total_rows
        protocol._write_string(end_buf, "Query successful")
        frame2 = self._pack_frame(protocol.CMD_QUERY_END, end_buf.getvalue())

        mock_sock = self._create_mock_socket([frame1, frame2])
        mock_create_connection.return_value = mock_sock

        query_string = 'QUERY cpu.usage FROM RELATIVE(1m)'
        result = self.client.query(query_string)

        # Verify the client sent the correct frame
        sent_data = mock_sock.sendall.call_args[0][0]
        sent_cmd, sent_payload_len = struct.unpack('>BI', sent_data[:5])
        sent_payload_bytes = sent_data[5:5+sent_payload_len]

        self.assertEqual(sent_cmd, protocol.CMD_QUERY)
        self.assertEqual(protocol._read_string(io.BytesIO(sent_payload_bytes)), query_string)
        
        # Verify the aggregated result from the client
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['total_rows'], 1)
        self.assertEqual(len(result['results']), 1)
        self.assertEqual(result['results'][0]['metric'], 'cpu.usage')
        self.assertEqual(result['results'][0]['fields']['value'], 99.5)

    @patch('nbql.client.socket.create_connection')
    def test_query_api_error(self, mock_create_connection):
        """Test a query that results in an API error from the server."""
        # Mock a binary error response, not JSON
        error_buf = io.BytesIO()
        error_buf.write(struct.pack('>H', 400)) # Error code
        protocol._write_string(error_buf, "Syntax error")
        binary_error_payload = error_buf.getvalue()

        response_frame = self._pack_frame(protocol.CMD_ERROR, binary_error_payload)
        mock_sock = self._create_mock_socket(response_frame)
        mock_create_connection.return_value = mock_sock

        with self.assertRaises(APIError) as cm:
            self.client.query("INVALID QUERY")
        self.assertEqual(str(cm.exception), "API Error 400: Syntax error")

    @patch('nbql.client.socket.create_connection')
    def test_push_simple_success(self, mock_create_connection):
        """Test pushing a simple metric successfully."""
        # Prepare server response
        response_frame = self._create_manipulate_response_frame(rows_affected=1, seq_ids=[12345])
        
        mock_sock = self._create_mock_socket(response_frame)
        mock_create_connection.return_value = mock_sock

        result = self.client.push("temperature", 25.5, tags={'room': 'A1'}, timestamp=1678886400)

        # Verify what was sent
        sent_data = mock_sock.sendall.call_args[0][0]
        sent_cmd, sent_payload_len = struct.unpack('>BI', sent_data[:5])
        sent_payload_bytes = sent_data[5:5+sent_payload_len]
        self.assertEqual(sent_cmd, protocol.CMD_PUSH)

        # Decode the payload to verify its contents
        p_reader = io.BytesIO(sent_payload_bytes)
        metric = protocol._read_string(p_reader)
        tags = protocol._read_tags(p_reader)
        timestamp = struct.unpack('>q', p_reader.read(8))[0]

        self.assertEqual(metric, "temperature")
        self.assertEqual(tags, {'room': 'A1'})
        self.assertEqual(timestamp, 1678886400)

        # Verify client response
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['rows_affected'], 1)
        self.assertEqual(result['sequence_ids'], [12345])

    @patch('nbql.client.socket.create_connection')
    def test_push_bulk_no_chunking(self, mock_create_connection):
        """Test that push_bulk sends all data in one request when not chunking."""
        response_frame = self._create_manipulate_response_frame(rows_affected=2)
        mock_sock = self._create_mock_socket(response_frame)
        mock_create_connection.return_value = mock_sock

        points = [
            {'metric': 'cpu', 'fields': {'value': 1.0}},
            {'metric': 'mem', 'fields': {'value': 2.0}},
        ]

        result = self.client.push_bulk(points)

        # Should only be called once
        mock_sock.sendall.assert_called_once()
        self.assertEqual(result['rows_affected'], 2)

        # Verify payload contents
        sent_data = mock_sock.sendall.call_args[0][0]
        sent_cmd, _ = struct.unpack('>BI', sent_data[:5])
        self.assertEqual(sent_cmd, protocol.CMD_PUSHS)

    @patch('nbql.client.socket.create_connection')
    def test_push_bulk_with_chunking(self, mock_create_connection):
        """Test that push_bulk sends data in multiple chunks using the binary protocol."""
        # Prepare server responses for two chunks
        response_frame1 = self._create_manipulate_response_frame(rows_affected=2)
        response_frame2 = self._create_manipulate_response_frame(rows_affected=1)

        # The new mock socket simulates a stream, so we just provide the full frames.
        mock_sock = self._create_mock_socket([
            response_frame1, # First response
            response_frame2  # Second response
        ])
        mock_create_connection.return_value = mock_sock

        points = [
            {'metric': 'cpu', 'fields': {'value': 1.0}},
            {'metric': 'cpu', 'fields': {'value': 2.0}},
            {'metric': 'cpu', 'fields': {'value': 3.0}},
        ]

        # Send 3 points in chunks of 2
        result = self.client.push_bulk(points, chunk_size=2)

        self.assertEqual(mock_sock.sendall.call_count, 2)
        self.assertEqual(result['rows_affected'], 1) # Returns the last response

    @patch('nbql.client.socket.create_connection')
    def test_connection_error_on_connect(self, mock_create_connection):
        """Test a query that fails due to a connection error during connect."""
        mock_create_connection.side_effect = socket.timeout("Connection timed out")
        with self.assertRaises(ConnectionError):
            self.client.query("QUERY cpu.usage FROM RELATIVE(1m)")

    @patch('nbql.client.socket.create_connection')
    def test_connection_error_on_send(self, mock_create_connection):
        """Test a query that fails during sendall."""
        mock_sock = MagicMock()
        mock_sock.sendall.side_effect = BrokenPipeError("Pipe is broken")
        mock_create_connection.return_value = mock_sock

        with self.assertRaises(ConnectionError):
            self.client.query("QUERY cpu.usage FROM RELATIVE(1m)")
        mock_sock.close.assert_called_once()

    @patch('nbql.client.socket.create_connection')
    def test_connection_error_on_recv(self, mock_create_connection):
        """Test a query that fails during recv."""
        mock_sock = MagicMock()
        mock_sock.recv.side_effect = socket.error("Socket error")
        mock_create_connection.return_value = mock_sock

        with self.assertRaises(ConnectionError):
            self.client.query("QUERY cpu.usage FROM RELATIVE(1m)")
        mock_sock.close.assert_called_once()

    def test_push_bulk_empty_list(self):
        """Test pushing an empty list of points, which should not call the API."""
        with patch('nbql.client.socket.create_connection') as mock_create_connection:
            result = self.client.push_bulk([])
            mock_create_connection.assert_not_called()
            self.assertEqual(result, {'status': 'ok', 'message': 'No points to push.'})

    def test_push_bulk_invalid_point(self):
        """Test pushing a list with an invalid point (e.g., missing 'fields')."""
        points = [
            {'metric': 'cpu', 'fields': {'value': 99.0}},
            {'metric': 'mem'} # Missing 'fields'
        ]
        with self.assertRaisesRegex(ValueError, "Each point must be a dict and contain 'metric' and 'fields' keys."):
            self.client.push_bulk(points)

    def tearDown(self):
        self.client.close()

if __name__ == '__main__':
    unittest.main()

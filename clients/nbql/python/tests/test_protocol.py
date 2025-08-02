import unittest
import struct
import io

# Add project root to path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nbql import protocol
from nbql.exceptions import ConnectionError
from unittest.mock import MagicMock

class TestProtocolFunctions(unittest.TestCase):

    def test_string_rw(self):
        """Tests writing and reading a string."""
        buf = io.BytesIO()
        test_str = "hello world"
        protocol._write_string(buf, test_str)
        buf.seek(0)
        read_str = protocol._read_string(buf)
        self.assertEqual(test_str, read_str)

    def test_empty_string_rw(self):
        """Tests writing and reading an empty string."""
        buf = io.BytesIO()
        protocol._write_string(buf, "")
        buf.seek(0)
        read_str = protocol._read_string(buf)
        self.assertEqual("", read_str)

    def test_tags_rw(self):
        """Tests writing and reading tags."""
        buf = io.BytesIO()
        tags = {"host": "server-a", "region": "us-west-1"}
        protocol._write_tags(buf, tags)
        buf.seek(0)
        read_tags = protocol._read_tags(buf)
        self.assertEqual(tags, read_tags)

    def test_no_tags_rw(self):
        """Tests writing and reading nil/empty tags."""
        # Test with None
        buf_none = io.BytesIO()
        protocol._write_tags(buf_none, None)
        buf_none.seek(0)
        read_tags_none = protocol._read_tags(buf_none)
        self.assertIsNone(read_tags_none)

        # Test with empty dict
        buf_empty = io.BytesIO()
        protocol._write_tags(buf_empty, {})
        buf_empty.seek(0)
        read_tags_empty = protocol._read_tags(buf_empty)
        self.assertIsNone(read_tags_empty)

    def test_fields_rw(self):
        """Tests writing and reading a complex fields block."""
        buf = io.BytesIO()
        fields = {
            "float_val": 123.456,
            "int_val": -789,
            "str_val": "this is a test string",
            "bool_val_true": True,
            "bool_val_false": False,
        }
        protocol._write_fields(buf, fields)
        buf.seek(0)
        read_fields = protocol._read_fields(buf)
        self.assertEqual(fields, read_fields)

    def test_encode_push_request(self):
        """Verifies the binary format of an encoded PUSH request."""
        metric = "cpu.load"
        tags = {"host": "worker-1"}
        timestamp = 1678886400
        fields = {"value": 99.9, "cores": 8}

        payload = protocol.encode_push_request(metric, tags, timestamp, fields)
        
        # Manually decode to verify structure
        r = io.BytesIO(payload)
        read_metric = protocol._read_string(r)
        self.assertEqual(metric, read_metric)

        read_tags = protocol._read_tags(r)
        self.assertEqual(tags, read_tags)

        read_ts = struct.unpack('>q', r.read(8))[0]
        self.assertEqual(timestamp, read_ts)

        read_fields = protocol._read_fields(r)
        self.assertEqual(fields, read_fields)

    def test_decode_manipulate_response(self):
        """Tests decoding a successful ManipulateResponse."""
        buf = io.BytesIO()
        buf.write(struct.pack('>B', protocol.RESP_OK)) # status
        buf.write(struct.pack('>Q', 2)) # rows_affected
        buf.write(struct.pack('>H', 2)) # seq_id_count
        buf.write(struct.pack('>Q', 101)) # seq_id 1
        buf.write(struct.pack('>Q', 102)) # seq_id 2
        buf.seek(0)

        resp = protocol.decode_manipulate_response(buf)
        self.assertEqual("ok", resp["status"])
        self.assertEqual(2, resp["rows_affected"])
        self.assertEqual([101, 102], resp["sequence_ids"])

    def test_decode_query_response_stream(self):
        """Tests decoding a streaming query response (part and end)."""
        # 1. Create a QueryResultPart payload
        part_buf = io.BytesIO()
        part_buf.write(struct.pack('>B', protocol.RESP_OK))
        part_buf.write(struct.pack('>B', 0)) # flags
        protocol._write_string(part_buf, "next_cursor_token")
        part_buf.write(struct.pack('>I', 1)) # point count
        # point data
        part_buf.write(struct.pack('>Q', 123)) # seq id
        protocol._write_string(part_buf, "test.metric")
        protocol._write_tags(part_buf, {"dc": "us-east"})
        part_buf.write(struct.pack('>q', 1234567890)) # timestamp
        protocol._write_fields(part_buf, {"value": 1.23, "status": "ok"})
        part_buf.seek(0)

        decoded_part = protocol.decode_query_response_part(part_buf)
        self.assertEqual("ok", decoded_part["status"])
        self.assertEqual("next_cursor_token", decoded_part["next_cursor"])
        self.assertEqual(1, len(decoded_part["results"]))
        self.assertEqual("test.metric", decoded_part["results"][0]["metric"])
        self.assertEqual(1.23, decoded_part["results"][0]["fields"]["value"])

        # 2. Create a QueryEndResponse payload
        end_buf = io.BytesIO()
        end_buf.write(struct.pack('>B', protocol.RESP_OK))
        end_buf.write(struct.pack('>Q', 100)) # total_rows
        protocol._write_string(end_buf, "Query finished.")
        end_buf.seek(0)

        decoded_end = protocol.decode_query_end_response(end_buf)
        self.assertEqual("ok", decoded_end["status"])
        self.assertEqual(100, decoded_end["total_rows"])
        self.assertEqual("Query finished.", decoded_end["message"])

    def test_frame_rw_success(self):
        """Tests writing and reading a valid frame."""
        write_buffer = io.BytesIO()
        mock_write_socket = MagicMock()
        mock_write_socket.sendall.side_effect = write_buffer.write

        cmd_type = protocol.CMD_QUERY
        payload = b"QUERY cpu.usage FROM RELATIVE(1m)"
        
        protocol.write_frame(mock_write_socket, cmd_type, payload)

        class MockSocket:
            def __init__(self, buffer): self._buffer = buffer
            def recv(self, num_bytes): return self._buffer.read(num_bytes)
        
        read_cmd, read_payload = protocol.read_frame(MockSocket(io.BytesIO(write_buffer.getvalue())))

        self.assertEqual(cmd_type, read_cmd)
        self.assertEqual(payload, read_payload)

    def test_frame_rw_invalid_crc(self):
        """Tests that reading a frame with an invalid CRC raises an error."""
        write_buffer = io.BytesIO()
        mock_write_socket = MagicMock()
        mock_write_socket.sendall.side_effect = write_buffer.write
        cmd_type = protocol.CMD_PUSH
        payload = b"some data"
        
        protocol.write_frame(mock_write_socket, cmd_type, payload)
        
        frame_bytes = bytearray(write_buffer.getvalue())
        frame_bytes[-1] ^= 0xFF # Flip a bit to corrupt checksum
        corrupted_sock_buf = io.BytesIO(frame_bytes)

        class MockSocket:
            def __init__(self, buffer): self._buffer = buffer
            def recv(self, num_bytes): return self._buffer.read(num_bytes)

        with self.assertRaises(ConnectionError) as cm:
            protocol.read_frame(MockSocket(corrupted_sock_buf))
        
        self.assertIn("Checksum mismatch", str(cm.exception))

    def test_frame_rw_incomplete_frame(self):
        """Tests that reading an incomplete frame raises EOFError."""
        payload = b"some data"
        header = struct.pack('>BI', protocol.CMD_PUSH, len(payload))
        checksum = protocol.crc32c(header + payload)
        frame = header + payload + struct.pack('>I', checksum)

        incomplete_frame_buf = io.BytesIO(frame[:-2]) # Truncate the frame

        class MockSocket:
            def __init__(self, buffer): self._buffer = buffer
            def recv(self, num_bytes): return self._buffer.read(num_bytes)

        with self.assertRaises(EOFError):
            protocol.read_frame(MockSocket(incomplete_frame_buf))

if __name__ == '__main__':
    unittest.main()

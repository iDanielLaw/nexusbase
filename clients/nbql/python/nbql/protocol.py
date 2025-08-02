import struct
import io
import json
from crc32c import crc32c
from .exceptions import ConnectionError

# Command Types (must match Go server)
CMD_PUSH = 0x01
CMD_PUSHS = 0x02
CMD_QUERY = 0x10
CMD_MANIPULATE = 0x20
CMD_QUERY_RESULT_PART = 0x11
CMD_QUERY_END = 0x12
CMD_ERROR = 0xEE

# Response Status Types (must match Go server)
RESP_OK = 0x00
RESP_ERROR = 0x01
RESP_DATA_ROW = 0x10
RESP_DATA_END = 0x11

# Field Types (must match Go server's core.FieldValueType)
FIELD_TYPE_FLOAT = 1
FIELD_TYPE_INTEGER = 2
FIELD_TYPE_STRING = 3
FIELD_TYPE_BOOL = 4

# PointItemFlag (must match Go server)
FLAG_WITH_SEQUENCE_ID = 0x01
FLAG_IS_AGGREGATED = 0x02

def _write_string(w, s):
    """Writes a length-prefixed string (uint16 length)."""
    b = s.encode('utf-8')
    w.write(struct.pack('>H', len(b)))
    if len(b) > 0:
        w.write(b)

def _read_string(r):
    """Reads a length-prefixed string (uint16 length)."""
    len_bytes = r.read(2)
    if len(len_bytes) < 2:
        raise EOFError("Unexpected EOF while reading string length")
    length = struct.unpack('>H', len_bytes)[0]
    if length == 0:
        return ""
    b = r.read(length)
    if len(b) < length:
        raise EOFError("Unexpected EOF while reading string content")
    return b.decode('utf-8')

def _write_tags(w, tags):
    """Writes a map of tags."""
    if tags is None:
        tags = {}
    w.write(struct.pack('>H', len(tags)))
    for k, v in tags.items():
        _write_string(w, k)
        _write_string(w, v)

def _read_tags(r):
    """Reads a map of tags."""
    count_bytes = r.read(2)
    if len(count_bytes) < 2:
        raise EOFError("Unexpected EOF while reading tag count")
    count = struct.unpack('>H', count_bytes)[0]
    if count == 0:
        return None
    tags = {}
    for _ in range(count):
        k = _read_string(r)
        v = _read_string(r)
        tags[k] = v
    return tags

def _write_fields(w, fields):
    """
    Encodes a dictionary of fields into a binary format by first writing the
    total length of the encoded block (uint32), followed by the block itself.
    """
    if fields is None:
        fields = {}
    
    # Use a temporary buffer to calculate the total length first
    field_buf = io.BytesIO()
    field_buf.write(struct.pack('>H', len(fields)))
    for key, value in fields.items():
        _write_string(field_buf, key)
        if isinstance(value, float):
            field_buf.write(struct.pack('>B', FIELD_TYPE_FLOAT))
            field_buf.write(struct.pack('>d', value))
        elif isinstance(value, int):
            field_buf.write(struct.pack('>B', FIELD_TYPE_INTEGER))
            field_buf.write(struct.pack('>q', value))
        elif isinstance(value, str):
            field_buf.write(struct.pack('>B', FIELD_TYPE_STRING))
            # String values within a field list are prefixed with a uint32 length,
            # unlike other strings in the protocol (e.g., keys, tags).
            b = value.encode('utf-8')
            field_buf.write(struct.pack('>I', len(b)))
            if len(b) > 0:
                field_buf.write(b)
        elif isinstance(value, bool):
            field_buf.write(struct.pack('>B', FIELD_TYPE_BOOL))
            field_buf.write(struct.pack('>?', value))
        else:
            # Fallback to JSON for unknown types, though server might not support this
            # For this client, we'll stick to the basic types.
            raise TypeError(f"Unsupported field type for key '{key}': {type(value)}")

    # Now, write the total length of the encoded block, followed by the block itself.
    encoded_data = field_buf.getvalue()
    w.write(struct.pack('>I', len(encoded_data)))
    w.write(encoded_data)

def _read_fields(r):
    """Reads a block of fields."""
    len_bytes = r.read(4)
    if len(len_bytes) < 4:
        raise EOFError("Unexpected EOF while reading fields length")
    total_len = struct.unpack('>I', len_bytes)[0]

    block_bytes = r.read(total_len)
    if len(block_bytes) < total_len:
        raise EOFError("Unexpected EOF while reading fields block")
    
    block_reader = io.BytesIO(block_bytes)
    
    num_fields_bytes = block_reader.read(2)
    if len(num_fields_bytes) < 2:
        raise EOFError("Unexpected EOF while reading number of fields")
    num_fields = struct.unpack('>H', num_fields_bytes)[0]

    fields = {}
    for _ in range(num_fields):
        key = _read_string(block_reader)
        type_byte = block_reader.read(1)
        field_type = struct.unpack('>B', type_byte)[0]
        if field_type == FIELD_TYPE_FLOAT:
            value = struct.unpack('>d', block_reader.read(8))[0]
        elif field_type == FIELD_TYPE_INTEGER:
            value = struct.unpack('>q', block_reader.read(8))[0]
        elif field_type == FIELD_TYPE_STRING:
            # String values within a field list are prefixed with a uint32 length.
            len_bytes = block_reader.read(4)
            if len(len_bytes) < 4:
                raise EOFError("Unexpected EOF while reading string field length")
            length = struct.unpack('>I', len_bytes)[0]
            if length == 0:
                value = ""
            else:
                str_bytes = block_reader.read(length)
                if len(str_bytes) < length:
                    raise EOFError("Unexpected EOF while reading string field content")
                value = str_bytes.decode('utf-8')
        elif field_type == FIELD_TYPE_BOOL:
            value = struct.unpack('>?', block_reader.read(1))[0]
        fields[key] = value
    return fields

def encode_push_request(metric, tags, timestamp, fields):
    """Encodes a PUSH request payload."""
    buf = io.BytesIO()
    _write_string(buf, metric)
    _write_tags(buf, tags)
    buf.write(struct.pack('>q', timestamp))
    _write_fields(buf, fields)
    return buf.getvalue()

def encode_pushs_request(points):
    """Encodes a PUSHS (bulk) request payload."""
    buf = io.BytesIO()
    buf.write(struct.pack('>I', len(points)))
    for point in points:
        _write_string(buf, point.get('metric', ''))
        _write_tags(buf, point.get('tags'))
        buf.write(struct.pack('>q', point.get('timestamp', 0)))
        _write_fields(buf, point.get('fields'))
    return buf.getvalue()

def encode_query_request(query_string):
    """Encodes a QUERY request payload."""
    buf = io.BytesIO()
    _write_string(buf, query_string)
    return buf.getvalue()

def decode_manipulate_response(r):
    """Decodes a ManipulateResponse payload."""
    status = struct.unpack('>B', r.read(1))[0]
    rows_affected = struct.unpack('>Q', r.read(8))[0]
    
    seq_id_count = struct.unpack('>H', r.read(2))[0]
    seq_ids = []
    for _ in range(seq_id_count):
        seq_id = struct.unpack('>Q', r.read(8))[0]
        seq_ids.append(seq_id)

    return {
        "status": "ok" if status == RESP_OK else "error",
        "rows_affected": rows_affected,
        "sequence_ids": seq_ids,
    }

def decode_error_response(r):
    """Decodes a binary ErrorResponse payload from the server."""
    try:
        code_bytes = r.read(2)
        if len(code_bytes) < 2:
            raise EOFError("Incomplete error code")
        code = struct.unpack('>H', code_bytes)[0]
        message = _read_string(r)
        return {"code": code, "message": message}
    except EOFError:
        return {"message": "Received malformed error response from server"}

def _read_aggregated_values(r):
    """Reads a map of aggregated values."""
    count_bytes = r.read(2)
    if len(count_bytes) < 2:
        raise EOFError("Unexpected EOF while reading aggregated value count")
    count = struct.unpack('>H', count_bytes)[0]
    if count == 0:
        return None
    values = {}
    for _ in range(count):
        key = _read_string(r)
        value_bytes = r.read(8)
        if len(value_bytes) < 8:
            raise EOFError("Unexpected EOF while reading aggregated value")
        value = struct.unpack('>d', value_bytes)[0]
        values[key] = value
    return values

def decode_query_response_part(r):
    """Decodes a QueryResultPart payload."""
    status = struct.unpack('>B', r.read(1))[0]
    flags = struct.unpack('>B', r.read(1))[0]
    next_cursor = _read_string(r)
    
    count = struct.unpack('>I', r.read(4))[0]
    results = []
    for _ in range(count):
        point = {}
        point['sequence_id'] = struct.unpack('>Q', r.read(8))[0]
        point['metric'] = _read_string(r)
        point['tags'] = _read_tags(r)
        point['timestamp'] = struct.unpack('>q', r.read(8))[0]
        
        if flags & FLAG_IS_AGGREGATED:
            point['aggregated_values'] = _read_aggregated_values(r)
        else:
            point['fields'] = _read_fields(r)
        results.append(point)
    
    return {
        "status": "ok" if status == RESP_OK else "error",
        "flags": flags,
        "next_cursor": next_cursor,
        "results": results
    }

def decode_query_end_response(r):
    """Decodes a QueryEndResponse payload."""
    status = struct.unpack('>B', r.read(1))[0]
    total_rows = struct.unpack('>Q', r.read(8))[0]
    message = _read_string(r)
    return {
        "status": "ok" if status == RESP_OK else "error",
        "total_rows": total_rows,
        "message": message
    }

def _read_bytes(sock, num_bytes):
    """Helper to read an exact number of bytes from a socket, handling short reads."""
    if num_bytes == 0:
        return b''
    chunks = []
    bytes_recvd = 0
    while bytes_recvd < num_bytes:
        chunk = sock.recv(num_bytes - bytes_recvd)
        if not chunk:
            # This indicates the connection was closed prematurely.
            raise EOFError("Socket connection broken while reading")
        chunks.append(chunk)
        bytes_recvd += len(chunk)
    return b''.join(chunks)

def read_frame(sock):
    """
    Reads a complete binary frame from the socket.
    Returns (command_type, payload_bytes)
    """
    try:
        # 1. Read the 5-byte header (1-byte command type + 4-byte frame length)
        header_bytes = _read_bytes(sock, 5)
        cmd_type, frame_len = struct.unpack('>BI', header_bytes)

        # The frame_len from the header is the length of the payload PLUS the 4-byte checksum.
        if frame_len < 4: # Frame must be at least 4 bytes for the checksum.
            raise ConnectionError(f"Invalid frame length received: {frame_len}. Must be at least 4.")

        # 2. Read the rest of the frame (payload + checksum)
        frame_body_bytes = _read_bytes(sock, frame_len)
        
        # 3. Separate the actual payload from the checksum
        payload = frame_body_bytes[:-4]
        checksum_bytes = frame_body_bytes[-4:]
        received_checksum = struct.unpack('>I', checksum_bytes)[0]

    except EOFError as e:
        # If we get an EOF at any point, it means the frame was incomplete.
        raise EOFError("Incomplete frame received") from e

    # 4. Verify the checksum against the header and the payload.
    calculated_checksum = crc32c(header_bytes + payload)
    if received_checksum != calculated_checksum:
        raise ConnectionError(f"Checksum mismatch. Got {received_checksum}, expected {calculated_checksum}")

    return cmd_type, payload

def write_frame(sock, cmd_type, payload):
    """
    Constructs and writes a complete binary frame to the socket, mirroring the Go server's logic.
    """
    # The length in the header is the length of the payload PLUS the 4-byte checksum.
    header = struct.pack('>BI', cmd_type, len(payload) + 4)
    checksum = crc32c(header + payload)
    frame = header + payload + struct.pack('>I', checksum)
    sock.sendall(frame)

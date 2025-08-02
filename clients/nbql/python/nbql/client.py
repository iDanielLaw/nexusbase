import socket
import json
import io
from .exceptions import ConnectionError, APIError
from . import protocol

class Client:
    """
    A client for interacting with a Nexusbase (NBQL) server.
    """
    def __init__(self, host='127.0.0.1', port=50052, timeout=30, _sock=None):
        """
        Initializes the NBQL client.

        Args:
            host (str): The server host.
            port (int): The server port.
            timeout (int): Request timeout in seconds.
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = _sock # Can be pre-set for testing

    def _connect(self):
        """Establishes a connection to the server."""
        if self._sock:
            return
        try:
            self._sock = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error as e:
            self._sock = None
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}") from e

    def _send_frame_and_receive_response(self, cmd_type, payload):
        """
        Internal method to send a binary frame and receive a response.
        This handles both single and multi-frame (streaming) responses.
        """
        self._connect()
        try:
            protocol.write_frame(self._sock, cmd_type, payload)
            
            # Read the first response frame
            resp_cmd_type, resp_payload = protocol.read_frame(self._sock)

            if resp_cmd_type == protocol.CMD_ERROR:
                # The payload is a JSON error message from the server
                # The payload is a binary error message from the server
                reader = io.BytesIO(resp_payload)
                error_info = protocol.decode_error_response(reader)
                raise APIError(error_info.get('message', 'Unknown server error'), status_code=error_info.get('code'))
            
            return resp_cmd_type, resp_payload

        except (socket.error, BrokenPipeError, EOFError) as e:
            self.close()
            raise ConnectionError(f"Socket error during communication: {e}") from e

    def _quote_param(self, param):
        """Safely quotes a parameter for use in an NBQL query."""
        if isinstance(param, (int, float)):
            return str(param)
        if isinstance(param, str):
            # Escape double quotes by doubling them, and wrap the whole thing in double quotes.
            return '"' + param.replace('"', '""') + '"'
        # For other types, we could raise an error or try to convert to string.
        # Raising an error is safer to avoid unexpected behavior.
        raise TypeError(f"Unsupported parameter type for query: {type(param)}")

    def _format_query(self, query_template, params):
        """Formats a query by substituting placeholders with quoted parameters."""
        parts = query_template.split('?')
        if len(parts) - 1 != len(params):
            raise ValueError(
                f"Query placeholder mismatch: {len(parts) - 1} placeholders ('?') found, "
                f"but {len(params)} parameters were provided."
            )

        result = []
        for i, part in enumerate(parts):
            result.append(part)
            if i < len(params):
                result.append(self._quote_param(params[i]))

        return "".join(result)

    def query(self, q, *params):
        """
        Executes an NBQL query, with optional parameter substitution to prevent injection.

        This method allows for safe substitution of values into a query.
        It is designed to prevent NBQL injection attacks by correctly quoting
        and escaping parameters.

        Example:
            client.query("QUERY cpu.usage TAGGED (host=?)", "server-01")
            # This will execute: QUERY cpu.usage TAGGED (host="server-01")

        Note: This mechanism is primarily for parameter *values* (e.g., tag values),
        but can also be used for identifiers like metric names.

        Args:
            q (str): The NBQL query string. It can contain '?' as placeholders
                     for parameters.
            *params: A variable number of arguments to be safely substituted into
                     the query string's placeholders.

        Returns:
            dict: The result from the server.

        Raises:
            ConnectionError: If there's a network problem.
            APIError: If the server returns an error.
            ValueError: If the number of parameters does not match placeholders.
            TypeError: If a parameter has an unsupported type.
        """
        if params:
            q = self._format_query(q, params)

        payload = protocol.encode_query_request(q)
        
        # A query can have a streaming response. We need to handle it.
        self._connect()
        try:
            protocol.write_frame(self._sock, protocol.CMD_QUERY, payload)

            all_results = []
            final_response = {}

            while True:
                resp_cmd_type, resp_payload = protocol.read_frame(self._sock)
                
                if resp_cmd_type == protocol.CMD_ERROR:
                    # The payload is a JSON error message from the server
                    reader = io.BytesIO(resp_payload)
                    error_info = protocol.decode_error_response(reader)
                    raise APIError(error_info.get('message', 'Unknown server error'), status_code=error_info.get('code'))

                elif resp_cmd_type == protocol.CMD_QUERY_RESULT_PART:
                    reader = io.BytesIO(resp_payload)
                    part = protocol.decode_query_response_part(reader)
                    if part.get("results"):
                        all_results.extend(part["results"])

                elif resp_cmd_type == protocol.CMD_QUERY_END:
                    reader = io.BytesIO(resp_payload)
                    final_response = protocol.decode_query_end_response(reader)
                    break  # End of stream

                else:
                    raise APIError(f"Unexpected command type {resp_cmd_type} received during query.")

            # Combine results into a single response object
            final_response['results'] = all_results
            return final_response

        except (socket.error, BrokenPipeError, EOFError) as e:
            self.close()
            raise ConnectionError(f"Socket error during query: {e}") from e

    def push(self, metric, value, timestamp=None, tags=None):
        """
        Pushes a single data point.

        Args:
            metric (str): The name of the metric.
            value (float or int): The value of the data point.
            timestamp (int, optional): Unix timestamp. Defaults to server time.
            tags (dict, optional): A dictionary of tags.

        Returns:
            dict: The result from the server.
        """
        ts = timestamp or 0
        fields = {'value': value} # Legacy push uses a single 'value' field
        payload = protocol.encode_push_request(metric, tags, ts, fields)
        
        resp_cmd_type, resp_payload = self._send_frame_and_receive_response(protocol.CMD_PUSH, payload)

        if resp_cmd_type != protocol.CMD_MANIPULATE:
            raise APIError(f"Unexpected response type for PUSH: {resp_cmd_type}")

        return protocol.decode_manipulate_response(io.BytesIO(resp_payload))

    def push_bulk(self, points, chunk_size=None):
        """
        Pushes multiple data points in a single request.

        Args:
            points (list[dict]): A list of data points. Each point is a dictionary
                                 with keys 'metric', 'fields', and optional 'timestamp' and 'tags'.
            chunk_size (int, optional): If specified, sends points in chunks of this size.

        Returns:
            dict: The result from the server for the last chunk sent.
        """
        if not points:
            return {'status': 'ok', 'message': 'No points to push.'}

        for point in points:
            if not isinstance(point, dict) or 'metric' not in point or 'fields' not in point:
                raise ValueError(f"Each point must be a dict and contain 'metric' and 'fields' keys. Invalid point: {point}")

        if not chunk_size:
            payload = protocol.encode_pushs_request(points)
            resp_cmd_type, resp_payload = self._send_frame_and_receive_response(protocol.CMD_PUSHS, payload)
            if resp_cmd_type != protocol.CMD_MANIPULATE:
                raise APIError(f"Unexpected response type for PUSHS: {resp_cmd_type}")
            return protocol.decode_manipulate_response(io.BytesIO(resp_payload))
        else:
            last_response = None
            for i in range(0, len(points), chunk_size):
                chunk = points[i:i + chunk_size]
                if not chunk:
                    continue
                payload = protocol.encode_pushs_request(chunk)
                resp_cmd_type, resp_payload = self._send_frame_and_receive_response(protocol.CMD_PUSHS, payload)
                if resp_cmd_type != protocol.CMD_MANIPULATE:
                    raise APIError(f"Unexpected response type for PUSHS chunk: {resp_cmd_type}")
                last_response = protocol.decode_manipulate_response(io.BytesIO(resp_payload))
                if last_response.get('status') == 'error':
                    return last_response
            return last_response or {'status': 'ok', 'message': 'No points to push.'}


    def close(self):
        """Closes the connection to the server."""
        if self._sock:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

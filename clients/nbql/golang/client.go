package nbql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/clients/nbql/golang/core"
	"github.com/INLOpen/nexusbase/clients/nbql/golang/protocol"
)

// Options holds configuration for the client.
type Options struct {
	Address  string
	Username string
	Password string
	Logger   *slog.Logger
}

// Client is a client for the NBQL TCP protocol.
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	logger *slog.Logger
	mu     sync.Mutex // Protects writes to the connection
}

// Row represents a single row of data returned from a query.
type Row struct {
	Timestamp        int64
	Tags             map[string]string
	Fields           map[string]interface{}
	AggregatedValues map[string]float64
	IsAggregated     bool
}

// QueryResult holds the results of a successful query.
type QueryResult struct {
	Rows      []Row
	TotalRows uint64
}

// Point represents a single data point for bulk insertion.
type Point struct {
	Metric    string
	Tags      map[string]string
	Fields    map[string]interface{}
	Timestamp int64
}

// Connect establishes a connection to the server and performs authentication.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	if opts.Address == "" {
		return nil, fmt.Errorf("address is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", opts.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server at %s: %w", opts.Address, err)
	}

	c := &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		logger: opts.Logger,
	}

	// Perform authentication if credentials are provided
	if opts.Username != "" {
		if err := c.authenticate(ctx, opts.Username, opts.Password); err != nil {
			c.Close() // Close connection on auth failure
			return nil, err
		}
		c.logger.Debug("Authentication successful")
	}

	return c, nil
}

// Close terminates the connection to the server.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// authenticate handles the initial handshake with the server.
func (c *Client) authenticate(ctx context.Context, username, password string) error {
	authRequest := &protocol.AuthenticationPacket{
		Version: 1,
		Op:      protocol.ConnectRequestAuthenticationOp,
		Payload: &protocol.RequestAuthenticationPacket{
			Username: username,
			Password: password,
		},
	}

	reqData, err := authRequest.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal auth request: %w", err)
	}

	if _, err := c.writer.Write(reqData); err != nil {
		return fmt.Errorf("failed to send auth request: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush auth request: %w", err)
	}

	// Read the response header to determine payload length.
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return fmt.Errorf("failed to read auth response header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint16(header[2:4])
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return fmt.Errorf("failed to read auth response payload: %w", err)
		}
	}

	respPacket := &protocol.AuthenticationPacket{Payload: &protocol.ResponseAuthenticationPacket{}}
	if err := respPacket.UnmarshalBinary(append(header, payload...)); err != nil {
		return fmt.Errorf("failed to unmarshal auth response: %w", err)
	}

	respPayload := respPacket.Payload.(*protocol.ResponseAuthenticationPacket)
	if respPayload.Status != protocol.ResponseOK {
		return fmt.Errorf("server authentication failed: %s", respPayload.Message)
	}

	return nil
}

// PushBulk sends multiple data points to the server in a single batch.
// This is more efficient than sending points one by one with Push.
func (c *Client) PushBulk(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil // Nothing to do
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	pushItems := make([]protocol.PushItem, len(points))
	now := time.Now().UnixNano() // Fallback timestamp

	for i, p := range points {
		fieldValues, err := core.NewFieldValuesFromMap(p.Fields)
		if err != nil {
			return fmt.Errorf("failed to create field values for point %d (metric: %s): %w", i, p.Metric, err)
		}

		ts := p.Timestamp
		if ts == 0 {
			ts = now
		}

		pushItems[i] = protocol.PushItem{
			Metric:    p.Metric,
			Tags:      p.Tags,
			Timestamp: ts,
			Fields:    fieldValues,
		}
	}

	req := protocol.PushsRequest{Items: pushItems}
	payload := new(bytes.Buffer)
	if err := protocol.EncodePushsRequest(payload, req); err != nil {
		return fmt.Errorf("failed to encode pushs request: %w", err)
	}

	if err := protocol.WriteFrame(c.writer, protocol.CommandPushs, payload.Bytes()); err != nil {
		return fmt.Errorf("failed to write pushs frame: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush pushs frame: %w", err)
	}

	// Read response
	cmdType, respPayload, err := protocol.ReadFrame(c.reader)
	if err != nil {
		return fmt.Errorf("failed to read pushs response: %w", err)
	}

	if cmdType == protocol.CommandError {
		return protocol.DecodeErrorMessage(bytes.NewReader(respPayload))
	}

	if cmdType != protocol.CommandManipulate {
		return fmt.Errorf("unexpected response type for PUSHS: %v", cmdType)
	}

	return nil
}

// quoteParam safely quotes a parameter for use in an NBQL query.
func (c *Client) quoteParam(param interface{}) (string, error) {
	switch v := param.(type) {
	case string:
		// Escape double quotes by doubling them, and wrap the whole thing in double quotes.
		return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		// Use a general format that avoids scientific notation for most common cases.
		return fmt.Sprintf("%g", v), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	default:
		// For other types, we could raise an error or try to convert to string.
		// Raising an error is safer to avoid unexpected behavior.
		return "", fmt.Errorf("unsupported parameter type for query: %T", v)
	}
}

// formatQuery formats a query by substituting placeholders with quoted parameters.
func (c *Client) formatQuery(queryTemplate string, params []interface{}) (string, error) {
	parts := strings.Split(queryTemplate, "?")
	if len(parts)-1 != len(params) {
		return "", fmt.Errorf("query placeholder mismatch: %d placeholders ('?') found, but %d parameters were provided", len(parts)-1, len(params))
	}

	var result strings.Builder
	// Estimate capacity to avoid reallocations
	result.Grow(len(queryTemplate) + len(params)*10)

	for i, part := range parts {
		result.WriteString(part)
		if i < len(params) {
			quotedParam, err := c.quoteParam(params[i])
			if err != nil {
				return "", err
			}
			result.WriteString(quotedParam)
		}
	}
	return result.String(), nil
}

// Query sends a query to the server and returns the structured results.
// It supports parameterized queries to prevent NBQL injection by safely quoting
// and escaping parameters. Use '?' as a placeholder for parameters.
func (c *Client) Query(ctx context.Context, query string, params ...interface{}) (*QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	finalQuery := query
	if len(params) > 0 {
		var err error
		finalQuery, err = c.formatQuery(query, params)
		if err != nil {
			return nil, fmt.Errorf("failed to format query: %w", err)
		}
	}

	payload := new(bytes.Buffer)
	if err := protocol.EncodeQueryRequest(payload, protocol.QueryRequest{QueryString: finalQuery}); err != nil {
		return nil, fmt.Errorf("failed to encode query request: %w", err)
	}

	if err := protocol.WriteFrame(c.writer, protocol.CommandQuery, payload.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write query frame: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush query frame: %w", err)
	}

	result := &QueryResult{
		Rows: make([]Row, 0),
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		cmdType, packet, err := protocol.ReadFrame(c.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read response frame: %w", err)
		}

		switch cmdType {
		case protocol.CommandQueryResultPart:
			resp, err := protocol.DecodeQueryResponse(bytes.NewReader(packet))
			
			if err != nil {
				return nil, fmt.Errorf("failed to decode query result part: %w", err)
			}

			for _, item := range resp.Results {
				row := Row{
					Timestamp:        item.Timestamp,
					Tags:             item.Tags,
					IsAggregated:     resp.Flags&protocol.PointItemFlagIsAggregated != 0,
					AggregatedValues: item.AggregatedValues,
				}
				if item.Fields != nil {
					row.Fields = item.Fields.ToMap()
				}
				result.Rows = append(result.Rows, row)
			}
		case protocol.CommandQueryEnd:
			resp, err := protocol.DecodeQueryEndResponse(bytes.NewReader(packet))
			if err != nil {
				return nil, fmt.Errorf("failed to decode query end response: %w", err)
			}
			result.TotalRows = resp.TotalRows
			return result, nil
		case protocol.CommandError:
			return nil, protocol.DecodeErrorMessage(bytes.NewReader(packet))
		default:
			return nil, fmt.Errorf("received unexpected command type from server: %v", cmdType)
		}
	}
}

// Push sends a single data point to the server using the binary fast path.
func (c *Client) Push(ctx context.Context, metric string, tags map[string]string, fields map[string]interface{}, timestamp int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fieldValues, err := core.NewFieldValuesFromMap(fields)
	if err != nil {
		return fmt.Errorf("failed to create field values: %w", err)
	}

	req := protocol.PushRequest{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
		Fields:    fieldValues,
	}

	payload := new(bytes.Buffer)
	if err := protocol.EncodePushRequest(payload, req); err != nil {
		return fmt.Errorf("failed to encode push request: %w", err)
	}

	if err := protocol.WriteFrame(c.writer, protocol.CommandPush, payload.Bytes()); err != nil {
		return fmt.Errorf("failed to write push frame: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush push frame: %w", err)
	}

	// Read response
	cmdType, respPayload, err := protocol.ReadFrame(c.reader)
	
	if err != nil {
		return fmt.Errorf("failed to read push response: %w", err)
	}

	if cmdType == protocol.CommandError {
		return protocol.DecodeErrorMessage(bytes.NewReader(respPayload))
	}

	if cmdType != protocol.CommandManipulate {
		return fmt.Errorf("unexpected response type for PUSH: %v", cmdType)
	}

	return nil
}

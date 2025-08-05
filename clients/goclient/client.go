package goclient

import (
	"context"
	"fmt"

	pb "github.com/INLOpen/nexusbase/clients/goclient/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// Client is a client for the TSDB gRPC server.
type Client struct {
	conn   *grpc.ClientConn
	client pb.TSDBServiceClient
}

// NewClient creates a new TSDB client.
func NewClient(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewTSDBServiceClient(conn),
	}, nil
}

// Close closes the connection to the server.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Put writes a single data point to the database.
func (c *Client) Put(ctx context.Context, dp DataPoint) error {
	return c.PutBatch(ctx, []DataPoint{dp})
}

// PutBatch writes multiple data points to the database.
func (c *Client) PutBatch(ctx context.Context, dps []DataPoint) error {
	pbPoints := make([]*pb.PutRequest, len(dps))
	for i, dp := range dps {
		fields, err := structpb.NewStruct(dp.Fields)
		if err != nil {
			return fmt.Errorf("failed to convert fields for point %d: %w", i, err)
		}
		pbPoints[i] = &pb.PutRequest{
			Metric:    dp.Metric,
			Tags:      dp.Tags,
			Timestamp: dp.Timestamp,
			Fields:    fields,
		}
	}

	req := &pb.PutBatchRequest{Points: pbPoints}
	_, err := c.client.PutBatch(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC PutBatch failed: %w", err)
	}
	return nil
}

// Get retrieves a single data point from the database.
func (c *Client) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (map[string]interface{}, error) {
	req := &pb.GetRequest{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
	}

	res, err := c.client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC Get failed: %w", err)
	}

	if res.Fields == nil {
		return make(map[string]interface{}), nil
	}

	return res.Fields.AsMap(), nil
}

// Query executes a query against the database.
func (c *Client) Query(ctx context.Context, params QueryParams) (*QueryResultIterator, error) {
	pbAggs := make([]*pb.AggregationSpec, len(params.AggregationSpecs))
	for i, agg := range params.AggregationSpecs {
		pbAggs[i] = &pb.AggregationSpec{
			Function: pb.AggregationSpec_AggregationFunc(pb.AggregationSpec_AggregationFunc_value[agg.Function]),
			Field:    agg.Field,
		}
	}

	req := &pb.QueryRequest{
		Metric:             params.Metric,
		Tags:               params.Tags,
		StartTime:          params.StartTime,
		EndTime:            params.EndTime,
		AggregationSpecs:   pbAggs,
		DownsampleInterval: params.DownsampleInterval,
		Limit:              params.Limit,
	}

	stream, err := c.client.Query(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start gRPC Query stream: %w", err)
	}

	return &QueryResultIterator{stream: stream}, nil
}

// DeleteSeries removes all data points for a given series.
func (c *Client) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	req := &pb.DeleteSeriesRequest{
		Metric: metric,
		Tags:   tags,
	}
	_, err := c.client.DeleteSeries(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC DeleteSeries failed: %w", err)
	}
	return nil
}

// CreateSnapshot
func (c *Client) CreateSnapshot(ctx context.Context, SnapshotDir string) error {
	req :=&pb.CreateSnapshotRequest{
		SnapshotDir: SnapshotDir,
	}
	_, err := c.client.CreateSnapshot(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC CreateSnapshot failed: %w", err)
	}
	return nil

}
package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/luxfi/zap"
)

// Client is a ZAP-based replication RPC client.
type Client struct {
	node   *zap.Node
	peerID string
}

// NewClient creates a new ZAP client that connects to the worker at the given address.
func NewClient(address string, logger *slog.Logger) (*Client, error) {
	node := zap.NewNode(zap.NodeConfig{
		NodeID:      "ledger-api",
		ServiceType: "_ledger-repl._tcp",
		Port:        0, // ephemeral port for client
		Logger:      logger,
		NoDiscovery: true,
	})

	if err := node.Start(); err != nil {
		return nil, fmt.Errorf("starting client node: %w", err)
	}

	if err := node.ConnectDirect(address); err != nil {
		node.Stop()
		return nil, fmt.Errorf("connecting to worker at %s: %w", address, err)
	}

	return &Client{
		node:   node,
		peerID: "ledger-worker",
	}, nil
}

// Stop shuts down the client node.
func (c *Client) Stop() {
	c.node.Stop()
}

func (c *Client) call(ctx context.Context, op uint16, req any) (json.RawMessage, error) {
	var payload []byte
	if req != nil {
		var err error
		payload, err = json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("marshaling request: %w", err)
		}
	}

	b := zap.NewBuilder(len(payload) + 64)
	obj := b.StartObject(8)
	obj.SetBytes(0, payload)
	// Encode the opcode in the upper 8 bits of flags for handler dispatch
	obj.FinishAsRoot()
	msgBytes := b.FinishWithFlags(uint16(op) << 8)

	msg, err := zap.Parse(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("building request message: %w", err)
	}

	resp, err := c.node.Call(ctx, c.peerID, msg)
	if err != nil {
		return nil, fmt.Errorf("RPC call (op=%d): %w", op, err)
	}

	// Decode wire response
	respPayload := resp.Root().Bytes(0)
	var wire wireResponse
	if err := json.Unmarshal(respPayload, &wire); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if wire.Error != nil {
		return nil, &RPCError{Code: wire.Error.Code, Msg: wire.Error.Message}
	}

	return wire.Data, nil
}

// RPCError is returned when the server sends a structured error.
type RPCError struct {
	Code string
	Msg  string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

// IsCode checks if the error is an RPCError with the given code.
func IsCode(err error, code string) bool {
	if rpcErr, ok := err.(*RPCError); ok {
		return rpcErr.Code == code
	}
	return false
}

// Error codes matching the gRPC codes we previously used
const (
	CodeInvalidArgument   = "invalid_argument"
	CodeNotFound          = "not_found"
	CodeFailedPrecondition = "failed_precondition"
)

// Typed client methods

func (c *Client) CreateExporter(ctx context.Context, req *CreateExporterRequest) (*CreateExporterResponse, error) {
	data, err := c.call(ctx, OpCreateExporter, req)
	if err != nil {
		return nil, err
	}
	var resp CreateExporterResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) ListExporters(ctx context.Context) (*ListExportersResponse, error) {
	data, err := c.call(ctx, OpListExporters, nil)
	if err != nil {
		return nil, err
	}
	var resp ListExportersResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetExporter(ctx context.Context, id string) (*GetExporterResponse, error) {
	data, err := c.call(ctx, OpGetExporter, &IDRequest{ID: id})
	if err != nil {
		return nil, err
	}
	var resp GetExporterResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) UpdateExporter(ctx context.Context, req *UpdateExporterRequest) error {
	_, err := c.call(ctx, OpUpdateExporter, req)
	return err
}

func (c *Client) DeleteExporter(ctx context.Context, id string) error {
	_, err := c.call(ctx, OpDeleteExporter, &IDRequest{ID: id})
	return err
}

func (c *Client) ListPipelines(ctx context.Context) (*ListPipelinesResponse, error) {
	data, err := c.call(ctx, OpListPipelines, nil)
	if err != nil {
		return nil, err
	}
	var resp ListPipelinesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetPipeline(ctx context.Context, id string) (*GetPipelineResponse, error) {
	data, err := c.call(ctx, OpGetPipeline, &IDRequest{ID: id})
	if err != nil {
		return nil, err
	}
	var resp GetPipelineResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) CreatePipeline(ctx context.Context, req *CreatePipelineRequest) (*CreatePipelineResponse, error) {
	data, err := c.call(ctx, OpCreatePipeline, req)
	if err != nil {
		return nil, err
	}
	var resp CreatePipelineResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) DeletePipeline(ctx context.Context, id string) error {
	_, err := c.call(ctx, OpDeletePipeline, &IDRequest{ID: id})
	return err
}

func (c *Client) StartPipeline(ctx context.Context, id string) error {
	_, err := c.call(ctx, OpStartPipeline, &IDRequest{ID: id})
	return err
}

func (c *Client) StopPipeline(ctx context.Context, id string) error {
	_, err := c.call(ctx, OpStopPipeline, &IDRequest{ID: id})
	return err
}

func (c *Client) ResetPipeline(ctx context.Context, id string) error {
	_, err := c.call(ctx, OpResetPipeline, &IDRequest{ID: id})
	return err
}

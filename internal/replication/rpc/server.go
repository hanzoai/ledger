package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/luxfi/zap"
)

// Message type opcodes for ZAP handlers
const (
	OpCreateExporter uint16 = 0x01
	OpListExporters  uint16 = 0x02
	OpGetExporter    uint16 = 0x03
	OpUpdateExporter uint16 = 0x04
	OpDeleteExporter uint16 = 0x05
	OpListPipelines  uint16 = 0x06
	OpGetPipeline    uint16 = 0x07
	OpCreatePipeline uint16 = 0x08
	OpDeletePipeline uint16 = 0x09
	OpStartPipeline  uint16 = 0x0A
	OpStopPipeline   uint16 = 0x0B
	OpResetPipeline  uint16 = 0x0C
)

// ReplicationHandler is the server-side interface that the Manager must satisfy.
type ReplicationHandler interface {
	CreateExporter(ctx context.Context, req *CreateExporterRequest) (*CreateExporterResponse, *ErrorResponse)
	ListExporters(ctx context.Context) (*ListExportersResponse, *ErrorResponse)
	GetExporter(ctx context.Context, req *IDRequest) (*GetExporterResponse, *ErrorResponse)
	UpdateExporter(ctx context.Context, req *UpdateExporterRequest) (*ErrorResponse)
	DeleteExporter(ctx context.Context, req *IDRequest) *ErrorResponse
	ListPipelines(ctx context.Context) (*ListPipelinesResponse, *ErrorResponse)
	GetPipeline(ctx context.Context, req *IDRequest) (*GetPipelineResponse, *ErrorResponse)
	CreatePipeline(ctx context.Context, req *CreatePipelineRequest) (*CreatePipelineResponse, *ErrorResponse)
	DeletePipeline(ctx context.Context, req *IDRequest) *ErrorResponse
	StartPipeline(ctx context.Context, req *IDRequest) *ErrorResponse
	StopPipeline(ctx context.Context, req *IDRequest) *ErrorResponse
	ResetPipeline(ctx context.Context, req *IDRequest) *ErrorResponse
}

// Server wraps a ZAP node that serves replication RPC calls.
type Server struct {
	node *zap.Node
	addr string
}

// NewServer creates a ZAP-based replication server.
func NewServer(address string, handler ReplicationHandler, logger *slog.Logger) (*Server, error) {
	_, port, err := parseHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", address, err)
	}

	node := zap.NewNode(zap.NodeConfig{
		NodeID:      "ledger-worker",
		ServiceType: "_ledger-repl._tcp",
		Port:        port,
		Logger:      logger,
		NoDiscovery: true,
	})

	registerHandlers(node, handler)

	return &Server{node: node, addr: address}, nil
}

// Start starts the ZAP server.
func (s *Server) Start() error {
	return s.node.Start()
}

// Stop stops the ZAP server.
func (s *Server) Stop() {
	s.node.Stop()
}

// Addr returns the configured address.
func (s *Server) Addr() string {
	return s.addr
}

func registerHandlers(node *zap.Node, h ReplicationHandler) {
	node.Handle(OpCreateExporter, jsonHandler(func(ctx context.Context, req *CreateExporterRequest) (any, *ErrorResponse) {
		return h.CreateExporter(ctx, req)
	}))
	node.Handle(OpListExporters, jsonHandler(func(ctx context.Context, _ *struct{}) (any, *ErrorResponse) {
		return h.ListExporters(ctx)
	}))
	node.Handle(OpGetExporter, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return h.GetExporter(ctx, req)
	}))
	node.Handle(OpUpdateExporter, jsonHandler(func(ctx context.Context, req *UpdateExporterRequest) (any, *ErrorResponse) {
		return nil, h.UpdateExporter(ctx, req)
	}))
	node.Handle(OpDeleteExporter, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return nil, h.DeleteExporter(ctx, req)
	}))
	node.Handle(OpListPipelines, jsonHandler(func(ctx context.Context, _ *struct{}) (any, *ErrorResponse) {
		return h.ListPipelines(ctx)
	}))
	node.Handle(OpGetPipeline, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return h.GetPipeline(ctx, req)
	}))
	node.Handle(OpCreatePipeline, jsonHandler(func(ctx context.Context, req *CreatePipelineRequest) (any, *ErrorResponse) {
		return h.CreatePipeline(ctx, req)
	}))
	node.Handle(OpDeletePipeline, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return nil, h.DeletePipeline(ctx, req)
	}))
	node.Handle(OpStartPipeline, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return nil, h.StartPipeline(ctx, req)
	}))
	node.Handle(OpStopPipeline, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return nil, h.StopPipeline(ctx, req)
	}))
	node.Handle(OpResetPipeline, jsonHandler(func(ctx context.Context, req *IDRequest) (any, *ErrorResponse) {
		return nil, h.ResetPipeline(ctx, req)
	}))
}

// Wire envelope for responses
type wireResponse struct {
	Error *ErrorResponse `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// jsonHandler adapts a typed JSON request/response handler to a ZAP Handler.
func jsonHandler[Req any](fn func(ctx context.Context, req *Req) (any, *ErrorResponse)) zap.Handler {
	return func(ctx context.Context, from string, msg *zap.Message) (*zap.Message, error) {
		var req Req
		payload := msg.Root().Bytes(0)
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &req); err != nil {
				return buildResponse(nil, &ErrorResponse{Code: "invalid_request", Message: err.Error()})
			}
		}

		result, errResp := fn(ctx, &req)
		return buildResponse(result, errResp)
	}
}

func buildResponse(data any, errResp *ErrorResponse) (*zap.Message, error) {
	resp := wireResponse{Error: errResp}
	if data != nil && errResp == nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshaling response: %w", err)
		}
		resp.Data = raw
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshaling wire response: %w", err)
	}

	b := zap.NewBuilder(len(respBytes) + 64)
	obj := b.StartObject(8)
	obj.SetBytes(0, respBytes)
	obj.FinishAsRoot()
	return zap.Parse(b.Finish())
}

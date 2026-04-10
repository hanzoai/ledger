package rpc

import "time"

// Wire types for replication RPC. Plain Go structs, JSON-encoded over ZAP.

type Cursor struct {
	Next    string `json:"next,omitempty"`
	HasMore bool   `json:"has_more,omitempty"`
	Prev    string `json:"prev,omitempty"`
}

type ExporterConfiguration struct {
	Driver string `json:"driver"`
	Config string `json:"config"`
}

type Exporter struct {
	ID        string                `json:"id"`
	CreatedAt time.Time             `json:"created_at"`
	Config    ExporterConfiguration `json:"config"`
}

type PipelineConfiguration struct {
	ExporterID string `json:"exporter_id"`
	Ledger     string `json:"ledger"`
}

type Pipeline struct {
	Config    PipelineConfiguration `json:"config"`
	CreatedAt time.Time             `json:"created_at"`
	ID        string                `json:"id"`
	Enabled   bool                  `json:"enabled"`
	LastLogID *uint64               `json:"last_log_id,omitempty"`
	Error     string                `json:"error,omitempty"`
}

// Request/response envelopes

type IDRequest struct {
	ID string `json:"id"`
}

type CreateExporterRequest struct {
	Config ExporterConfiguration `json:"config"`
}

type CreateExporterResponse struct {
	Exporter Exporter `json:"exporter"`
}

type ListExportersResponse struct {
	Data   []Exporter `json:"data"`
	Cursor Cursor     `json:"cursor"`
}

type GetExporterResponse struct {
	Exporter Exporter `json:"exporter"`
}

type UpdateExporterRequest struct {
	ID     string                `json:"id"`
	Config ExporterConfiguration `json:"config"`
}

type CreatePipelineRequest struct {
	Config PipelineConfiguration `json:"config"`
}

type CreatePipelineResponse struct {
	Pipeline Pipeline `json:"pipeline"`
}

type ListPipelinesResponse struct {
	Data   []Pipeline `json:"data"`
	Cursor Cursor     `json:"cursor"`
}

type GetPipelineResponse struct {
	Pipeline Pipeline `json:"pipeline"`
}

// Error response for structured error passing
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

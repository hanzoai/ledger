package replication

import (
	"encoding/json"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/rpc"
)

func mapExporterToRPC(exporter ledger.Exporter) rpc.Exporter {
	return rpc.Exporter{
		ID:        exporter.ID,
		CreatedAt: exporter.CreatedAt.Time,
		Config:    mapExporterConfigToRPC(exporter.ExporterConfiguration),
	}
}

func mapExporterFromRPC(exporter rpc.Exporter) ledger.Exporter {
	return ledger.Exporter{
		ID:        exporter.ID,
		CreatedAt: time.New(exporter.CreatedAt),
		ExporterConfiguration: ledger.ExporterConfiguration{
			Driver: exporter.Config.Driver,
			Config: json.RawMessage(exporter.Config.Config),
		},
	}
}

func mapExporterConfigToRPC(cfg ledger.ExporterConfiguration) rpc.ExporterConfiguration {
	return rpc.ExporterConfiguration{
		Driver: cfg.Driver,
		Config: string(cfg.Config),
	}
}

func mapPipelineToRPC(pipeline ledger.Pipeline) rpc.Pipeline {
	return rpc.Pipeline{
		Config: rpc.PipelineConfiguration{
			ExporterID: pipeline.ExporterID,
			Ledger:     pipeline.Ledger,
		},
		CreatedAt: pipeline.CreatedAt.Time,
		ID:        pipeline.ID,
		Enabled:   pipeline.Enabled,
		LastLogID: pipeline.LastLogID,
		Error:     pipeline.Error,
	}
}

func mapPipelineFromRPC(pipeline rpc.Pipeline) ledger.Pipeline {
	return ledger.Pipeline{
		PipelineConfiguration: ledger.PipelineConfiguration{
			ExporterID: pipeline.Config.ExporterID,
			Ledger:     pipeline.Config.Ledger,
		},
		CreatedAt: time.New(pipeline.CreatedAt),
		ID:        pipeline.ID,
		Enabled:   pipeline.Enabled,
		LastLogID: pipeline.LastLogID,
		Error:     pipeline.Error,
	}
}

func mapCursorToRPC[V any](ret *bunpaginate.Cursor[V]) rpc.Cursor {
	return rpc.Cursor{
		Next:    ret.Next,
		HasMore: ret.HasMore,
		Prev:    ret.Previous,
	}
}

func mapCursorFromRPC[V any](ret rpc.Cursor, data []V) *bunpaginate.Cursor[V] {
	return &bunpaginate.Cursor[V]{
		Next:     ret.Next,
		HasMore:  ret.HasMore,
		Previous: ret.Prev,
		Data:     data,
	}
}

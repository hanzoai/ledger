package replication

import (
	"context"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v4/collectionutils"
	"github.com/formancehq/go-libs/v4/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/rpc"
)

type ThroughRPCBackend struct {
	client *rpc.Client
}

func (t ThroughRPCBackend) ListExporters(ctx context.Context) (*bunpaginate.Cursor[ledger.Exporter], error) {
	ret, err := t.client.ListExporters(ctx)
	if err != nil {
		return nil, err
	}

	return mapCursorFromRPC(ret.Cursor, Map(ret.Data, mapExporterFromRPC)), nil
}

func (t ThroughRPCBackend) CreateExporter(ctx context.Context, configuration ledger.ExporterConfiguration) (*ledger.Exporter, error) {
	resp, err := t.client.CreateExporter(ctx, &rpc.CreateExporterRequest{
		Config: mapExporterConfigToRPC(configuration),
	})
	if err != nil {
		if rpc.IsCode(err, rpc.CodeInvalidArgument) {
			return nil, system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
		}
		return nil, err
	}

	return pointer.For(mapExporterFromRPC(resp.Exporter)), nil
}

func (t ThroughRPCBackend) UpdateExporter(ctx context.Context, id string, configuration ledger.ExporterConfiguration) error {
	err := t.client.UpdateExporter(ctx, &rpc.UpdateExporterRequest{
		ID:     id,
		Config: mapExporterConfigToRPC(configuration),
	})
	if err != nil {
		switch {
		case rpc.IsCode(err, rpc.CodeInvalidArgument):
			return system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
		case rpc.IsCode(err, rpc.CodeNotFound):
			return system.NewErrExporterNotFound(id)
		default:
			return err
		}
	}
	return nil
}

func (t ThroughRPCBackend) DeleteExporter(ctx context.Context, id string) error {
	err := t.client.DeleteExporter(ctx, id)
	if err != nil && rpc.IsCode(err, rpc.CodeNotFound) {
		return system.NewErrExporterNotFound(id)
	}
	return err
}

func (t ThroughRPCBackend) GetExporter(ctx context.Context, id string) (*ledger.Exporter, error) {
	resp, err := t.client.GetExporter(ctx, id)
	if err != nil {
		if rpc.IsCode(err, rpc.CodeNotFound) {
			return nil, system.NewErrExporterNotFound(id)
		}
		return nil, err
	}

	return pointer.For(mapExporterFromRPC(resp.Exporter)), nil
}

func (t ThroughRPCBackend) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	resp, err := t.client.ListPipelines(ctx)
	if err != nil {
		return nil, err
	}

	return mapCursorFromRPC(resp.Cursor, Map(resp.Data, mapPipelineFromRPC)), nil
}

func (t ThroughRPCBackend) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	resp, err := t.client.GetPipeline(ctx, id)
	if err != nil {
		if rpc.IsCode(err, rpc.CodeNotFound) {
			return nil, ledger.NewErrPipelineNotFound(id)
		}
		return nil, err
	}

	return pointer.For(mapPipelineFromRPC(resp.Pipeline)), nil
}

func (t ThroughRPCBackend) CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	resp, err := t.client.CreatePipeline(ctx, &rpc.CreatePipelineRequest{
		Config: rpc.PipelineConfiguration{
			ExporterID: pipelineConfiguration.ExporterID,
			Ledger:     pipelineConfiguration.Ledger,
		},
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapPipelineFromRPC(resp.Pipeline)), nil
}

func (t ThroughRPCBackend) DeletePipeline(ctx context.Context, id string) error {
	err := t.client.DeletePipeline(ctx, id)
	if err != nil && rpc.IsCode(err, rpc.CodeNotFound) {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

func (t ThroughRPCBackend) StartPipeline(ctx context.Context, id string) error {
	err := t.client.StartPipeline(ctx, id)
	if err != nil && rpc.IsCode(err, rpc.CodeFailedPrecondition) {
		return ledger.NewErrAlreadyStarted(id)
	}
	return err
}

func (t ThroughRPCBackend) ResetPipeline(ctx context.Context, id string) error {
	err := t.client.ResetPipeline(ctx, id)
	if err != nil && rpc.IsCode(err, rpc.CodeNotFound) {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

func (t ThroughRPCBackend) StopPipeline(ctx context.Context, id string) error {
	err := t.client.StopPipeline(ctx, id)
	if err != nil && rpc.IsCode(err, rpc.CodeNotFound) {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

var _ system.ReplicationBackend = (*ThroughRPCBackend)(nil)

func NewThroughRPCBackend(client *rpc.Client) *ThroughRPCBackend {
	return &ThroughRPCBackend{
		client: client,
	}
}

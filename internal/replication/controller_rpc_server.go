package replication

import (
	"context"
	"encoding/json"
	"errors"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/rpc"

	"github.com/formancehq/go-libs/v4/collectionutils"
)

// RPCServiceImpl adapts Manager to the rpc.ReplicationHandler interface.
type RPCServiceImpl struct {
	manager *Manager
}

func (srv RPCServiceImpl) CreateExporter(ctx context.Context, req *rpc.CreateExporterRequest) (*rpc.CreateExporterResponse, *rpc.ErrorResponse) {
	exporter, err := srv.manager.CreateExporter(ctx, ledger.ExporterConfiguration{
		Driver: req.Config.Driver,
		Config: json.RawMessage(req.Config.Config),
	})
	if err != nil {
		var invalidErr system.ErrInvalidDriverConfiguration
		if errors.As(err, &invalidErr) {
			return nil, &rpc.ErrorResponse{Code: rpc.CodeInvalidArgument, Message: err.Error()}
		}
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.CreateExporterResponse{
		Exporter: mapExporterToRPC(*exporter),
	}, nil
}

func (srv RPCServiceImpl) ListExporters(ctx context.Context) (*rpc.ListExportersResponse, *rpc.ErrorResponse) {
	ret, err := srv.manager.ListExporters(ctx)
	if err != nil {
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.ListExportersResponse{
		Data:   collectionutils.Map(ret.Data, mapExporterToRPC),
		Cursor: mapCursorToRPC(ret),
	}, nil
}

func (srv RPCServiceImpl) GetExporter(ctx context.Context, req *rpc.IDRequest) (*rpc.GetExporterResponse, *rpc.ErrorResponse) {
	ret, err := srv.manager.GetExporter(ctx, req.ID)
	if err != nil {
		if errors.Is(err, system.ErrExporterNotFound("")) {
			return nil, &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.GetExporterResponse{
		Exporter: mapExporterToRPC(*ret),
	}, nil
}

func (srv RPCServiceImpl) UpdateExporter(ctx context.Context, req *rpc.UpdateExporterRequest) *rpc.ErrorResponse {
	err := srv.manager.UpdateExporter(ctx, req.ID, ledger.ExporterConfiguration{
		Driver: req.Config.Driver,
		Config: json.RawMessage(req.Config.Config),
	})
	if err != nil {
		var invalidErr system.ErrInvalidDriverConfiguration
		if errors.As(err, &invalidErr) {
			return &rpc.ErrorResponse{Code: rpc.CodeInvalidArgument, Message: err.Error()}
		}
		if errors.Is(err, system.ErrExporterNotFound("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

func (srv RPCServiceImpl) DeleteExporter(ctx context.Context, req *rpc.IDRequest) *rpc.ErrorResponse {
	if err := srv.manager.DeleteExporter(ctx, req.ID); err != nil {
		if errors.Is(err, system.ErrExporterNotFound("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

func (srv RPCServiceImpl) ListPipelines(ctx context.Context) (*rpc.ListPipelinesResponse, *rpc.ErrorResponse) {
	cursor, err := srv.manager.ListPipelines(ctx)
	if err != nil {
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.ListPipelinesResponse{
		Data:   collectionutils.Map(cursor.Data, mapPipelineToRPC),
		Cursor: mapCursorToRPC(cursor),
	}, nil
}

func (srv RPCServiceImpl) GetPipeline(ctx context.Context, req *rpc.IDRequest) (*rpc.GetPipelineResponse, *rpc.ErrorResponse) {
	pipeline, err := srv.manager.GetPipeline(ctx, req.ID)
	if err != nil {
		if errors.Is(err, ledger.ErrPipelineNotFound("")) {
			return nil, &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.GetPipelineResponse{
		Pipeline: mapPipelineToRPC(*pipeline),
	}, nil
}

func (srv RPCServiceImpl) CreatePipeline(ctx context.Context, req *rpc.CreatePipelineRequest) (*rpc.CreatePipelineResponse, *rpc.ErrorResponse) {
	pipeline, err := srv.manager.CreatePipeline(ctx, ledger.PipelineConfiguration{
		ExporterID: req.Config.ExporterID,
		Ledger:     req.Config.Ledger,
	})
	if err != nil {
		return nil, &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}

	return &rpc.CreatePipelineResponse{
		Pipeline: mapPipelineToRPC(*pipeline),
	}, nil
}

func (srv RPCServiceImpl) DeletePipeline(ctx context.Context, req *rpc.IDRequest) *rpc.ErrorResponse {
	if err := srv.manager.DeletePipeline(ctx, req.ID); err != nil {
		if errors.Is(err, ledger.ErrPipelineNotFound("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

func (srv RPCServiceImpl) StartPipeline(ctx context.Context, req *rpc.IDRequest) *rpc.ErrorResponse {
	if err := srv.manager.StartPipeline(ctx, req.ID); err != nil {
		if errors.Is(err, ledger.ErrAlreadyStarted("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeFailedPrecondition, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

func (srv RPCServiceImpl) StopPipeline(ctx context.Context, req *rpc.IDRequest) *rpc.ErrorResponse {
	if err := srv.manager.StopPipeline(ctx, req.ID); err != nil {
		if errors.Is(err, ledger.ErrPipelineNotFound("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

func (srv RPCServiceImpl) ResetPipeline(ctx context.Context, req *rpc.IDRequest) *rpc.ErrorResponse {
	if err := srv.manager.ResetPipeline(ctx, req.ID); err != nil {
		if errors.Is(err, ledger.ErrPipelineNotFound("")) {
			return &rpc.ErrorResponse{Code: rpc.CodeNotFound, Message: err.Error()}
		}
		return &rpc.ErrorResponse{Code: "internal", Message: err.Error()}
	}
	return nil
}

var _ rpc.ReplicationHandler = (*RPCServiceImpl)(nil)

func NewReplicationServiceImpl(runner *Manager) *RPCServiceImpl {
	return &RPCServiceImpl{
		manager: runner,
	}
}

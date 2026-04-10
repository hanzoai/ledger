package worker

import (
	"context"
	"log/slog"

	"go.uber.org/fx"

	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/rpc"
	"github.com/formancehq/ledger/internal/storage"
)

type ZAPServerModuleConfig struct {
	Address string
}

type ModuleConfig struct {
	AsyncBlockRunnerConfig    storage.AsyncBlockRunnerConfig
	ReplicationConfig         replication.WorkerModuleConfig
	BucketCleanupRunnerConfig storage.BucketCleanupRunnerConfig
}

// NewFXModule constructs an fx.Option that installs the storage async block runner,
// the replication worker, and the bucket cleanup runner modules into an Fx application.
func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		storage.NewAsyncBlockRunnerModule(cfg.AsyncBlockRunnerConfig),
		replication.NewWorkerFXModule(cfg.ReplicationConfig),
		storage.NewBucketCleanupRunnerModule(cfg.BucketCleanupRunnerConfig),
	)
}

// NewZAPServerFXModule creates an fx module that starts a ZAP-based replication server.
func NewZAPServerFXModule(cfg ZAPServerModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(handler rpc.ReplicationHandler) (*rpc.Server, error) {
			return rpc.NewServer(cfg.Address, handler, slog.Default())
		}),
		fx.Invoke(func(lc fx.Lifecycle, server *rpc.Server) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return server.Start()
				},
				OnStop: func(ctx context.Context) error {
					server.Stop()
					return nil
				},
			})
		}),
	)
}

// NewZAPClientFxModule creates an fx module that connects a ZAP client to the worker.
func NewZAPClientFxModule(address string) fx.Option {
	return fx.Options(
		fx.Provide(func(lc fx.Lifecycle) (*rpc.Client, error) {
			client, err := rpc.NewClient(address, slog.Default())
			if err != nil {
				return nil, err
			}
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					client.Stop()
					return nil
				},
			})
			return client, nil
		}),
	)
}

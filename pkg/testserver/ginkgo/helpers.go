package ginkgo

import (
	"fmt"
	"net"

	"github.com/formancehq/go-libs/v4/bun/bunconnect"
	"github.com/formancehq/go-libs/v4/testing/deferred"
	"github.com/formancehq/go-libs/v4/testing/testservice"
	. "github.com/formancehq/go-libs/v4/testing/testservice/ginkgo"

	"github.com/formancehq/ledger/cmd"
	"github.com/formancehq/ledger/pkg/testserver"
)

func DeferTestServer(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testserver.GetTestServerOptions(postgresConnectionOptions),
		}, options...)...,
	)
}

// DeferTestWorkerWithAddr starts a worker on a free port and returns both the
// deferred service and the address string the API server should connect to.
func DeferTestWorkerWithAddr(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) (*deferred.Deferred[*testservice.Service], string) {
	port := freePort()
	addr := fmt.Sprintf(":%d", port)
	svc := DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testservice.WithInstruments(
				testservice.AppendArgsInstrumentation("worker"),
				testservice.PostgresInstrumentation(postgresConnectionOptions),
				testserver.ZAPAddressInstrumentation(addr),
			),
		}, options...)...,
	)
	return svc, fmt.Sprintf("localhost:%d", port)
}

// DeferTestWorker starts a worker and returns the deferred service.
// Use DeferTestWorkerWithAddr if you need the resolved address.
func DeferTestWorker(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	svc, _ := DeferTestWorkerWithAddr(postgresConnectionOptions, options...)
	return svc
}

func freePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("failed to find free port: %v", err))
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

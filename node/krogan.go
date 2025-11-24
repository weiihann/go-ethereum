// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package node

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gofrs/flock"
	"golang.org/x/exp/slices"
)

type KroganNode struct {
	config *KroganConfig
	log    log.Logger

	lifecycles    []Lifecycle                   // All registered backends, services, and auxiliary services that have a lifecycle
	rpcAPIs       []rpc.API                     // List of APIs currently provided by the node
	http          *httpServer                   //
	ws            *httpServer                   //
	httpAuth      *httpServer                   //
	wsAuth        *httpServer                   //
	ipc           *ipcServer                    // Stores information about the ipc http server
	inprocHandler *rpc.Server                   // In-process RPC request handler to process the API requests
	databases     map[*closeTrackingDB]struct{} // All open databases

	startOnce sync.Once
	started   atomic.Bool
	closeOnce sync.Once
	dirLock   *flock.Flock
	stop      chan struct{}
}

func NewKrogan(conf *KroganConfig) (*KroganNode, error) {
	// Copy config and resolve the datadir so future changes to the current
	// working directory don't affect the node.
	confCopy := *conf
	conf = &confCopy
	if conf.DataDir != "" {
		absdatadir, err := filepath.Abs(conf.DataDir)
		if err != nil {
			return nil, err
		}
		conf.DataDir = absdatadir
	}
	if conf.Logger == nil {
		conf.Logger = log.New()
	}

	server := rpc.NewServer()
	server.SetBatchLimits(conf.BatchRequestLimit, conf.BatchResponseMaxSize)
	node := &KroganNode{
		config:        conf,
		inprocHandler: server,
		log:           conf.Logger,
		databases:     make(map[*closeTrackingDB]struct{}),
	}

	// Register built-in APIs.
	node.rpcAPIs = append(node.rpcAPIs, node.apis()...)

	if err := node.openDataDir(); err != nil {
		return nil, err
	}

	// Check HTTP/WS prefixes are valid.
	if err := validatePrefix("HTTP", conf.HTTPPathPrefix); err != nil {
		return nil, err
	}
	if err := validatePrefix("WebSocket", conf.WSPathPrefix); err != nil {
		return nil, err
	}

	// Configure RPC servers.
	node.http = newHTTPServer(node.log, conf.HTTPTimeouts)
	node.httpAuth = newHTTPServer(node.log, conf.HTTPTimeouts)
	node.ws = newHTTPServer(node.log, rpc.DefaultHTTPTimeouts)
	node.wsAuth = newHTTPServer(node.log, rpc.DefaultHTTPTimeouts)
	node.ipc = newIPCServer(node.log, conf.IPCEndpoint())

	// TODO(weiihann): initialize HTTP and WS clients for master nodes

	return node, nil
}

func (k *KroganNode) Start() error {
	var err error

	k.startOnce.Do(func() {
		// open networking and RPC endpoints
		err = k.openEndpoints()
		if err != nil {
			k.doClose()
			return
		}

		// Start all registered lifecycles.
		var started []Lifecycle
		for _, lifecycle := range k.lifecycles {
			if err = lifecycle.Start(); err != nil {
				break
			}
			started = append(started, lifecycle)
		}

		// Check if any lifecycle failed to start.
		if err != nil {
			k.stopServices(started)
			k.doClose()
		}
	})

	return err
}

func (k *KroganNode) Close() error {
	if err := k.stopServices(k.lifecycles); err != nil {
		return err
	}
	k.doClose()
	return nil
}

func (k *KroganNode) apis() []rpc.API {
	return []rpc.API{
		{
			Namespace: "debug",
			Service:   debug.Handler,
		},
	}
}

func (k *KroganNode) Wait() {
	<-k.stop
}

func (k *KroganNode) openDataDir() error {
	if k.config.DataDir == "" {
		return nil // ephemeral
	}

	instdir := filepath.Join(k.config.DataDir, k.config.name())
	if err := os.MkdirAll(instdir, 0o700); err != nil {
		return err
	}
	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	k.dirLock = flock.New(filepath.Join(instdir, "LOCK"))

	if locked, err := k.dirLock.TryLock(); err != nil {
		return err
	} else if !locked {
		return ErrDatadirUsed
	}
	return nil
}

func (k *KroganNode) closeDataDir() {
	// Release instance directory lock.
	if k.dirLock != nil && k.dirLock.Locked() {
		k.dirLock.Unlock()
		k.dirLock = nil
	}
}

func (k *KroganNode) openEndpoints() error {
	err := k.startRPC()
	if err != nil {
		k.stopRPC()
	}
	return err
}

// startRPC is a helper method to configure all the various RPC endpoints during node
// startup. It's not meant to be called at any time afterwards as it makes certain
// assumptions about the state of the node.
func (k *KroganNode) startRPC() error {
	if err := k.startInProc(k.rpcAPIs); err != nil {
		return err
	}

	// Configure IPC.
	if k.ipc.endpoint != "" {
		if err := k.ipc.start(k.rpcAPIs); err != nil {
			return err
		}
	}

	var servers []*httpServer

	rpcConfig := rpcEndpointConfig{
		batchItemLimit:         k.config.BatchRequestLimit,
		batchResponseSizeLimit: k.config.BatchResponseMaxSize,
	}

	initHttp := func(server *httpServer, port int) error {
		if err := server.setListenAddr(k.config.HTTPHost, port); err != nil {
			return err
		}
		if err := server.enableRPC(k.rpcAPIs, httpConfig{
			CorsAllowedOrigins: k.config.HTTPCors,
			Vhosts:             k.config.HTTPVirtualHosts,
			Modules:            k.config.HTTPModules,
			prefix:             k.config.HTTPPathPrefix,
			rpcEndpointConfig:  rpcConfig,
		}); err != nil {
			return err
		}
		servers = append(servers, server)
		return nil
	}

	initWS := func(server *httpServer, port int) error {
		if err := server.setListenAddr(k.config.WSHost, port); err != nil {
			return err
		}
		if err := server.enableWS(k.rpcAPIs, wsConfig{
			Modules:           k.config.WSModules,
			Origins:           k.config.WSOrigins,
			prefix:            k.config.WSPathPrefix,
			rpcEndpointConfig: rpcConfig,
		}); err != nil {
			return err
		}
		servers = append(servers, server)
		return nil
	}

	// Set up HTTP.
	if k.config.HTTPHost != "" {
		// Configure legacy unauthenticated HTTP.
		if err := initHttp(k.http, k.config.HTTPPort); err != nil {
			return err
		}
	}
	// Configure WebSocket.
	if k.config.WSHost != "" {
		// legacy unauthenticated
		if err := initWS(k.ws, k.config.WSPort); err != nil {
			return err
		}
	}

	// Start the servers
	for _, server := range servers {
		if err := server.start(); err != nil {
			return err
		}
	}
	return nil
}

// startInProc registers all RPC APIs on the inproc server.
func (k *KroganNode) startInProc(apis []rpc.API) error {
	for _, api := range apis {
		if err := k.inprocHandler.RegisterName(api.Namespace, api.Service); err != nil {
			return err
		}
	}
	return nil
}

// stopInProc terminates the in-process RPC endpoint.
func (k *KroganNode) stopInProc() {
	k.inprocHandler.Stop()
}

func (k *KroganNode) stopRPC() {
	k.http.stop()
	k.ws.stop()
	k.httpAuth.stop()
	k.wsAuth.stop()
	k.ipc.stop()
	k.stopInProc()
}

func (k *KroganNode) doClose() {
	k.closeOnce.Do(func() {
		k.closeDataDir()
		close(k.stop)
	})
}

func (k *KroganNode) stopServices(running []Lifecycle) error {
	k.stopRPC()

	// Stop running lifecycles in reverse order.
	failure := &StopError{Services: make(map[reflect.Type]error)}
	for i := len(running) - 1; i >= 0; i-- {
		if err := running[i].Stop(); err != nil {
			failure.Services[reflect.TypeOf(running[i])] = err
		}
	}

	if len(failure.Services) > 0 {
		return failure
	}

	return nil
}

func (k *KroganNode) OpenDatabaseWithOptions(name string, opt DatabaseOptions) (ethdb.Database, error) {
	panic("TODO(weiihann): implement")
}

func (k *KroganNode) RegisterLifecycle(lifecycle Lifecycle) {
	if slices.Contains(k.lifecycles, lifecycle) {
		panic(fmt.Sprintf("attempt to register lifecycle %T more than once", lifecycle))
	}
	k.lifecycles = append(k.lifecycles, lifecycle)
}

func (k *KroganNode) RegisterAPIs(apis []rpc.API) {
	k.rpcAPIs = append(k.rpcAPIs, apis...)
}

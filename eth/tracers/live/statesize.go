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

package live

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/gorilla/websocket"
)

func init() {
	tracers.LiveDirectory.Register("statesize", newStateSizeTracer)
}

const (
	// idleTimeout is the maximum time to wait for a subscriber before shutting down.
	idleTimeout = 30 * time.Minute

	// wsWriteTimeout is the timeout for writing to a WebSocket connection.
	wsWriteTimeout = 30 * time.Second
)

// stateSizeTracerConfig is the configuration for the statesize tracer.
type stateSizeTracerConfig struct {
	Address string `json:"address"` // WebSocket server address (e.g., ":8546")
}

// JSON-serializable types for StateUpdate

// StateUpdateJSON is the JSON-serializable version of tracing.StateUpdate.
type StateUpdateJSON struct {
	OriginRoot  common.Hash `json:"originRoot"`
	Root        common.Hash `json:"root"`
	BlockNumber uint64      `json:"blockNumber"`

	AccountChanges map[common.Address]*AccountChangeJSON                 `json:"accountChanges,omitempty"`
	StorageChanges map[common.Address]map[common.Hash]*StorageChangeJSON `json:"storageChanges,omitempty"`
	CodeChanges    map[common.Address]*CodeChangeJSON                    `json:"codeChanges,omitempty"`
	TrieChanges    map[common.Hash]map[string]*TrieNodeChangeJSON        `json:"trieChanges,omitempty"`
}

// AccountChangeJSON is the JSON-serializable version of tracing.AccountChange.
type AccountChangeJSON struct {
	Prev *StateAccountJSON `json:"prev,omitempty"`
	New  *StateAccountJSON `json:"new,omitempty"`
}

// StateAccountJSON is a JSON-serializable version of types.StateAccount.
type StateAccountJSON struct {
	Nonce    uint64        `json:"nonce"`
	Balance  *hexutil.Big  `json:"balance"`
	Root     common.Hash   `json:"root"`
	CodeHash hexutil.Bytes `json:"codeHash"`
}

// StorageChangeJSON is the JSON-serializable version of tracing.StorageChange.
type StorageChangeJSON struct {
	Prev common.Hash `json:"prev"`
	New  common.Hash `json:"new"`
}

// CodeChangeJSON is the JSON-serializable version of tracing.CodeChange.
type CodeChangeJSON struct {
	Prev *ContractCodeJSON `json:"prev,omitempty"`
	New  *ContractCodeJSON `json:"new,omitempty"`
}

// ContractCodeJSON is the JSON-serializable version of tracing.ContractCode.
type ContractCodeJSON struct {
	Hash   common.Hash   `json:"hash"`
	Code   hexutil.Bytes `json:"code"`
	Exists bool          `json:"exists"`
}

// TrieNodeChangeJSON is the JSON-serializable version of tracing.TrieNodeChange.
type TrieNodeChangeJSON struct {
	Prev *TrieNodeJSON `json:"prev,omitempty"`
	New  *TrieNodeJSON `json:"new,omitempty"`
}

// TrieNodeJSON is the JSON-serializable version of trienode.Node.
type TrieNodeJSON struct {
	Blob hexutil.Bytes `json:"blob"`
	Hash common.Hash   `json:"hash"`
}

type stateSizeTracer struct {
	mu          sync.RWMutex
	server      *http.Server
	subscribers map[*websocket.Conn]struct{}
	upgrader    websocket.Upgrader

	// Idle timeout tracking
	lastActivity time.Time
	done         chan struct{}
	closeOnce    sync.Once
}

func newStateSizeTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config stateSizeTracerConfig
	if err := json.Unmarshal(cfg, &config); err != nil {
		return nil, errors.New("failed to parse config: " + err.Error())
	}
	if config.Address == "" {
		return nil, errors.New("statesize tracer address is required")
	}

	t := &stateSizeTracer{
		subscribers:  make(map[*websocket.Conn]struct{}, 16),
		lastActivity: time.Now(),
		done:         make(chan struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}

	// Create HTTP server with WebSocket handler
	mux := http.NewServeMux()
	mux.HandleFunc("/", t.handleWebSocket)

	t.server = &http.Server{
		Addr:    config.Address,
		Handler: mux,
	}

	// Start the HTTP server
	go func() {
		log.Info("Starting statesize WebSocket server", "address", config.Address)
		if err := t.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("Statesize WebSocket server error", "err", err)
		}
	}()

	return &tracing.Hooks{
		OnStateUpdate: t.onStateUpdate,
		OnClose:       t.onClose,
	}, nil
}

// handleWebSocket upgrades HTTP connections to WebSocket and manages subscribers.
func (t *stateSizeTracer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := t.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Debug("WebSocket upgrade failed", "err", err)
		return
	}

	// Add subscriber
	t.mu.Lock()
	t.subscribers[conn] = struct{}{}
	t.lastActivity = time.Now()
	subscriberCount := len(t.subscribers)
	t.mu.Unlock()

	log.Info("New statesize WebSocket subscriber", "total", subscriberCount)

	// Read loop to detect disconnection
	defer func() {
		t.mu.Lock()
		delete(t.subscribers, conn)
		subscriberCount := len(t.subscribers)
		if subscriberCount == 0 {
			t.lastActivity = time.Now()
		}
		t.mu.Unlock()

		conn.Close()
		log.Info("Statesize WebSocket subscriber disconnected", "remaining", subscriberCount)
	}()

	// Keep connection alive by reading (and discarding) any incoming messages
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// broadcast sends a message to all connected subscribers.
func (t *stateSizeTracer) broadcast(data []byte) {
	t.mu.RLock()
	subscribers := make([]*websocket.Conn, 0, len(t.subscribers))
	for conn := range t.subscribers {
		subscribers = append(subscribers, conn)
	}
	t.mu.RUnlock()

	for _, conn := range subscribers {
		conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Debug("Failed to send to subscriber", "err", err)
			// Connection will be cleaned up by the read loop
		}
	}
}

func (t *stateSizeTracer) onStateUpdate(update *tracing.StateUpdate) {
	if update == nil || t.isClosed() {
		return
	}

	// Wait for a subscriber to connect (up to idleTimeout)
	if !t.waitForSubscriber() {
		// Check if we're closing - if so, exit silently
		if t.isClosed() {
			return
		}
		log.Warn("Statesize tracer: no subscriber connected within timeout, triggering shutdown")
		// Send SIGTERM to self for graceful shutdown
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			p.Signal(syscall.SIGTERM)
		}
		return
	}

	// Check again after waiting - we might have been closed
	if t.isClosed() {
		return
	}

	// Convert to JSON-serializable format
	msg := convertStateUpdate(update)

	// Marshal to JSON
	data, err := json.Marshal(msg)
	if err != nil {
		log.Warn("Failed to marshal state update", "err", err)
		return
	}

	// Broadcast to all subscribers
	t.broadcast(data)

	// Update activity timestamp
	t.mu.Lock()
	t.lastActivity = time.Now()
	t.mu.Unlock()
}

// waitForSubscriber waits for at least one subscriber to connect.
// Returns true if a subscriber connected, false if timeout was reached.
func (t *stateSizeTracer) waitForSubscriber() bool {
	// Quick check first
	t.mu.RLock()
	hasSubscribers := len(t.subscribers) > 0
	t.mu.RUnlock()
	if hasSubscribers {
		return true
	}

	// Wait with periodic checks
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Info("Statesize tracer: waiting for subscriber")
	timeout := time.After(idleTimeout)

	for {
		select {
		case <-ticker.C:
			t.mu.RLock()
			hasSubscribers := len(t.subscribers) > 0
			t.mu.RUnlock()
			if hasSubscribers {
				log.Info("Statesize tracer: subscriber connected")
				return true
			}
			log.Info("Statesize tracer: waiting for subscriber")
		case <-timeout:
			log.Info("Statesize tracer: timeout reached")
			return false
		case <-t.done:
			return false
		}
	}
}

// convertStateUpdate converts a tracing.StateUpdate to a JSON-serializable format.
func convertStateUpdate(update *tracing.StateUpdate) *StateUpdateJSON {
	msg := &StateUpdateJSON{
		OriginRoot:  update.OriginRoot,
		Root:        update.Root,
		BlockNumber: update.BlockNumber,
	}

	// Convert account changes
	if len(update.AccountChanges) > 0 {
		msg.AccountChanges = make(map[common.Address]*AccountChangeJSON, len(update.AccountChanges))
		for addr, change := range update.AccountChanges {
			msg.AccountChanges[addr] = &AccountChangeJSON{
				Prev: convertStateAccount(change.Prev),
				New:  convertStateAccount(change.New),
			}
		}
	}

	// Convert storage changes
	if len(update.StorageChanges) > 0 {
		msg.StorageChanges = make(map[common.Address]map[common.Hash]*StorageChangeJSON, len(update.StorageChanges))
		for addr, slots := range update.StorageChanges {
			msg.StorageChanges[addr] = make(map[common.Hash]*StorageChangeJSON, len(slots))
			for slot, change := range slots {
				msg.StorageChanges[addr][slot] = &StorageChangeJSON{
					Prev: change.Prev,
					New:  change.New,
				}
			}
		}
	}

	// Convert code changes
	if len(update.CodeChanges) > 0 {
		msg.CodeChanges = make(map[common.Address]*CodeChangeJSON, len(update.CodeChanges))
		for addr, change := range update.CodeChanges {
			msg.CodeChanges[addr] = &CodeChangeJSON{
				Prev: convertContractCode(change.Prev),
				New:  convertContractCode(change.New),
			}
		}
	}

	// Convert trie changes
	if len(update.TrieChanges) > 0 {
		msg.TrieChanges = make(map[common.Hash]map[string]*TrieNodeChangeJSON, len(update.TrieChanges))
		for owner, nodes := range update.TrieChanges {
			msg.TrieChanges[owner] = make(map[string]*TrieNodeChangeJSON, len(nodes))
			for path, change := range nodes {
				msg.TrieChanges[owner][path] = &TrieNodeChangeJSON{
					Prev: convertTrieNode(change.Prev),
					New:  convertTrieNode(change.New),
				}
			}
		}
	}

	return msg
}

func convertStateAccount(acct *types.StateAccount) *StateAccountJSON {
	if acct == nil {
		return nil
	}
	return &StateAccountJSON{
		Nonce:    acct.Nonce,
		Balance:  (*hexutil.Big)(acct.Balance.ToBig()),
		Root:     acct.Root,
		CodeHash: acct.CodeHash,
	}
}

func convertContractCode(code *tracing.ContractCode) *ContractCodeJSON {
	if code == nil {
		return nil
	}
	return &ContractCodeJSON{
		Hash:   code.Hash,
		Code:   code.Code,
		Exists: code.Exists,
	}
}

func convertTrieNode(node *trienode.Node) *TrieNodeJSON {
	if node == nil {
		return nil
	}
	return &TrieNodeJSON{
		Blob: node.Blob,
		Hash: node.Hash,
	}
}

func (t *stateSizeTracer) onClose() {
	t.closeOnce.Do(func() {
		// Signal any waiting goroutines to stop
		close(t.done)

		// Close all WebSocket connections
		t.mu.Lock()
		for conn := range t.subscribers {
			conn.Close()
		}
		t.subscribers = nil
		t.mu.Unlock()

		// Shutdown the HTTP server gracefully
		if t.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := t.server.Shutdown(ctx); err != nil {
				log.Warn("Failed to shutdown statesize WebSocket server gracefully", "err", err)
			}
		}

		log.Info("Statesize tracer closed")
	})
}

// isClosed returns true if the tracer has been closed.
func (t *stateSizeTracer) isClosed() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

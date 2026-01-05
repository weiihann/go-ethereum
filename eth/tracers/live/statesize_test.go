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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/gorilla/websocket"
	"github.com/holiman/uint256"
)

func TestStateSizeTracerConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  `{"address": ":8546"}`,
			wantErr: false,
		},
		{
			name:    "missing address",
			config:  `{}`,
			wantErr: true,
		},
		{
			name:    "invalid json",
			config:  `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hooks, err := newStateSizeTracer(json.RawMessage(tt.config))
			if (err != nil) != tt.wantErr {
				t.Errorf("newStateSizeTracer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if hooks != nil && hooks.OnClose != nil {
				hooks.OnClose()
			}
		})
	}
}

func TestConvertStateUpdate(t *testing.T) {
	update := &tracing.StateUpdate{
		BlockNumber: 100,
		Root:        common.HexToHash("0x1234"),
		OriginRoot:  common.HexToHash("0x5678"),
		AccountChanges: map[common.Address]*tracing.AccountChange{
			common.HexToAddress("0x1"): {
				Prev: nil,
				New: &types.StateAccount{
					Nonce:    1,
					Balance:  uint256.NewInt(1000),
					Root:     common.HexToHash("0xroot"),
					CodeHash: []byte{0x01, 0x02},
				},
			},
			common.HexToAddress("0x2"): {
				Prev: &types.StateAccount{Nonce: 1, Balance: uint256.NewInt(500)},
				New:  nil,
			},
		},
		StorageChanges: map[common.Address]map[common.Hash]*tracing.StorageChange{
			common.HexToAddress("0x1"): {
				common.HexToHash("0x1"): {
					Prev: common.Hash{},
					New:  common.HexToHash("0x1234"),
				},
			},
		},
		TrieChanges: map[common.Hash]map[string]*tracing.TrieNodeChange{
			{}: { // Account trie
				"": {
					Prev: nil,
					New:  &trienode.Node{Blob: []byte{0x01, 0x02}, Hash: common.HexToHash("0xnode")},
				},
			},
		},
		CodeChanges: map[common.Address]*tracing.CodeChange{
			common.HexToAddress("0x3"): {
				New: &tracing.ContractCode{
					Hash:   common.HexToHash("0xcode"),
					Code:   []byte{0x60, 0x00},
					Exists: false,
				},
			},
		},
	}

	msg := convertStateUpdate(update)

	if msg.BlockNumber != 100 {
		t.Errorf("BlockNumber = %d, want 100", msg.BlockNumber)
	}
	if msg.Root != common.HexToHash("0x1234") {
		t.Errorf("Root = %v, want 0x1234", msg.Root)
	}
	if len(msg.AccountChanges) != 2 {
		t.Errorf("AccountChanges len = %d, want 2", len(msg.AccountChanges))
	}
	if len(msg.StorageChanges) != 1 {
		t.Errorf("StorageChanges len = %d, want 1", len(msg.StorageChanges))
	}
	if len(msg.TrieChanges) != 1 {
		t.Errorf("TrieChanges len = %d, want 1", len(msg.TrieChanges))
	}
	if len(msg.CodeChanges) != 1 {
		t.Errorf("CodeChanges len = %d, want 1", len(msg.CodeChanges))
	}

	// Test JSON serialization
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestWebSocketBroadcast(t *testing.T) {
	tracer := &stateSizeTracer{
		subscribers: make(map[*websocket.Conn]struct{}, 16),
		done:        make(chan struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		lastActivity: time.Now(),
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(tracer.handleWebSocket))
	defer server.Close()

	// Connect WebSocket client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect WebSocket: %v", err)
	}
	defer ws.Close()

	// Wait for subscriber to be registered
	time.Sleep(50 * time.Millisecond)

	// Check subscriber count
	tracer.mu.RLock()
	if len(tracer.subscribers) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(tracer.subscribers))
	}
	tracer.mu.RUnlock()

	// Broadcast a message
	msg := &StateUpdateJSON{
		BlockNumber: 100,
		Root:        common.HexToHash("0x1234"),
	}
	data, _ := json.Marshal(msg)
	tracer.broadcast(data)

	// Read the message from client
	ws.SetReadDeadline(time.Now().Add(time.Second))
	_, receivedData, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	var received StateUpdateJSON
	if err := json.Unmarshal(receivedData, &received); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if received.BlockNumber != 100 {
		t.Errorf("BlockNumber = %d, want 100", received.BlockNumber)
	}

	// Close tracer
	close(tracer.done)
}

func TestWaitForSubscriberTimeout(t *testing.T) {
	tracer := &stateSizeTracer{
		subscribers:  make(map[*websocket.Conn]struct{}),
		done:         make(chan struct{}),
		lastActivity: time.Now(),
	}

	// Close done channel after a short delay to simulate cancellation
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(tracer.done)
	}()

	// This should return false because done was closed
	result := tracer.waitForSubscriber()
	if result {
		t.Error("waitForSubscriber should return false when done is closed")
	}
}

func TestWaitForSubscriber(t *testing.T) {
	tracer := &stateSizeTracer{
		subscribers:  make(map[*websocket.Conn]struct{}),
		done:         make(chan struct{}),
		lastActivity: time.Now(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(tracer.handleWebSocket))
	defer server.Close()

	// Start waiting for subscriber in background
	result := make(chan bool, 1)
	go func() {
		result <- tracer.waitForSubscriber()
	}()

	// Give it a moment, then connect
	time.Sleep(50 * time.Millisecond)

	// Connect WebSocket client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect WebSocket: %v", err)
	}
	defer ws.Close()

	// Wait for result
	select {
	case got := <-result:
		if !got {
			t.Error("waitForSubscriber returned false, expected true")
		}
	case <-time.After(time.Second):
		t.Error("waitForSubscriber timed out")
	}

	close(tracer.done)
}

func TestConvertStateAccount(t *testing.T) {
	tests := []struct {
		name    string
		account *types.StateAccount
		wantNil bool
	}{
		{
			name:    "nil account",
			account: nil,
			wantNil: true,
		},
		{
			name: "valid account",
			account: &types.StateAccount{
				Nonce:    100,
				Balance:  uint256.NewInt(1000),
				Root:     common.HexToHash("0x1234"),
				CodeHash: []byte{0x01, 0x02},
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertStateAccount(tt.account)
			if tt.wantNil && result != nil {
				t.Errorf("convertStateAccount() = %v, want nil", result)
			}
			if !tt.wantNil && result == nil {
				t.Error("convertStateAccount() = nil, want non-nil")
			}
			if !tt.wantNil && result != nil {
				if result.Nonce != tt.account.Nonce {
					t.Errorf("Nonce = %d, want %d", result.Nonce, tt.account.Nonce)
				}
			}
		})
	}
}

func TestConvertTrieNode(t *testing.T) {
	tests := []struct {
		name    string
		node    *trienode.Node
		wantNil bool
	}{
		{
			name:    "nil node",
			node:    nil,
			wantNil: true,
		},
		{
			name: "valid node",
			node: &trienode.Node{
				Blob: []byte{0x01, 0x02},
				Hash: common.HexToHash("0x1234"),
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertTrieNode(tt.node)
			if tt.wantNil && result != nil {
				t.Errorf("convertTrieNode() = %v, want nil", result)
			}
			if !tt.wantNil && result == nil {
				t.Error("convertTrieNode() = nil, want non-nil")
			}
			if !tt.wantNil && result != nil {
				if result.Hash != tt.node.Hash {
					t.Errorf("Hash = %v, want %v", result.Hash, tt.node.Hash)
				}
			}
		})
	}
}

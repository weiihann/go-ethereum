// Copyright 2021 go-ethereum Authors
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

package trie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
)

// VerkleTrie is a wrapper around VerkleNode that implements the trie.Trie
// interface so that Verkle trees can be reused verbatim.
type VerkleTrie struct {
	root       verkle.VerkleNode
	db         *Database
	pointCache *utils.PointCache
	ended      bool
	currEpoch  verkle.StateEpoch
}

func (t *VerkleTrie) SetCurrEpoch(epoch verkle.StateEpoch) {
	if epoch > t.currEpoch {
		t.currEpoch = epoch
	}
}

func (t *VerkleTrie) Revive(reviveKV types.ReviveKeyValues) error {
	var err error
	switch root := t.root.(type) {
	case *verkle.InternalNode:
		root.UpdateCurrEpoch(t.currEpoch)
		err = root.Revive(reviveKV.Key, reviveKV.Values, t.flatdbNodeResolver)
	default:
		return errInvalidRootType
	}

	return err
}

func (t *VerkleTrie) HashKey(key []byte) []byte {
	return key
}

func (vt *VerkleTrie) ToDot() string {
	return verkle.ToDot(vt.root)
}

func NewVerkleTrie(root verkle.VerkleNode, db *Database, pointCache *utils.PointCache, ended bool) *VerkleTrie {
	return &VerkleTrie{
		root:       root,
		db:         db,
		pointCache: pointCache,
		ended:      ended,
	}
}

func (trie *VerkleTrie) flatdbNodeResolver(path []byte) ([]byte, error) {
	return trie.db.diskdb.Get(append(FlatDBVerkleNodeKeyPrefix, path...))
}

func (trie *VerkleTrie) InsertMigratedLeaves(leaves []verkle.LeafNode) error {
	return trie.root.(*verkle.InternalNode).InsertMigratedLeaves(leaves, trie.flatdbNodeResolver)
}

var (
	errInvalidProof    = errors.New("invalid proof")
	errInvalidRootType = errors.New("invalid node type for root")

	// WORKAROUND: this special error is returned if it has been
	// detected that the account was deleted in the verkle tree.
	// This is needed in case an account was translated while it
	// was in the MPT, and was selfdestructed in verkle mode.
	//
	// This is only a problem for replays, and this code is not
	// needed after SELFDESTRUCT has been removed.
	errDeletedAccount = errors.New("account deleted in VKT")

	FlatDBVerkleNodeKeyPrefix = []byte("flat-") // prefix for flatdb keys
)

// GetKey returns the sha3 preimage of a hashed key that was previously used
// to store a value.
func (trie *VerkleTrie) GetKey(key []byte) []byte {
	return key
}

// Get returns the value for key stored in the trie. The value bytes must
// not be modified by the caller. If a node was not found in the database, a
// trie.MissingNodeError is returned.
func (trie *VerkleTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	pointEval := trie.pointCache.GetTreeKeyHeader(addr[:])
	k := utils.GetTreeKeyStorageSlotWithEvaluatedAddress(pointEval, key)
	trie.root.UpdateCurrEpoch(trie.currEpoch)
	return trie.root.Get(k, trie.flatdbNodeResolver)
}

// GetWithHashedKey returns the value, assuming that the key has already
// been hashed.
func (trie *VerkleTrie) GetWithHashedKey(key []byte) ([]byte, error) {
	trie.root.UpdateCurrEpoch(trie.currEpoch)
	return trie.root.Get(key, trie.flatdbNodeResolver)
}

func (t *VerkleTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	acc := &types.StateAccount{}
	versionkey := t.pointCache.GetTreeKeyVersionCached(addr[:])
	var (
		values [][]byte
		err    error
	)
	t.root.UpdateCurrEpoch(t.currEpoch)
	switch t.root.(type) {
	case *verkle.InternalNode:
		if t.currEpoch == 0 {
			values, err = t.root.(*verkle.InternalNode).GetStem(versionkey[:31], t.flatdbNodeResolver)
		} else {
			values, err = t.root.(*verkle.InternalNode).GetStemWithEpoch(versionkey[:31], versionkey[31], t.flatdbNodeResolver, t.currEpoch)
		}
	default:
		return nil, errInvalidRootType
	}
	if err != nil {
		return nil, fmt.Errorf("GetAccount (%x) error: %v", addr, err)
	}

	if values == nil {
		return nil, nil
	}
	if len(values[utils.NonceLeafKey]) > 0 {
		acc.Nonce = binary.LittleEndian.Uint64(values[utils.NonceLeafKey])
	}
	// if the account has been deleted, then values[10] will be 0 and not nil. If it has
	// been recreated after that, then its code keccak will NOT be 0. So return `nil` if
	// the nonce, and values[10], and code keccak is 0.

	if acc.Nonce == 0 && len(values) > 10 && len(values[10]) > 0 && bytes.Equal(values[utils.CodeKeccakLeafKey], zero[:]) {
		if !t.ended {
			return nil, errDeletedAccount
		} else {
			return nil, nil
		}
	}
	var balance [32]byte
	copy(balance[:], values[utils.BalanceLeafKey])
	for i := 0; i < len(balance)/2; i++ {
		balance[len(balance)-i-1], balance[i] = balance[i], balance[len(balance)-i-1]
	}
	// var balance [32]byte
	// if len(values[utils.BalanceLeafKey]) > 0 {
	// 	for i := 0; i < len(balance); i++ {
	// 		balance[len(balance)-i-1] = values[utils.BalanceLeafKey][i]
	// 	}
	// }
	acc.Balance = new(big.Int).SetBytes(balance[:])
	acc.CodeHash = values[utils.CodeKeccakLeafKey]
	// TODO fix the code size as well

	return acc, nil
}

var zero [32]byte

func (t *VerkleTrie) UpdateAccount(addr common.Address, acc *types.StateAccount) error {
	var (
		err            error
		nonce, balance [32]byte
		values         = make([][]byte, verkle.NodeWidth)
		versionKey     = t.pointCache.GetTreeKeyVersionCached(addr[:])
	)

	// Only evaluate the polynomial once
	values[utils.VersionLeafKey] = zero[:]
	values[utils.NonceLeafKey] = nonce[:]
	values[utils.BalanceLeafKey] = balance[:]
	values[utils.CodeKeccakLeafKey] = acc.CodeHash[:]

	binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
	bbytes := acc.Balance.Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			balance[len(bbytes)-i-1] = b
		}
	}

	t.root.UpdateCurrEpoch(t.currEpoch)
	switch root := t.root.(type) {
	case *verkle.InternalNode:
		if t.currEpoch == 0 {
			err = root.InsertStem(versionKey[:31], values, t.flatdbNodeResolver)
		} else {
			err = root.InsertStemWithEpoch(versionKey[:31], versionKey[31], values, t.flatdbNodeResolver, t.currEpoch)
		}
	default:
		return errInvalidRootType
	}
	if err != nil {
		return fmt.Errorf("UpdateAccount (%x) error: %v", addr, err)
	}
	// TODO figure out if the code size needs to be updated, too

	return nil
}

func (trie *VerkleTrie) UpdateStem(key []byte, values [][]byte) error {
	switch root := trie.root.(type) {
	case *verkle.InternalNode:
		return root.InsertStem(key, values, trie.flatdbNodeResolver)
	default:
		panic("invalid root type")
	}
}

// Update associates key with value in the trie. If value has length zero, any
// existing value is deleted from the trie. The value bytes must not be modified
// by the caller while they are stored in the trie. If a node was not found in the
// database, a trie.MissingNodeError is returned.
func (trie *VerkleTrie) UpdateStorage(address common.Address, key, value []byte) error {
	k := utils.GetTreeKeyStorageSlotWithEvaluatedAddress(trie.pointCache.GetTreeKeyHeader(address[:]), key)
	var v [32]byte
	if len(value) >= 32 {
		copy(v[:], value[:32])
	} else {
		copy(v[32-len(value):], value[:])
	}
	trie.root.UpdateCurrEpoch(trie.currEpoch)
	return trie.root.Insert(k, v[:], trie.flatdbNodeResolver)
}

func (t *VerkleTrie) DeleteAccount(addr common.Address) error {
	var (
		err    error
		values = make([][]byte, verkle.NodeWidth)
		stem   = t.pointCache.GetTreeKeyVersionCached(addr[:])
	)
	t.root.UpdateCurrEpoch(t.currEpoch)

	for i := 0; i < verkle.NodeWidth; i++ {
		values[i] = zero[:]
	}

	switch root := t.root.(type) {
	case *verkle.InternalNode:
		err = root.InsertStem(stem, values, t.flatdbNodeResolver)
	default:
		return errInvalidRootType
	}
	if err != nil {
		return fmt.Errorf("DeleteAccount (%x) error: %v", addr, err)
	}
	// TODO figure out if the code size needs to be updated, too

	return nil
}

// Delete removes any existing value for key from the trie. If a node was not
// found in the database, a trie.MissingNodeError is returned.
func (trie *VerkleTrie) DeleteStorage(addr common.Address, key []byte) error {
	pointEval := trie.pointCache.GetTreeKeyHeader(addr[:])
	k := utils.GetTreeKeyStorageSlotWithEvaluatedAddress(pointEval, key)
	var zero [32]byte
	trie.root.UpdateCurrEpoch(trie.currEpoch)

	return trie.root.Insert(k, zero[:], trie.flatdbNodeResolver)
}

// Hash returns the root hash of the trie. It does not write to the database and
// can be used even if the trie doesn't have one.
func (trie *VerkleTrie) Hash() common.Hash {
	return trie.root.Commit().Bytes()
}

func nodeToDBKey(n verkle.VerkleNode) []byte {
	ret := n.Commitment().Bytes()
	return ret[:]
}

// Commit writes all nodes to the trie's memory database, tracking the internal
// and external (for account tries) references.
func (trie *VerkleTrie) Commit(_ bool) (common.Hash, *trienode.NodeSet, error) {
	root, ok := trie.root.(*verkle.InternalNode)
	if !ok {
		return common.Hash{}, nil, errors.New("unexpected root node type")
	}
	nodes, err := root.BatchSerialize()
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("serializing tree nodes: %s", err)
	}

	batch := trie.db.diskdb.NewBatch()
	path := make([]byte, 0, len(FlatDBVerkleNodeKeyPrefix)+32)
	path = append(path, FlatDBVerkleNodeKeyPrefix...)
	for _, node := range nodes {
		path := append(path[:len(FlatDBVerkleNodeKeyPrefix)], node.Path...)

		if err := batch.Put(path, node.SerializedBytes); err != nil {
			return common.Hash{}, nil, fmt.Errorf("put node to disk: %s", err)
		}

		if batch.ValueSize() >= ethdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()

	// Serialize root commitment form
	rootH := root.Hash().BytesLE()
	return common.BytesToHash(rootH[:]), nil, nil
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration
// starts at the key after the given start key.
func (trie *VerkleTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	return newVerkleNodeIterator(trie, nil)
}

// Prove constructs a Merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root), ending
// with the node that proves the absence of the key.
func (trie *VerkleTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	panic("not implemented")
}

func (trie *VerkleTrie) Copy() *VerkleTrie {
	return &VerkleTrie{
		root:       trie.root.Copy(),
		db:         trie.db,
		pointCache: utils.NewPointCache(),
		currEpoch:  trie.currEpoch,
	}
}

func (trie *VerkleTrie) IsVerkle() bool {
	return true
}

func (trie *VerkleTrie) ProveAndSerialize(keys [][]byte) (*verkle.VerkleProof, verkle.StateDiff, error) {
	proof, _, _, _, err := verkle.MakeVerkleMultiProof(trie.root, nil, keys, nil)
	if err != nil {
		return nil, nil, err
	}

	p, kvps, err := verkle.SerializeProof(proof)
	if err != nil {
		return nil, nil, err
	}

	return p, kvps, nil
}

type set = map[string]struct{}

func addKey(s set, key []byte) {
	s[string(key)] = struct{}{}
}

func DeserializeAndVerifyVerkleProof(vp *verkle.VerkleProof, root []byte, statediff verkle.StateDiff) error {
	rootC := new(verkle.Point)
	// rootC.SetBytesTrusted(root)
	proof, cis, indices, yis, err := deserializeVerkleProof(vp, rootC, statediff)
	if err != nil {
		return fmt.Errorf("could not deserialize proof: %w", err)
	}
	cfg := verkle.GetConfig()
	if ok, err := verkle.VerifyVerkleProof(proof, cis, indices, yis, cfg); !ok && err != nil {
		return errInvalidProof
	}

	return nil
}

func deserializeVerkleProof(vp *verkle.VerkleProof, rootC *verkle.Point, statediff verkle.StateDiff) (*verkle.Proof, []*verkle.Point, []byte, []*verkle.Fr, error) {
	var others set = set{} // Mark when an "other" stem has been seen

	proof, err := verkle.DeserializeProof(vp, statediff)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("verkle proof deserialization error: %w", err)
	}

	for _, stem := range proof.PoaStems {
		addKey(others, stem)
	}

	if len(proof.Keys) != len(proof.PreValues) {
		return nil, nil, nil, nil, fmt.Errorf("keys and values are of different length %d != %d", len(proof.Keys), len(proof.PreValues))
	}

	// tree, err := verkle.TreeFromProof(proof, rootC)
	// if err != nil {
	// 	return nil, nil, nil, nil, fmt.Errorf("error rebuilding the tree from proof: %w", err)
	// }
	// for _, stemdiff := range statediff {
	// 	for _, suffixdiff := range stemdiff.SuffixDiffs {
	// 		var key [32]byte
	// 		copy(key[:31], stemdiff.Stem[:])
	// 		key[31] = suffixdiff.Suffix

	// 		val, err := tree.Get(key[:], nil)
	// 		if err != nil {
	// 			return nil, nil, nil, nil, fmt.Errorf("could not find key %x in tree rebuilt from proof: %w", key, err)
	// 		}
	// 		if len(val) > 0 {
	// 			if !bytes.Equal(val, suffixdiff.CurrentValue[:]) {
	// 				return nil, nil, nil, nil, fmt.Errorf("could not find correct value at %x in tree rebuilt from proof: %x != %x", key, val, *suffixdiff.CurrentValue)
	// 			}
	// 		} else {
	// 			if suffixdiff.CurrentValue != nil && len(suffixdiff.CurrentValue) != 0 {
	// 				return nil, nil, nil, nil, fmt.Errorf("could not find correct value at %x in tree rebuilt from proof: %x != %x", key, val, *suffixdiff.CurrentValue)
	// 			}
	// 		}
	// 	}
	// }

	// // no need to resolve as the tree has been reconstructed from the proof
	// // and must not contain any unresolved nodes.
	// pe, _, _, err := tree.GetProofItems(proof.Keys)
	// if err != nil {
	// 	return nil, nil, nil, nil, fmt.Errorf("could not get proof items from tree rebuilt from proof: %w", err)
	// }

	// return proof, pe.Cis, pe.Zis, pe.Yis, err
	return proof, nil, nil, nil, err
}

// ChunkedCode represents a sequence of 32-bytes chunks of code (31 bytes of which
// are actual code, and 1 byte is the pushdata offset).
type ChunkedCode []byte

// Copy the values here so as to avoid an import cycle
const (
	PUSH1  = byte(0x60)
	PUSH3  = byte(0x62)
	PUSH4  = byte(0x63)
	PUSH7  = byte(0x66)
	PUSH21 = byte(0x74)
	PUSH30 = byte(0x7d)
	PUSH32 = byte(0x7f)
)

// ChunkifyCode generates the chunked version of an array representing EVM bytecode
func ChunkifyCode(code []byte) ChunkedCode {
	var (
		chunkOffset = 0 // offset in the chunk
		chunkCount  = len(code) / 31
		codeOffset  = 0 // offset in the code
	)
	if len(code)%31 != 0 {
		chunkCount++
	}
	chunks := make([]byte, chunkCount*32)
	for i := 0; i < chunkCount; i++ {
		// number of bytes to copy, 31 unless
		// the end of the code has been reached.
		end := 31 * (i + 1)
		if len(code) < end {
			end = len(code)
		}

		// Copy the code itself
		copy(chunks[i*32+1:], code[31*i:end])

		// chunk offset = taken from the
		// last chunk.
		if chunkOffset > 31 {
			// skip offset calculation if push
			// data covers the whole chunk
			chunks[i*32] = 31
			chunkOffset = 1
			continue
		}
		chunks[32*i] = byte(chunkOffset)
		chunkOffset = 0

		// Check each instruction and update the offset
		// it should be 0 unless a PUSHn overflows.
		for ; codeOffset < end; codeOffset++ {
			if code[codeOffset] >= PUSH1 && code[codeOffset] <= PUSH32 {
				codeOffset += int(code[codeOffset] - PUSH1 + 1)
				if codeOffset+1 >= 31*(i+1) {
					codeOffset++
					chunkOffset = codeOffset - 31*(i+1)
					break
				}
			}
		}
	}

	return chunks
}

func (t *VerkleTrie) SetStorageRootConversion(addr common.Address, root common.Hash) {
	t.db.SetStorageRootConversion(addr, root)
}

func (t *VerkleTrie) ClearStrorageRootConversion(addr common.Address) {
	t.db.ClearStorageRootConversion(addr)
}

func (t *VerkleTrie) UpdateContractCode(addr common.Address, codeHash common.Hash, code []byte) error {
	var (
		chunks = ChunkifyCode(code)
		values [][]byte
		key    []byte
		err    error
	)
	for i, chunknr := 0, uint64(0); i < len(chunks); i, chunknr = i+32, chunknr+1 {
		groupOffset := (chunknr + 128) % 256
		if groupOffset == 0 /* start of new group */ || chunknr == 0 /* first chunk in header group */ {
			values = make([][]byte, verkle.NodeWidth)
			key = utils.GetTreeKeyCodeChunkWithEvaluatedAddress(t.pointCache.GetTreeKeyHeader(addr[:]), uint256.NewInt(chunknr))
		}
		values[groupOffset] = chunks[i : i+32]

		// Reuse the calculated key to also update the code size.
		if i == 0 {
			cs := make([]byte, 32)
			binary.LittleEndian.PutUint64(cs, uint64(len(code)))
			values[utils.CodeSizeLeafKey] = cs
		}

		if groupOffset == 255 || len(chunks)-i <= 32 {
			err = t.UpdateStem(key[:31], values)

			if err != nil {
				return fmt.Errorf("UpdateContractCode (addr=%x) error: %w", addr[:], err)
			}
		}
	}
	return nil
}
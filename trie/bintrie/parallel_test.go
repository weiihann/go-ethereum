// Copyright 2026 go-ethereum Authors
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

package bintrie

import "testing"

func TestCutDepthFor(t *testing.T) {
	tests := []struct {
		numCPU, groupDepth, want int
	}{
		{1, 5, 5},       // 2^5=32 >= 1
		{8, 5, 5},       // 2^5=32 >= 8
		{32, 5, 5},      // 2^5=32 >= 32
		{33, 5, 10},     // 2^5=32 < 33 -> 10
		{512, 5, 10},    // 2^10=1024 >= 512
		{0, 5, 5},       // clamp to >=1
		{4, 2, 2},       // 2^2=4 >= 4
		{5, 2, 4},       // 2^2=4 < 5 -> 4
		{999999, 5, 20}, // steps 5,10,15,20 -> 2^20 >= 999999 at d=20
		{999999, 8, 24}, // steps 8,16,24 -> stops at 24 since 24 >= 20
	}
	for _, tt := range tests {
		if got := cutDepthFor(tt.numCPU, tt.groupDepth); got != tt.want {
			t.Errorf("cutDepthFor(%d,%d)=%d want %d", tt.numCPU, tt.groupDepth, got, tt.want)
		}
	}
}

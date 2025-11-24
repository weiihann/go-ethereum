// Copyright 2025 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"fmt"
	"slices"

	// "github.com/ethereum/go-ethereum/cmd/utils"
	// "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/eth/krogan"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/urfave/cli/v2"
)

const (
	kroganBanner = `
 ___  __    ________  ________  ________  ________  ________      
|\  \|\  \ |\   __  \|\   __  \|\   ____\|\   __  \|\   ___  \    
\ \  \/  /|\ \  \|\  \ \  \|\  \ \  \___|\ \  \|\  \ \  \\ \  \   
 \ \   ___  \ \   _  _\ \  \\\  \ \  \  __\ \   __  \ \  \\ \  \  
  \ \  \\ \  \ \  \\  \\ \  \\\  \ \  \|\  \ \  \ \  \ \  \\ \  \ 
   \ \__\\ \__\ \__\\ _\\ \_______\ \_______\ \__\ \__\ \__\\ \__\
    \|__| \|__|\|__|\|__|\|_______|\|_______|\|__|\|__|\|__| \|__|                                                                                                                                                                       
	`
)

var kroganDbFlags = []cli.Flag{
	utils.DataDirFlag,
}

var kroganFlags = []cli.Flag{
	utils.HttpMasterNodesFlag,
	utils.WsMasterNodesFlag,
	utils.ChainSizeFlag,
}

var kroganCommand = &cli.Command{
	Name:        "krogan",
	Usage:       "Run a Krogan node",
	Action:      startKrogan,
	Description: "TODO(weiihann)",
	Flags: slices.Concat(
		kroganDbFlags,
		kroganFlags,
		rpcFlags,
		debug.Flags,
	),
	Before: func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	},
}

func startKrogan(ctx *cli.Context) error {
	if args := ctx.Args().Slice(); len(args) > 0 {
		return fmt.Errorf("invalid arguments: %v", args)
	}

	stack := makeKroganNode(ctx)
	defer stack.Close()

	log.Info(kroganBanner)

	stack.Start()
	stack.Wait()

	return nil
}

func makeKroganNode(ctx *cli.Context) *node.KroganNode {
	stack, cfg := makeConfigKroganNode(ctx)

	_, err := krogan.New(stack, &cfg.Node)
	if err != nil {
		utils.Fatalf("Failed to create the Krogan service: %v", err)
	}

	// TODO(weiihann): handle GraphQL stuff

	utils.SetupMetrics(&cfg.Metrics)
	return stack
}

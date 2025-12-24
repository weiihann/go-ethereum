#!/usr/bin/env python3
"""
Calculate cumulative state size from delta CSV output.

Reads statesize.csv (delta values per block) and computes cumulative totals.
"""

import csv
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CumulativeState:
    """Cumulative state size at a given block."""

    block_number: int
    state_root: str
    accounts: int = 0
    account_bytes: int = 0
    account_trienodes: int = 0
    account_trienode_bytes: int = 0
    contract_codes: int = 0
    contract_code_bytes: int = 0
    storages: int = 0
    storage_bytes: int = 0
    storage_trienodes: int = 0
    storage_trienode_bytes: int = 0

    def total_bytes(self) -> int:
        """Return total state size in bytes."""
        return (
            self.account_bytes
            + self.account_trienode_bytes
            + self.contract_code_bytes
            + self.storage_bytes
            + self.storage_trienode_bytes
        )

    def apply_delta(
        self,
        block_number: int,
        state_root: str,
        account_delta: int,
        account_bytes_delta: int,
        account_trienode_delta: int,
        account_trienode_bytes_delta: int,
        contract_code_delta: int,
        contract_code_bytes_delta: int,
        storage_delta: int,
        storage_bytes_delta: int,
        storage_trienode_delta: int,
        storage_trienode_bytes_delta: int,
    ) -> "CumulativeState":
        """Apply a delta and return a new cumulative state."""
        return CumulativeState(
            block_number=block_number,
            state_root=state_root,
            accounts=self.accounts + account_delta,
            account_bytes=self.account_bytes + account_bytes_delta,
            account_trienodes=self.account_trienodes + account_trienode_delta,
            account_trienode_bytes=self.account_trienode_bytes
            + account_trienode_bytes_delta,
            contract_codes=self.contract_codes + contract_code_delta,
            contract_code_bytes=self.contract_code_bytes + contract_code_bytes_delta,
            storages=self.storages + storage_delta,
            storage_bytes=self.storage_bytes + storage_bytes_delta,
            storage_trienodes=self.storage_trienodes + storage_trienode_delta,
            storage_trienode_bytes=self.storage_trienode_bytes
            + storage_trienode_bytes_delta,
        )


def calculate_cumulative(input_path: Path, output_path: Path | None = None) -> None:
    """
    Read delta CSV and calculate cumulative state size.

    Args:
        input_path: Path to statesize.csv with delta values
        output_path: Optional path for cumulative output CSV
    """
    state = CumulativeState(block_number=0, state_root="")

    output_headers = [
        "block_number",
        "state_root",
        "accounts",
        "account_bytes",
        "account_trienodes",
        "account_trienode_bytes",
        "contract_codes",
        "contract_code_bytes",
        "storages",
        "storage_bytes",
        "storage_trienodes",
        "storage_trienode_bytes",
        "total_bytes",
    ]

    out_file = None
    writer = None

    if output_path:
        out_file = open(output_path, "w", newline="")
        writer = csv.writer(out_file)
        writer.writerow(output_headers)

    try:
        with open(input_path, newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                state = state.apply_delta(
                    block_number=int(row["block_number"]),
                    state_root=row["state_root"],
                    account_delta=int(row["account_delta"]),
                    account_bytes_delta=int(row["account_bytes_delta"]),
                    account_trienode_delta=int(row["account_trienode_delta"]),
                    account_trienode_bytes_delta=int(
                        row["account_trienode_bytes_delta"]
                    ),
                    contract_code_delta=int(row["contract_code_delta"]),
                    contract_code_bytes_delta=int(row["contract_code_bytes_delta"]),
                    storage_delta=int(row["storage_delta"]),
                    storage_bytes_delta=int(row["storage_bytes_delta"]),
                    storage_trienode_delta=int(row["storage_trienode_delta"]),
                    storage_trienode_bytes_delta=int(
                        row["storage_trienode_bytes_delta"]
                    ),
                )

                if writer:
                    writer.writerow(
                        [
                            state.block_number,
                            state.state_root,
                            state.accounts,
                            state.account_bytes,
                            state.account_trienodes,
                            state.account_trienode_bytes,
                            state.contract_codes,
                            state.contract_code_bytes,
                            state.storages,
                            state.storage_bytes,
                            state.storage_trienodes,
                            state.storage_trienode_bytes,
                            state.total_bytes(),
                        ]
                    )

        # Print final state summary
        print(f"Final state at block {state.block_number}:")
        print(f"  State root: {state.state_root}")
        print(f"  Accounts: {state.accounts:,}")
        print(f"  Account bytes: {state.account_bytes:,}")
        print(f"  Account trie nodes: {state.account_trienodes:,}")
        print(f"  Account trie node bytes: {state.account_trienode_bytes:,}")
        print(f"  Contract codes: {state.contract_codes:,}")
        print(f"  Contract code bytes: {state.contract_code_bytes:,}")
        print(f"  Storage slots: {state.storages:,}")
        print(f"  Storage bytes: {state.storage_bytes:,}")
        print(f"  Storage trie nodes: {state.storage_trienodes:,}")
        print(f"  Storage trie node bytes: {state.storage_trienode_bytes:,}")
        print(f"  Total bytes: {state.total_bytes():,}")

    finally:
        if out_file:
            out_file.close()


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: statesize.py <input.csv> [output.csv]")
        print("  input.csv: Path to statesize.csv with delta values")
        print("  output.csv: Optional path for cumulative output")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    calculate_cumulative(input_path, output_path)


if __name__ == "__main__":
    main()

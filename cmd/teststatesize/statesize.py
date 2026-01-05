# %%
import pandas as pd
import matplotlib.pyplot as plt

# %%
# Read the CSV file
df = pd.read_csv("/Users/han/Documents/Codes/test/statesize.csv")

# %%
# Build a valid chain by following parent-root relationships
# This handles cases where there are multiple blocks with same number but different roots (reorgs)


def build_canonical_chain(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the canonical chain by following parent-root relationships.

    Algorithm:
    1. Create a mapping from root -> row data
    2. Find all roots that are referenced as parent_root (these have children)
    3. Tips are roots that have no children (not in any parent_root)
    4. For each tip, trace back through parent_root links
    5. Return the longest chain (or highest block number tip)
    """
    # Create root -> row mapping
    root_to_row = {row["root"]: row for _, row in df.iterrows()}

    # Find all parent roots (roots that have children)
    parent_roots = set(df["parent_root"].unique())

    # Tips are roots that are not anyone's parent (leaf nodes)
    all_roots = set(df["root"].unique())
    tips = all_roots - parent_roots

    if not tips:
        # No tips found - might be circular or empty, fall back to highest block
        tips = {df.loc[df["block_number"].idxmax(), "root"]}

    # For each tip, trace back to find the chain
    best_chain = []
    best_tip = None

    for tip in tips:
        chain = []
        current_root = tip

        # Trace back through parent roots
        while current_root in root_to_row:
            row = root_to_row[current_root]
            chain.append(row)
            current_root = row["parent_root"]

        # Reverse to get chronological order (oldest first)
        chain = chain[::-1]

        # Keep the longest chain, or if same length, the one with highest tip block
        if len(chain) > len(best_chain):
            best_chain = chain
            best_tip = tip
        elif len(chain) == len(best_chain) and chain:
            # Same length, prefer higher block number
            if chain[-1]["block_number"] > best_chain[-1]["block_number"]:
                best_chain = chain
                best_tip = tip

    if not best_chain:
        print("Warning: Could not build chain, using original order")
        return df

    print(f"Built canonical chain: {len(best_chain)} blocks")
    print(f"  First block: {best_chain[0]['block_number']}")
    print(f"  Last block:  {best_chain[-1]['block_number']}")
    print(f"  Tip root:    {best_tip[:18]}...")

    if len(best_chain) < len(df):
        orphaned = len(df) - len(best_chain)
        print(f"  Orphaned blocks (not in canonical chain): {orphaned}")

    return pd.DataFrame(best_chain)


# %%
# Build canonical chain
canonical_df = build_canonical_chain(df)

# %%
# Calculate cumulative state sizes on the canonical chain
canonical_df["accounts_cumulative"] = canonical_df["accounts_delta"].cumsum()
canonical_df["account_bytes_cumulative"] = canonical_df["account_bytes_delta"].cumsum()
canonical_df["storages_cumulative"] = canonical_df["storages_delta"].cumsum()
canonical_df["storage_bytes_cumulative"] = canonical_df["storage_bytes_delta"].cumsum()
canonical_df["account_trienodes_cumulative"] = canonical_df[
    "account_trienodes_delta"
].cumsum()
canonical_df["account_trienode_bytes_cumulative"] = canonical_df[
    "account_trienode_bytes_delta"
].cumsum()
canonical_df["storage_trienodes_cumulative"] = canonical_df[
    "storage_trienodes_delta"
].cumsum()
canonical_df["storage_trienode_bytes_cumulative"] = canonical_df[
    "storage_trienode_bytes_delta"
].cumsum()
canonical_df["codes_cumulative"] = canonical_df["codes_delta"].cumsum()
canonical_df["code_bytes_cumulative"] = canonical_df["code_bytes_delta"].cumsum()

# Total bytes cumulative
canonical_df["total_bytes_cumulative"] = (
    canonical_df["account_bytes_cumulative"]
    + canonical_df["storage_bytes_cumulative"]
    + canonical_df["account_trienode_bytes_cumulative"]
    + canonical_df["storage_trienode_bytes_cumulative"]
    + canonical_df["code_bytes_cumulative"]
)

# %%
# Display summary statistics
print("\nState Size Summary (Canonical Chain)")
print("=" * 50)
print(
    f"Block range: {canonical_df['block_number'].min()} - {canonical_df['block_number'].max()}"
)
print(f"Total blocks in chain: {len(canonical_df)}")
print()
print("Cumulative counts at latest block:")
print(f"  Accounts:           {canonical_df['accounts_cumulative'].iloc[-1]:,}")
print(f"  Storage slots:      {canonical_df['storages_cumulative'].iloc[-1]:,}")
print(
    f"  Account trie nodes: {canonical_df['account_trienodes_cumulative'].iloc[-1]:,}"
)
print(
    f"  Storage trie nodes: {canonical_df['storage_trienodes_cumulative'].iloc[-1]:,}"
)
print(f"  Code entries:       {canonical_df['codes_cumulative'].iloc[-1]:,}")
print()
print("Cumulative bytes at latest block:")
print(
    f"  Account bytes:           {canonical_df['account_bytes_cumulative'].iloc[-1]:,} ({canonical_df['account_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)
print(
    f"  Storage bytes:           {canonical_df['storage_bytes_cumulative'].iloc[-1]:,} ({canonical_df['storage_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)
print(
    f"  Account trie node bytes: {canonical_df['account_trienode_bytes_cumulative'].iloc[-1]:,} ({canonical_df['account_trienode_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)
print(
    f"  Storage trie node bytes: {canonical_df['storage_trienode_bytes_cumulative'].iloc[-1]:,} ({canonical_df['storage_trienode_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)
print(
    f"  Code bytes:              {canonical_df['code_bytes_cumulative'].iloc[-1]:,} ({canonical_df['code_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)
print(
    f"  Total bytes:             {canonical_df['total_bytes_cumulative'].iloc[-1]:,} ({canonical_df['total_bytes_cumulative'].iloc[-1] / 1e9:.2f} GB)"
)

# %%
# Plot cumulative bytes over blocks
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Plot 1: Total cumulative bytes
ax1 = axes[0, 0]
ax1.plot(
    canonical_df["block_number"],
    canonical_df["total_bytes_cumulative"] / 1e9,
    label="Total",
    linewidth=2,
)
ax1.set_xlabel("Block Number")
ax1.set_ylabel("Cumulative Size (GB)")
ax1.set_title("Total State Size")
ax1.grid(True, alpha=0.3)
ax1.legend()

# Plot 2: Bytes by category
ax2 = axes[0, 1]
ax2.plot(
    canonical_df["block_number"],
    canonical_df["account_bytes_cumulative"] / 1e9,
    label="Accounts",
)
ax2.plot(
    canonical_df["block_number"],
    canonical_df["storage_bytes_cumulative"] / 1e9,
    label="Storage",
)
ax2.plot(
    canonical_df["block_number"],
    canonical_df["account_trienode_bytes_cumulative"] / 1e9,
    label="Account Trie",
)
ax2.plot(
    canonical_df["block_number"],
    canonical_df["storage_trienode_bytes_cumulative"] / 1e9,
    label="Storage Trie",
)
ax2.plot(
    canonical_df["block_number"],
    canonical_df["code_bytes_cumulative"] / 1e9,
    label="Code",
)
ax2.set_xlabel("Block Number")
ax2.set_ylabel("Cumulative Size (GB)")
ax2.set_title("State Size by Category")
ax2.grid(True, alpha=0.3)
ax2.legend()

# Plot 3: Cumulative counts
ax3 = axes[1, 0]
ax3.plot(
    canonical_df["block_number"],
    canonical_df["accounts_cumulative"] / 1e6,
    label="Accounts",
)
ax3.plot(
    canonical_df["block_number"],
    canonical_df["storages_cumulative"] / 1e6,
    label="Storage Slots",
)
ax3.set_xlabel("Block Number")
ax3.set_ylabel("Count (Millions)")
ax3.set_title("Cumulative Counts")
ax3.grid(True, alpha=0.3)
ax3.legend()

# Plot 4: Trie node counts
ax4 = axes[1, 1]
ax4.plot(
    canonical_df["block_number"],
    canonical_df["account_trienodes_cumulative"] / 1e6,
    label="Account Trie Nodes",
)
ax4.plot(
    canonical_df["block_number"],
    canonical_df["storage_trienodes_cumulative"] / 1e6,
    label="Storage Trie Nodes",
)
ax4.set_xlabel("Block Number")
ax4.set_ylabel("Count (Millions)")
ax4.set_title("Trie Node Counts")
ax4.grid(True, alpha=0.3)
ax4.legend()

plt.tight_layout()
plt.savefig("statesize_analysis.png", dpi=150)
plt.show()

# %%
# Save cumulative data to a new CSV
cumulative_df = canonical_df[
    [
        "block_number",
        "root",
        "accounts_cumulative",
        "account_bytes_cumulative",
        "storages_cumulative",
        "storage_bytes_cumulative",
        "account_trienodes_cumulative",
        "account_trienode_bytes_cumulative",
        "storage_trienodes_cumulative",
        "storage_trienode_bytes_cumulative",
        "codes_cumulative",
        "code_bytes_cumulative",
        "total_bytes_cumulative",
    ]
]
cumulative_df.to_csv("statesize_cumulative.csv", index=False)
print("\nCumulative data saved to statesize_cumulative.csv")

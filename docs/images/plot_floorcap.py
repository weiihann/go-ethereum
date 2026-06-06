#!/usr/bin/env python3
"""Render the floor-2 / cap-N archive-expiry sweep figure.

Data is the mainnet floor/cap sweep (head 19,999,256, inactive-min-age 2),
see docs/eip8188-archive-expiry-end-to-end.md and the run dirs
eip8188-mainnet-runs/ae-footprint-2026060{5,6}-fc-cap{2,3,4,5}/.

    python3 docs/images/plot_floorcap.py
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

CAPS = [2, 3, 4, 5]
DISK_SAVED = [35.54, 54.28, 62.58, 64.87]  # net compressed, GB saved (positive magnitude)
MAX_LEAVES = [16, 73, 295, 1118]  # worst-case leaves rebuilt per cold read

BLUE, RED = "#1f77b4", "#d62728"


def tradeoff() -> None:
    """Dual-axis: disk saved (up, good) vs worst-case rebuild (up, slower)."""
    fig, ax1 = plt.subplots(figsize=(7.2, 4.4))
    ax1.set_xlabel("subtree height cap (floor fixed at 2)")
    ax1.set_ylabel("disk saved, compressed (GB)", color=BLUE)
    ax1.plot(CAPS, DISK_SAVED, "o-", color=BLUE, lw=2.2)
    ax1.tick_params(axis="y", labelcolor=BLUE)
    ax1.set_xticks(CAPS)
    ax1.set_ylim(0, 78)
    for x, y in zip(CAPS, DISK_SAVED):
        ax1.annotate(f"-{y:.1f}", (x, y), textcoords="offset points",
                     xytext=(0, 9), ha="center", color=BLUE, fontsize=9)

    ax2 = ax1.twinx()
    ax2.set_ylabel("worst-case leaves rebuilt per read", color=RED)
    ax2.plot(CAPS, MAX_LEAVES, "s--", color=RED, lw=2.2)
    ax2.tick_params(axis="y", labelcolor=RED)
    ax2.set_yscale("log")
    for x, y in zip(CAPS, MAX_LEAVES):
        ax2.annotate(f"{y}", (x, y), textcoords="offset points",
                     xytext=(0, -15), ha="center", color=RED, fontsize=9)

    ax1.annotate("most saving,\nstill a small rebuild", (3, 54.28),
                 textcoords="offset points", xytext=(26, -10), ha="left", fontsize=9,
                 arrowprops=dict(arrowstyle="->"))
    ax1.set_title("Choosing the cap: more disk saved, but a slower rebuild")
    ax1.grid(True, axis="x", ls=":", alpha=0.4)
    fig.tight_layout()
    fig.savefig("docs/images/floorcap-tradeoff.png", dpi=130)
    plt.close(fig)


if __name__ == "__main__":
    tradeoff()
    print("wrote docs/images/floorcap-tradeoff.png")

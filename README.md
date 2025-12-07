# Notebooks

Everything currently lives in the `peerdas/` directory.

## Getting Started

1. `cd peerdas`
2. Install deps with `uv sync`
3. Create a `.env` file that defines `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD`
4. Launch any of the notebooks in order

<details>
<summary>.env file</summary>
```
CLICKHOUSE_HOST=your-host
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=your-user
CLICKHOUSE_PASSWORD=your-password
```
</details>

## Notebook Index
- [`01-blob-inclusion-per-block.ipynb`](peerdas/01-blob-inclusion-per-block.ipynb) — blob inclusion patterns per block and epoch
- [`02-blob-flow.ipynb`](peerdas/02-blob-flow.ipynb) — blob flow across validators, builders, and relays
- [`03-column-propagation.ipynb`](peerdas/03-column-propagation.ipynb) — column propagation timing across the 128 data columns

# PeerDAS Analysis

Analysis of blob and data column propagation on Ethereum mainnet.

## Setup

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Create a `.env` file with your ClickHouse credentials:
   ```
   CLICKHOUSE_HOST=your-host
   CLICKHOUSE_PORT=8443
   CLICKHOUSE_USER=your-user
   CLICKHOUSE_PASSWORD=your-password
   ```

3. Run the notebooks in order.

## Notebooks

### [01-blob-inclusion-per-block](01-blob-inclusion-per-block.ipynb)

Blob inclusion patterns in blocks:
- Blobs per slot (scatter plot)
- Blocks with blob counts per epoch (stacked bar)
- Blob count popularity by epoch (heatmap)
- Blob count per slot within epoch (heatmap)
- Blocks by blob count (histogram)

### [02-blob-flow](02-blob-flow.ipynb)

Blob flow through validators, builders, and relays:
- Validators -> Builders -> Blob Counts (Sankey)
- Blob count distribution by proposer entity (violin plot)
- Blob count distribution by relay (violin plot)
- Proposer Entity -> Blob Count (Sankey)
- Relay -> Blob Count (Sankey)
- Proposer Entity -> Relay (Sankey)
- Proposer Entity -> Relay -> Blob Count (Sankey)

### [03-column-propagation](03-column-propagation.ipynb)

Data column propagation timing across 128 columns:
- Column first seen (ms into slot start)
- Delta from fastest column (intraslot, ms)
- Delta normalized (0-1)

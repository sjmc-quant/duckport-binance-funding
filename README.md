# duckport-binance-funding

Standalone Binance USD-M funding-rate ingestor for duckport-rs.

## Tables

- `data.usdt_perp_funding`: funding-rate rows from historical files, WebSocket pre-settlement samples, and REST compensation, keyed by `(funding_time, symbol)`.

Historical files bulk-load the backfill. The live process always uses Binance's all-market mark-price stream, `!markPrice@arr@1s`, and writes one row per symbol for each funding settlement, sampled when the stream enters the final minute before `next_funding_time`. REST funding history writes the same table only as compensation when the WebSocket stream fails.

## Local Development

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e .
cp config.env.example config.env
```

Start duckport-rs first, then backfill historical funding files:

```bash
INGESTOR_ENV_FILE=./config.env loadhist
```

Run the live ingestor:

```bash
INGESTOR_ENV_FILE=./config.env binance-funding
```

## Verification

```bash
python -m unittest discover -s tests -v
```

# duckport-binance-funding

Standalone Binance USD-M funding-rate ingestor for duckport-rs.

## Tables

- `data.usdt_perp_funding`: settled USDT perpetual funding-rate rows from Binance USD-M REST funding history, keyed by `(funding_time, symbol)`.

The ingestor uses Binance `/fapi/v1/fundingRate` without a symbol parameter, then keeps rows whose `symbol` ends with `USDT`. On startup it first fills history from the current watermark, then runs once per hour after a configurable delay. Funding history requests are serial, use `limit=1000`, and continue from the exact funding watermark so missed polls are compensated by the next run.

Funding history requests are serial. Adjacent requests sleep for 0.2 seconds, and failed funding history requests sleep for 75 seconds before retrying.

## Local Development

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e .
cp config.env.example config.env
```

Start duckport-rs first, then run the ingestor:

```bash
INGESTOR_ENV_FILE=./config.env binance-funding
```

## Verification

```bash
python -m unittest discover -s tests -v
```

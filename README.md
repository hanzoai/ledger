# Hanzo Ledger

Programmable double-entry financial ledger. Atomic multi-posting transactions, account-based modeling, and scriptable with [Numscript](https://github.com/hanzoai/numscript) DSL.

## Architecture

```
hanzo/commerce    Storefront, catalog, orders
       |
hanzo/payments    Payment routing (50+ processors)
       |
hanzo/treasury    Ledger, reconciliation, wallets   <-- uses this
       |
lux/treasury      On-chain treasury, MPC/KMS wallets
```

## Features

- **Double-Entry** — Every transaction balances. No money created or destroyed.
- **Multi-Posting** — Atomic transactions across unlimited accounts in a single operation
- **Numscript DSL** — Model complex splits, fees, and routing with a purpose-built language
- **Immutable Audit Trail** — Append-only ledger with full transaction history
- **Multi-Currency** — Native support for any asset type with arbitrary precision
- **Metadata** — Attach structured metadata to accounts and transactions
- **Idempotency** — Safe retries with idempotency keys

## Quick Start

```bash
# Start with Docker
docker compose up -d

# Create a transaction
curl -X POST http://localhost:3068/v2/ledger/default/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "postings": [{
      "source": "world",
      "destination": "users:001",
      "amount": 10000,
      "asset": "USD/2"
    }]
  }'

# Query account balance
curl http://localhost:3068/v2/ledger/default/accounts/users:001
```

## Numscript Example

```numscript
// Multi-party fee split
send [USD/2 10000] (
  source = @users:001
  destination = {
    90% to @merchants:042
    10% to {
      50% to @platform:fees
      50% to @platform:reserve
    }
  }
)
```

## API

- `POST /v2/ledger/{name}/transactions` — Create transaction
- `GET /v2/ledger/{name}/transactions` — List transactions
- `GET /v2/ledger/{name}/accounts` — List accounts
- `GET /v2/ledger/{name}/accounts/{address}` — Get account balance
- `POST /v2/ledger/{name}/accounts/{address}/metadata` — Set metadata

## Development

```bash
go build ./...
go test ./...
```

## License

MIT — see [LICENSE](LICENSE)


# squeek-core

Backend Go untuk ekosistem **SqueekChain**: aggregator metrik on-chain + endpoint integrity.

## Endpoint
- `GET /api/integrity` -> `{"ts","measured","settled","lag","lagPct","index","fresh"}`
- Integrity = 100 - lag%*4 (cap 60) - stale (25/60) - incident 50 - queue anomali 30

## Deploy
- apiBase `https://api.squeekchain.com`, maxHistory 2880 sampel @30s, listen `:8890`
- Production: systemd unit `squeek-core.service` (User=agentuser, Restart=always) — JANGAN jalankan nohup dobel (port 8890 konflik)

## Build
```
go build -o squeek-core .
```

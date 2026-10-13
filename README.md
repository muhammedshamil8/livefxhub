# livefxhub

Real-time pricing engine. Consumes the LiveFXHub websocket price feed, computes its own
24h high / low / % change per symbol over a rolling window, keeps that state across restarts,
and serves it to any number of websocket clients as small delta updates.

Go 1.24, one external dependency (`gorilla/websocket`).

## Run

```bash
cp .env.example .env     # put API_KEY and SECRET_KEY in it
docker compose up        # or: docker-compose up
```

Then:

```bash
curl localhost:8080/healthz
# websocket endpoint: ws://localhost:8080/ws
```

Without Docker:

```bash
go run .
```

Config is all env vars (a `.env` file in the working dir is read too, real env wins):

| var             | default                                 |
|-----------------|-----------------------------------------|
| `API_KEY`       | required                                |
| `SECRET_KEY`    | required                                |
| `WS_URL`        | `wss://v3-prod.livefxhub.com/ws/algo`   |
| `LISTEN_ADDR`   | `:8080`                                 |
| `SNAPSHOT_PATH` | `data/snapshot.json` (`/data/...` in Docker) |
| `DEBUG_RAW`     | unset; set to log every raw feed frame  |

## Client protocol

```jsonc
// client -> server
{"action":"subscribe","symbols":["EURUSD","XAUUSD"]}
{"action":"unsubscribe","symbols":["EURUSD"]}

// server -> client: acks
{"event":"subscribed","symbols":["EURUSD","XAUUSD"]}
{"event":"unsubscribed","symbols":["EURUSD"]}
{"event":"error","message":"unknown action: foo"}

// server -> client: updates, batched, delta encoded
{"t":1791888214809,"u":[
  {"s":"EURUSD","b":1.11869,"a":1.11885,"h":1.11869,"l":1.11867,"c":0.0018},
  {"s":"XAUUSD","b":4180.365,"a":4180.645,"h":4180.495,"l":4180.205,"c":0}
]}
{"t":1791888214819,"u":[{"s":"XAUUSD","b":4180.315,"a":4180.595,"c":-0.0012}]}
```

- `s` symbol, `b` buy (bid), `a` sell (ask), `h`/`l` 24h high/low, `c` 24h change in %,
  `t` arrival time of the newest tick in the batch (unix ms).
- The first update for a symbol carries every field. After that only fields that changed are
  sent; merge them into your local copy. After `unsubscribe` + `subscribe` you get a full
  record again.
- On subscribe you immediately get the current state of every symbol we already have data for.
- Batches hold at most 32 symbols. `permessage-deflate` is used if your client offers it.

## How it works

```
LiveFXHub wss ──> feed ──chan──> hub ──outbox──> server writer ──ws──> clients
                  (reconnect,    (windows,       (batch, delta,
                   dedup)         fan-out)        per-client)
                                   │
                                   └── store (snapshot every 30s + on shutdown)
```

See [docs/architecture.md](docs/architecture.md) for the diagram and the goroutine layout.

- **`internal/feed`** – connects, authenticates, subscribes, reads. Reconnects with exponential
  backoff + jitter, detects dead links with ping/pong and a read deadline, drops consecutive
  duplicate ticks, sends a proper close frame on shutdown.
- **`internal/window`** – rolling 24h history per symbol as a ring of 1440 one-minute OHLC
  buckets. `Add` is O(1), `Stats` scans 1440 small structs (~7 µs). Handles gaps, late ticks and
  expiry. Fixed memory per symbol.
- **`internal/store`** – JSON snapshot of all buckets, written to a temp file and renamed
  (atomic), loaded on start. Buckets older than 24h are dropped on restore.
- **`internal/hub`** – one goroutine owns all windows and the subscriber lists. Ticks come in on
  a buffered channel, updates go out to each subscribed client's outbox. No locks on the windows.
- **`internal/server`** – `net/http` + websocket upgrade. One reader and one writer goroutine per
  client. The writer drains the outbox into delta batches. `/healthz` reports clients, symbols,
  frames, bytes and latency.
- **`main.go`** – wires it up and handles shutdown order: stop feed → stop http → drain hub →
  final snapshot.

## Design decisions and trade-offs

**Compute, don't trust.** The feed sends high/low/percentage but the task says ignore them.
We only use `bid`/`ask` from the feed; everything else is derived locally. The feed also sends
no timestamp, so arrival time is the clock for everything (windows, snapshots, latency).

**Minute buckets, not raw ticks.** Keeping every tick for 24h means millions of entries and a
rescan per update. 1440 OHLC buckets give the exact 24h high and low (max of bucket highs is
the true max) with constant memory. Only the % change is minute-granular: it compares against
the oldest bucket's open rather than the exact tick 24h ago. Fine for a price display.

**One owner goroutine, no mutexes on the hot path.** The hub goroutine is the only thing that
touches the windows. Register/subscribe/snapshot all run as closures on that goroutine. The one
lock in the system is a tiny per-client mutex around the outbox, held for microseconds.
Cost: the hub is single-core. At ~55 ticks/s after dedup that is nowhere near a limit.

**Coalesce, don't queue.** Each client has at most one pending update per symbol; a newer one
replaces it. A slow client therefore cannot grow memory, cannot block the hub, and cannot delay
other clients. It just sees fewer, fresher updates. Right for prices, wrong for a tick recorder.

**Delta + batch over binary.** Short JSON keys, only changed fields, several symbols per frame.
~67 bytes per update measured on the real feed, worst frame 760 bytes against a 10 KB limit.
Binary would roughly halve that again at the cost of debuggability; not worth it here.

**Duplicates: best effort.** No sequence numbers or timestamps in the feed, so a replayed
message is indistinguishable from an unchanged price. We drop a tick when bid and ask equal the
previous tick for that symbol. That is 43–47 % of messages on the real feed and never changes a
statistic. A replayed old message with a different price would get through.

**Snapshot every 30 s, JSON.** Worst case a hard kill loses 30 s = one bucket per symbol.
Full-state JSON is ~9 KB for 50 symbols and takes a few ms; written off the hub goroutine from a
copy. Binary would be smaller but JSON is inspectable, which mattered more while building this.

**One feed session per API key.** The server refuses a second login ("already have an active
session") and, if the previous process died without a close frame, keeps the key locked for
~15 minutes. So: close frame on SIGINT/SIGTERM/SIGHUP, 10 s stop grace in compose, and a 30 s
minimum backoff on that specific error. It also means horizontal scaling needs one key per
instance or a fan-out tier behind a single feed consumer.

## Performance (real feed, 13 Oct 2026)

| metric                                   | value                         |
|------------------------------------------|-------------------------------|
| feed input                               | ~100 msg/s, ~120 B each       |
| duplicates dropped before processing     | 43–47 %                       |
| `window.Add`                             | ~17 ns                        |
| `window.Stats` (1440 buckets)            | ~7.5 µs                       |
| feed arrival → client socket write       | avg 228 µs, max 1.4 ms        |
| client-observed latency (ms-resolution)  | ~1 ms                         |
| output per update                        | 65–69 B (was 146 B unbatched) |
| largest frame, 61-symbol subscriber      | 760 B                         |
| snapshot write, 50 symbols               | ~9 KB, few ms                 |
| image size                               | 22.8 MB                       |

Requirement was ≤ 50 ms and < 10 KB.

## Known limitations

- 11 of the 61 requested symbols never send data on this feed (BCHUSD, DOGUSD, DOTUSD, FILUSD,
  GBPCAD, GBPCHF, GBPJPY, LTCUSD, UNIUSD, USDPLN, XLMUSD). They are subscribed; they just never
  arrive.
- After a restart, until the first live tick for a symbol, the subscribe snapshot reports
  `a == b` (the snapshot stores bid-based buckets only).
- No auth or rate limiting on the client websocket.
- One hub goroutine; see scaling note above.

See [CHALLENGES.md](CHALLENGES.md) for what went wrong along the way.

# Challenges

Things that did not go as planned, roughly in the order they happened.

## The feed does not match the spec

The assignment describes `buy`, `sell`, `low_24h`, `high_24h`, `change_24h`, `timestamp`.
The real feed sends `event`, `symbol`, `bid`, `ask`, `high`, `low`, `percentage` and no
timestamp at all. Found by logging raw frames before writing any struct (`DEBUG_RAW=1` is the
leftover of that). Consequences: bid→buy, ask→sell, and arrival time is the only clock, which
affects the windows, out-of-order handling and how latency can be measured.

Also, 11 of the 61 symbols never sent a single message across a whole day of testing.

## One session per API key, and a stale lock

The server allows one connection per key. A second login gets
`{"event":"error","message":"already have an active session"}`. A clean close releases the
key immediately (two runs 5 s apart both got in). But a process that dies without sending a
websocket close frame, for example a closed terminal, leaves the key locked server-side.
I measured that at roughly 15 minutes with no client connected anywhere.

Fixes: close frame on SIGINT, SIGTERM and SIGHUP; `stop_grace_period: 10s` in compose so Docker
does not kill the process mid-shutdown; and the reconnect loop treats that specific error with a
30 s minimum backoff instead of retrying every second. It also rules out running two instances
on one key, which shapes the scaling story.

## Duplicates without sequence numbers

No seq, no timestamp, so a replayed message cannot be told apart from an unchanged price. The
only safe rule is "same bid and ask as the previous tick for this symbol → drop". That turned
out to be 43–47 % of all messages on the live feed, which was a surprise and a free win: the
window, the snapshot and the fan-out all see half the traffic. A replayed old message with a
different price still gets through. Documented as a limitation rather than guessed around.

## First server draft had a data race

Acks for subscribe/unsubscribe were written to the socket from the reader goroutine while the
writer goroutine was also writing. `gorilla/websocket` allows one concurrent writer. Caught on
review before it hit the race detector; fixed by routing acks through a channel into the single
writer. The same channel later carried the "forget this symbol's delta state" signal for
unsubscribe.

## Bursts overflowed even a fast client

With a per-client channel of 256 updates, a burst of 500 ticks dropped messages for a client
that was reading as fast as it could, because the writer encodes one frame at a time. Raising
the buffer just moves the cliff. The real fix was to stop queueing: an outbox with one slot per
symbol where a newer update replaces the pending one. Bounded memory, nothing dropped, and it
naturally produced batching (drain the outbox into one frame) and made delta encoding easy
(the writer already knows what it last sent).

## `go mod edit` downloading a toolchain

A small one. `go.mod` was created by a Go 1.25 install and then the machine was switched to the
apt Go 1.24. Every `go` command, including `go mod edit -go=1.24`, tried to download 1.25
first because `GOTOOLCHAIN=auto` reads `go.mod` before doing anything. `GOTOOLCHAIN=local go mod
edit -go=1.24` fixed it.

## Verifying restarts while the key was locked

The persistence test (run, kill, restart, check restore) needs the live feed, and the key was
locked for a stretch. Rather than wait, I wrote a small fake feed server speaking the same
protocol and ran the full sequence against it, then repeated it against the real server once the
lock cleared. Both agreed. The fake feed stayed useful for every later step.

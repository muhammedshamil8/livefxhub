package main

import (
	"bufio"
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"muhammedshamil8/livefxhub/internal/feed"
	"muhammedshamil8/livefxhub/internal/store"
	"muhammedshamil8/livefxhub/internal/window"
)

const (
	defaultWSURL = "wss://v3-prod.livefxhub.com/ws/algo"

	windowSpan = 24 * time.Hour
	bucketSize = time.Minute

	defaultSnapshotPath = "data/snapshot.json"
	snapshotEvery       = 30 * time.Second
)

var symbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD",
	"AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY",
	"EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF",
	"NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK",
	"USDSGD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD",
	"XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD",
	"BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	loadDotEnv(".env")

	apiKey := os.Getenv("API_KEY")
	secret := os.Getenv("SECRET_KEY")
	if apiKey == "" || secret == "" {
		log.Fatal("API_KEY and SECRET_KEY must be set (see .env.example)")
	}
	wsURL := os.Getenv("WS_URL")
	if wsURL == "" {
		wsURL = defaultWSURL
	}
	snapshotPath := os.Getenv("SNAPSHOT_PATH")
	if snapshotPath == "" {
		snapshotPath = defaultSnapshotPath
	}

	windows, err := loadWindows(snapshotPath)
	if err != nil {
		log.Fatalf("load snapshot: %v", err)
	}

	// SIGHUP too: a closed terminal must still send the websocket close frame
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	cfg := feed.Config{
		URL:     wsURL,
		APIKey:  apiKey,
		Secret:  secret,
		Symbols: symbols,
		Debug:   os.Getenv("DEBUG_RAW") != "",
	}

	// feed.Run calls this from one goroutine, so windows need no locking
	var count int
	lastReport, lastSave := time.Now(), time.Now()
	onTick := func(t feed.Tick) {
		w := windows[t.Symbol]
		if w == nil {
			w = window.New(windowSpan, bucketSize)
			windows[t.Symbol] = w
		}
		w.Add(t.Bid, t.At)
		count++

		if t.At.Sub(lastReport) >= 5*time.Second {
			st := w.Stats(t.At)
			log.Printf("%d ticks, %d/%d symbols | %s buy=%.5f sell=%.5f high=%.5f low=%.5f chg=%.2f%% (%d buckets)",
				count, len(windows), len(symbols), t.Symbol, t.Bid, t.Ask, st.High, st.Low, st.ChangePct, st.Buckets)
			lastReport = t.At
		}
		if t.At.Sub(lastSave) >= snapshotEvery {
			lastSave = t.At
			snap := snapshotOf(windows)
			go func() {
				if err := store.Save(snapshotPath, snap); err != nil {
					log.Printf("snapshot failed: %v", err)
				}
			}()
		}
	}

	runErr := feed.Run(ctx, cfg, onTick)

	// always save on the way out, whatever stopped us
	if err := store.Save(snapshotPath, snapshotOf(windows)); err != nil {
		log.Printf("final snapshot failed: %v", err)
	} else {
		log.Printf("snapshot saved to %s (%d symbols)", snapshotPath, len(windows))
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Fatalf("client stopped: %v", runErr)
	}
	log.Println("bye")
}

func loadWindows(path string) (map[string]*window.Window, error) {
	snap, err := store.Load(path)
	if err != nil {
		return nil, err
	}
	windows := map[string]*window.Window{}
	now := time.Now()
	buckets := 0
	for sym, bs := range snap.Symbols {
		w := window.New(windowSpan, bucketSize)
		w.Restore(bs, now)
		if w.Len() == 0 {
			continue
		}
		windows[sym] = w
		buckets += w.Len()
	}
	if len(windows) > 0 {
		log.Printf("restored %d symbols, %d buckets from %s (saved %s ago)",
			len(windows), buckets, path, now.Sub(snap.SavedAt).Round(time.Second))
	}
	return windows, nil
}

// Buckets() copies, so the snapshot can be written off the owning goroutine.
func snapshotOf(windows map[string]*window.Window) store.Snapshot {
	s := store.Snapshot{Symbols: make(map[string][]window.Bucket, len(windows))}
	for sym, w := range windows {
		s.Symbols[sym] = w.Buckets()
	}
	return s
}

// minimal .env loader, real env vars take precedence
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

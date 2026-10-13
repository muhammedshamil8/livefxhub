package main

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"muhammedshamil8/livefxhub/internal/feed"
	"muhammedshamil8/livefxhub/internal/hub"
	"muhammedshamil8/livefxhub/internal/server"
	"muhammedshamil8/livefxhub/internal/store"
	"muhammedshamil8/livefxhub/internal/window"
)

const (
	defaultWSURL = "wss://v3-prod.livefxhub.com/ws/algo"
	defaultAddr  = ":8080"

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
	wsURL := env("WS_URL", defaultWSURL)
	addr := env("LISTEN_ADDR", defaultAddr)
	snapshotPath := env("SNAPSHOT_PATH", defaultSnapshotPath)

	windows, err := loadWindows(snapshotPath)
	if err != nil {
		log.Fatalf("load snapshot: %v", err)
	}

	// SIGHUP too: a closed terminal must still send the websocket close frame
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h := hub.New(windows, hub.Options{WindowSpan: windowSpan, BucketSize: bucketSize})
	hubDone := make(chan struct{})
	go func() { h.Run(ctx); close(hubDone) }()

	srv := &http.Server{Addr: addr, Handler: server.New(h).Handler()}
	go func() {
		log.Printf("websocket server on %s (/ws, /healthz)", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server: %v", err)
			cancel()
		}
	}()

	go func() {
		t := time.NewTicker(snapshotEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := store.Save(snapshotPath, h.Snapshot()); err != nil {
					log.Printf("snapshot failed: %v", err)
				}
			}
		}
	}()

	var pushed, dropped int
	lastReport := time.Now()
	feedErr := feed.Run(ctx, feed.Config{
		URL: wsURL, APIKey: apiKey, Secret: secret, Symbols: symbols,
		Debug: os.Getenv("DEBUG_RAW") != "",
	}, func(t feed.Tick) {
		if h.Push(hub.Tick{Symbol: t.Symbol, Bid: t.Bid, Ask: t.Ask, At: t.At}) {
			pushed++
		} else {
			dropped++
		}
		if t.At.Sub(lastReport) >= 10*time.Second {
			log.Printf("feed: %d ticks pushed, %d dropped, %d symbols, %d clients", pushed, dropped, h.Symbols(), h.Clients())
			lastReport = t.At
		}
	})

	// shutdown: stop accepting, drain the hub, save, exit
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
	<-hubDone

	if err := store.Save(snapshotPath, h.Snapshot()); err != nil {
		log.Printf("final snapshot failed: %v", err)
	} else {
		log.Printf("snapshot saved to %s (%d symbols)", snapshotPath, h.Symbols())
	}

	if feedErr != nil && !errors.Is(feedErr, context.Canceled) {
		log.Fatalf("client stopped: %v", feedErr)
	}
	log.Println("bye")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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

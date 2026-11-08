package main

// squeek-core: read-only backend for SqueekPulse v2 & SqueekFi v2.
// No private keys, no signing. Polls the public Squeekchain API,
// computes Integrity Index, keeps history, runs alert rules,
// streams live updates via SSE, serves optional Telegram alerts.

import (
	"flag"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiBase   = "https://api.squeekchain.com"
	defaultStaleAfter = 120 // seconds
	maxHistory        = 2880 // 24h @ 30s
)

// ---------- data model ----------

type Snapshot struct {
	TS          int64   `json:"ts"`
	Measured    float64 `json:"measured"`
	Settled     float64 `json:"settled"`
	Lag         float64 `json:"lag"`
	LagPct      float64 `json:"lagPct"`
	Index       float64 `json:"index"`
	Fresh       bool    `json:"fresh"`
	Raw         json.RawMessage `json:"-"`
	FetchErr    string  `json:"fetchErr,omitempty"`
}

type Alert struct {
	TS     int64  `json:"ts"`
	Kind   string `json:"kind"`
	Msg    string `json:"msg"`
	Level  string `json:"level"` // info|warn|crit
	Sent   bool   `json:"sent"`
}

type Store struct {
	mu       sync.RWMutex
	history  []Snapshot
	alerts   []Alert
	last     Snapshot
	lastFetch time.Time
	tgToken  string
	tgChat   string
	httpc    *http.Client
}

// ---------- api fetch ----------

func fetchJSON(url string, c *http.Client) (json.RawMessage, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "squeek-core/2.0 (read-only)")
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// probeAPI mirrors SqueekPulse logic: measured vs settled -> lag & index.
func (s *Store) probe() Snapshot {
	snap := Snapshot{TS: time.Now().Unix()}
	raw, err := fetchJSON(apiBase+"/v1/status", s.httpc)
	if err != nil {
		snap.FetchErr = err.Error()
		s.mu.Lock()
		if len(s.history) > 0 { // keep last index but mark stale
			snap = s.last
			snap.TS = time.Now().Unix()
			snap.Fresh = false
			snap.FetchErr = err.Error()
		}
		s.mu.Unlock()
		return snap
	}
	var st struct {
		Builder struct {
			Anchor struct {
				CumulativeWork string `json:"cumulativeWork"`
			} `json:"anchor"`
		} `json:"builder"`
		Measurement struct {
			CumulativeWorkUnits string `json:"cumulativeWorkUnits"`
			Freshness           string `json:"freshness"`
			StaleAfterSeconds   int    `json:"staleAfterSeconds"`
		} `json:"measurement"`
	}
	if err := json.Unmarshal(raw, &st); err == nil {
		snap.Measured = parseNum(st.Measurement.CumulativeWorkUnits)
		snap.Settled = parseNum(st.Builder.Anchor.CumulativeWork)
		snap.Lag = snap.Measured - snap.Settled
		if snap.Measured > 0 {
			snap.LagPct = snap.Lag / snap.Measured * 100
		}
		snap.Fresh = st.Measurement.Freshness == "online" || st.Measurement.Freshness == ""
		snap.Index = integrityIndex(snap.LagPct, snap.Fresh)
	} else {
		snap.FetchErr = "parse: " + err.Error()
	}
	return snap
}

func parseNum(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

func integrityIndex(lagPct float64, fresh bool) float64 {
	idx := 100.0
	lagScore := lagPct * 4
	if lagScore > 60 {
		lagScore = 60
	}
	idx -= lagScore
	if !fresh {
		idx -= 60
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}

// ---------- alert engine ----------

func (s *Store) evaluate(snap Snapshot) {
	var fire []Alert
	add := func(kind, level, msg string) {
		fire = append(fire, Alert{TS: time.Now().Unix(), Kind: kind, Level: level, Msg: msg})
	}
	idx := snap.Index
	if snap.FetchErr != "" && !snap.Fresh {
		add("fetch", "crit", "API fetch failed: "+snap.FetchErr)
	} else if idx < 40 {
		add("index", "crit", fmt.Sprintf("Integrity Index CRITICAL: %.0f/100 (lag %.1f%%)", idx, snap.LagPct))
	} else if idx < 70 {
		add("index", "warn", fmt.Sprintf("Integrity Index dropped: %.0f/100 (lag %.1f%%)", idx, snap.LagPct))
	} else if idx >= 85 {
		add("index", "info", fmt.Sprintf("Integrity Index healthy: %.0f/100", idx))
	}
	if len(fire) == 0 {
		return
	}
	s.mu.Lock()
	// dedupe: same kind+level within 10 min = skip
	now := time.Now().Unix()
	var toSend []Alert
	for _, a := range fire {
		dup := false
		for i := len(s.alerts) - 1; i >= 0 && now-s.alerts[i].TS < 600; i-- {
			if s.alerts[i].Kind == a.Kind && s.alerts[i].Level == a.Level {
				dup = true
				break
			}
		}
		if !dup {
			s.alerts = append(s.alerts, a)
			toSend = append(toSend, a)
		}
	}
	if len(s.alerts) > 200 {
		s.alerts = s.alerts[len(s.alerts)-200:]
	}
	s.mu.Unlock()
	for _, a := range toSend {
		go s.sendTelegram(a)
	}
}

func (s *Store) sendTelegram(a Alert) {
	if s.tgToken == "" || s.tgChat == "" {
		return
	}
	body := fmt.Sprintf(`{"chat_id":%q,"text":"🐹 squeek-core [%s] %s"}`,
		s.tgChat, strings.ToUpper(a.Level), a.Msg)
	req, _ := http.NewRequest("POST",
		"https://api.telegram.org/bot"+s.tgToken+"/sendMessage",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpc.Do(req)
	if err == nil {
		resp.Body.Close()
		s.mu.Lock()
		for i := range s.alerts {
			if s.alerts[i].TS == a.TS && s.alerts[i].Kind == a.Kind {
				s.alerts[i].Sent = true
			}
		}
		s.mu.Unlock()
	}
}

// ---------- poller ----------

func (s *Store) run(interval time.Duration) {
	for {
		snap := s.probe()
		s.mu.Lock()
		s.last = snap
		s.history = append(s.history, snap)
		if len(s.history) > maxHistory {
			s.history = s.history[len(s.history)-maxHistory:]
		}
		s.mu.Unlock()
		s.evaluate(snap)
		time.Sleep(interval)
	}
}

// ---------- http handlers ----------

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func (s *Store) handleIntegrity(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	s.mu.RLock()
	last := s.last
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(last)
}

func (s *Store) handleHistory(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	hours := 24
	if h, err := strconv.Atoi(r.URL.Query().Get("hours")); err == nil && h > 0 && h <= 24 {
		hours = h
	}
	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour).Unix()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Snapshot{}
	for _, snap := range s.history {
		if snap.TS >= cutoff {
			out = append(out, snap)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *Store) handleAlerts(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	s.mu.RLock()
	defer s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.alerts)
}

func (s *Store) handleStream(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no streaming", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ctx := r.Context()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.mu.RLock()
			last := s.last
			s.mu.RUnlock()
			b, _ := json.Marshal(last)
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		}
	}
}

func (s *Store) handleHealth(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	s.mu.RLock()
	n := len(s.history)
	last := s.last
	s.mu.RUnlock()
	fmt.Fprintf(w, `{"ok":true,"snapshots":%d,"lastTs":%d}`, n, last.TS)
}

// ---------- static file serving (optional, for local testing) ----------

func main() {
	addr := flag.String("addr", "127.0.0.1:8890", "listen address")
	staticDir := flag.String("static", "", "optional static file dir (SqueekPulse root + squeekfi/)")
	flag.Parse()

	s := &Store{
		tgToken: os.Getenv("TG_TOKEN"),
		tgChat:  os.Getenv("TG_CHAT"),
		httpc:   &http.Client{Timeout: 15 * time.Second},
		last:    Snapshot{TS: 0},
	}
	go s.run(30 * time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/integrity", s.handleIntegrity)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/alerts", s.handleAlerts)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/healthz", s.handleHealth)

	if *staticDir != "" {
		fs := http.FileServer(http.Dir(*staticDir))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			if p == "/" || p == "/index.html" || p == "/squeekfi" || strings.HasPrefix(p, "/squeekfi/") {
				w.Header().Set("Cache-Control", "no-cache")
				if p == "/squeekfi" {
					http.Redirect(w, r, "/squeekfi/", http.StatusMovedPermanently)
					return
				}
			}
			fs.ServeHTTP(w, r)
		})
	}

	log.Printf("squeek-core listening on %s (read-only, no keys)", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

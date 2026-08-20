package nntp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
)

func newMetricsStore(t *testing.T) *hybrid.Store {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	store, err := hybrid.New(hybrid.Config{
		DataPath:            t.TempDir() + "/provider_metrics.db",
		CacheSize:           64,
		SyncInterval:        -1,
		CompactionThreshold: 0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestCanonicalProviderKeyIncludesEndpointAndTLS(t *testing.T) {
	provider := config.UsenetProvider{Host: "News.Example.COM.", Port: 563, SSL: true}
	if got, want := canonicalProviderKey(provider), "news.example.com:563:tls"; got != want {
		t.Fatalf("canonicalProviderKey() = %q, want %q", got, want)
	}
	provider.SSL = false
	if got, want := canonicalProviderKey(provider), "news.example.com:563:plain"; got != want {
		t.Fatalf("canonicalProviderKey() = %q, want %q", got, want)
	}
}

func TestProviderUsagePersistsConcurrentAndResetsEveryThirtyDays(t *testing.T) {
	store := newMetricsStore(t)
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	providerKey := canonicalProviderKey(provider)
	clock := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.Local)
	now := func() time.Time { return clock }
	m := newProviderMonitorWithClock(nil, []config.UsenetProvider{provider}, store, now)

	m.RecordUsage(providerKey, 11)
	m.Flush()
	clock = time.Date(2026, time.January, 30, 12, 0, 0, 0, time.Local)
	m.RecordUsage(providerKey, 22)

	const goroutines = 32
	const increments = 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range increments {
				m.RecordUsage(providerKey, 1)
			}
		}()
	}
	wg.Wait()
	m.Flush()
	_, usage := m.Stats(provider)
	if got, want := usage.Period30dBytes, int64(11+22+goroutines*increments); got != want {
		t.Fatalf("first-period bytes = %d, want %d", got, want)
	}
	if usage.WindowStart != "2026-01-01" || usage.WindowEnd != "2026-01-30" {
		t.Fatalf("unexpected first period: %+v", usage)
	}

	clock = time.Date(2026, time.January, 31, 12, 0, 0, 0, time.Local)
	m.RecordUsage(providerKey, 33)
	m.Flush()

	for _, expiredDate := range []string{"2026-01-01", "2026-01-30"} {
		if store.Exists(providerStoreKey("usage", providerKey, expiredDate)) {
			t.Fatalf("expired usage bucket %s was not pruned", expiredDate)
		}
	}

	reloaded := newProviderMonitorWithClock(nil, []config.UsenetProvider{provider}, store, now)
	_, usage = reloaded.Stats(provider)
	if got, want := usage.TodayBytes, int64(33); got != want {
		t.Fatalf("today bytes = %d, want %d", got, want)
	}
	if got, want := usage.Period30dBytes, int64(33); got != want {
		t.Fatalf("reset-period bytes = %d, want %d", got, want)
	}
	if usage.WindowStart != "2026-01-31" || usage.WindowEnd != "2026-03-01" {
		t.Fatalf("unexpected window: %+v", usage)
	}
}

func TestProviderUsageUpgradeSeedsPeriodFromExistingData(t *testing.T) {
	store := newMetricsStore(t)
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	providerKey := canonicalProviderKey(provider)
	for date, bytes := range map[string]int64{"2026-08-10": 40, "2026-08-19": 60} {
		data, err := json.Marshal(persistedUsage{ProviderKey: providerKey, Date: date, Bytes: bytes})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(providerStoreKey("usage", providerKey, date), data, nil); err != nil {
			t.Fatal(err)
		}
	}
	clock := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.Local)
	now := func() time.Time { return clock }
	m := newProviderMonitorWithClock(nil, []config.UsenetProvider{provider}, store, now)
	_, usage := m.Stats(provider)
	if usage.WindowStart != "2026-08-10" || usage.WindowEnd != "2026-09-08" || usage.Period30dBytes != 100 {
		t.Fatalf("existing data did not seed the period: %+v", usage)
	}

	reloaded := newProviderMonitorWithClock(nil, []config.UsenetProvider{provider}, store, now)
	_, persisted := reloaded.Stats(provider)
	if persisted.WindowStart != usage.WindowStart || persisted.Period30dBytes != usage.Period30dBytes {
		t.Fatalf("usage period did not persist: first=%+v reloaded=%+v", usage, persisted)
	}
}

func TestProviderHealthPersistsAndUnknownDefaults(t *testing.T) {
	store := newMetricsStore(t)
	provider := config.UsenetProvider{Host: "news.example.com", Port: 119}
	other := config.UsenetProvider{Host: "backup.example.com", Port: 563, SSL: true}
	m := newProviderMonitor(nil, []config.UsenetProvider{provider, other}, store)
	latency := int64(42)
	m.setHealth(canonicalProviderKey(provider), ProviderHealth{
		Status: "healthy", CheckedAt: "2026-08-20T10:00:00-04:00", LatencyMs: &latency,
	})

	reloaded := newProviderMonitor(nil, []config.UsenetProvider{provider, other}, store)
	health, _ := reloaded.Stats(provider)
	if health.Status != "healthy" || health.LatencyMs == nil || *health.LatencyMs != 42 {
		t.Fatalf("unexpected persisted health: %+v", health)
	}
	unknown, usage := reloaded.Stats(other)
	if unknown.Status != "unknown" || unknown.CheckedAt != "" {
		t.Fatalf("unexpected default health: %+v", unknown)
	}
	if usage.TodayBytes != 0 || usage.Period30dBytes != 0 {
		t.Fatalf("unexpected default usage: %+v", usage)
	}
}

func TestProviderSpeedTestPersists(t *testing.T) {
	store := newMetricsStore(t)
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	firstClient := &Client{speedTestResults: xsync.NewMap[string, SpeedTestResult]()}
	first := newProviderMonitor(firstClient, []config.UsenetProvider{provider}, store)
	want := SpeedTestResult{
		Provider: provider.Host, SpeedMBps: 12.5, LatencyMs: 48, BytesRead: 1024,
		TestedAt: time.Date(2026, time.August, 20, 16, 20, 55, 0, time.Local),
	}
	first.setSpeedTest(provider, want)

	secondClient := &Client{speedTestResults: xsync.NewMap[string, SpeedTestResult]()}
	_ = newProviderMonitor(secondClient, []config.UsenetProvider{provider}, store)
	got, ok := secondClient.GetSpeedTestResult(provider.Host)
	if !ok || got.Provider != want.Provider || got.SpeedMBps != want.SpeedMBps || got.LatencyMs != want.LatencyMs || !got.TestedAt.Equal(want.TestedAt) {
		t.Fatalf("speed test did not persist: got=%+v ok=%v want=%+v", got, ok, want)
	}
}

func TestHealthChecksAreSequentialAndDoNotOverlap(t *testing.T) {
	providers := []config.UsenetProvider{
		{Host: "one", Port: 119}, {Host: "two", Port: 119}, {Host: "three", Port: 119},
	}
	m := newProviderMonitor(nil, providers, nil)
	var calls atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	block := make(chan struct{})
	started := make(chan struct{})
	m.checkProvider = func(context.Context, config.UsenetProvider) ProviderHealth {
		calls.Add(1)
		current := active.Add(1)
		for {
			maximum := maxActive.Load()
			if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
				break
			}
		}
		if calls.Load() == 1 {
			close(started)
			<-block
		}
		active.Add(-1)
		return ProviderHealth{Status: "healthy"}
	}

	done := make(chan struct{})
	go func() {
		m.runHealthChecks(context.Background())
		close(done)
	}()
	<-started
	m.runHealthChecks(context.Background()) // Must return without starting another pass.
	if got := calls.Load(); got != 1 {
		t.Fatalf("overlapping pass started, calls=%d", got)
	}
	close(block)
	<-done
	if got := calls.Load(); got != int32(len(providers)) {
		t.Fatalf("calls=%d, want %d", got, len(providers))
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("checks ran concurrently, max active=%d", got)
	}
}

func TestHealthLoopRunsImmediatelyAndPeriodically(t *testing.T) {
	m := newProviderMonitor(nil, []config.UsenetProvider{{Host: "one", Port: 119}}, nil)
	m.healthInterval = 10 * time.Millisecond
	m.flushInterval = time.Hour
	var calls atomic.Int32
	m.checkProvider = func(context.Context, config.UsenetProvider) ProviderHealth {
		calls.Add(1)
		return ProviderHealth{Status: "healthy"}
	}
	m.Start()
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	m.Close()
	if calls.Load() < 2 {
		t.Fatalf("health loop calls=%d, want startup plus periodic run", calls.Load())
	}
}

func TestUsagePeriodicAndShutdownFlush(t *testing.T) {
	store := newMetricsStore(t)
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	providerKey := canonicalProviderKey(provider)
	m := newProviderMonitor(nil, []config.UsenetProvider{provider}, store)
	m.healthInterval = time.Hour
	m.flushInterval = 5 * time.Millisecond
	m.checkProvider = func(context.Context, config.UsenetProvider) ProviderHealth {
		return ProviderHealth{Status: "healthy"}
	}
	m.Start()
	m.RecordUsage(providerKey, 17)

	date := time.Now().In(time.Local).Format("2006-01-02")
	key := providerStoreKey("usage", providerKey, date)
	deadline := time.Now().Add(time.Second)
	for !store.Exists(key) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !store.Exists(key) {
		t.Fatal("periodic flush did not persist usage")
	}

	m.RecordUsage(providerKey, 25)
	m.Close()
	reloaded := newProviderMonitor(nil, []config.UsenetProvider{provider}, store)
	_, usage := reloaded.Stats(provider)
	if got, want := usage.TodayBytes, int64(42); got != want {
		t.Fatalf("shutdown-flushed bytes = %d, want %d", got, want)
	}
}

func TestClientStatsIncludesProviderHealthAndUsage(t *testing.T) {
	clock := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.Local)
	providers := []config.UsenetProvider{
		{Host: "primary.example.com", Port: 563, SSL: true, MaxConnections: 4},
		{Host: "backup.example.com", Port: 119, MaxConnections: 2},
	}
	m := newProviderMonitorWithClock(nil, providers, nil, func() time.Time { return clock })
	m.setHealth(canonicalProviderKey(providers[0]), ProviderHealth{
		Status: "unhealthy", CheckedAt: clock.Format(time.RFC3339), Error: "authentication failed",
	})
	m.RecordUsage(canonicalProviderKey(providers[0]), 1234)
	client := &Client{
		providers:        providers,
		pools:            make(map[string]*ProviderPool),
		speedTestResults: xsync.NewMap[string, SpeedTestResult](),
		providerMonitor:  m,
	}
	for _, provider := range providers {
		client.pools[provider.Host] = &ProviderPool{
			slots: make(chan struct{}, provider.MaxConnections), max: provider.MaxConnections,
		}
	}

	encoded, err := json.Marshal(client.Stats())
	if err != nil {
		t.Fatal(err)
	}
	var stats struct {
		Providers []struct {
			ProviderKey string         `json:"provider_key"`
			Health      ProviderHealth `json:"health"`
			Usage       ProviderUsage  `json:"usage"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(encoded, &stats); err != nil {
		t.Fatal(err)
	}
	if len(stats.Providers) != 2 {
		t.Fatalf("provider count = %d, want 2", len(stats.Providers))
	}
	if stats.Providers[0].Health.Status != "unhealthy" || stats.Providers[0].Usage.TodayBytes != 1234 {
		t.Fatalf("unexpected primary stats: %+v", stats.Providers[0])
	}
	if stats.Providers[1].Health.Status != "unknown" || stats.Providers[1].Usage.TodayBytes != 0 {
		t.Fatalf("unexpected empty-provider stats: %+v", stats.Providers[1])
	}
}

func dateTestConnection(t *testing.T, dateCode int) *Connection {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	reader := bufio.NewReader(clientSide)
	conn := &Connection{
		conn: clientSide, reader: reader, text: textproto.NewReader(reader),
		writer: bufio.NewWriter(clientSide), logger: zerolog.Nop(),
	}
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	go func() {
		defer serverSide.Close()
		serverReader := bufio.NewReader(serverSide)
		_, _ = serverReader.ReadString('\n')
		_, _ = io.WriteString(serverSide, fmt.Sprintf("%d test date response\r\n", dateCode))
	}()
	return conn
}

func TestHealthCheckClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dateCode   int
		openErr    error
		wantStatus string
		wantDetail bool
	}{
		{name: "healthy", dateCode: 111, wantStatus: "healthy"},
		{name: "invalid credentials", openErr: errors.New("auth: password rejected"), wantStatus: "unhealthy"},
		{name: "unsupported DATE", dateCode: 500, wantStatus: "healthy", wantDetail: true},
		{name: "unreachable", openErr: errors.New("dial: connection refused"), wantStatus: "unhealthy"},
		{name: "TLS failure", openErr: errors.New("tls: handshake failure"), wantStatus: "unhealthy"},
		{name: "timeout", openErr: context.DeadlineExceeded, wantStatus: "unhealthy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
			m := newProviderMonitor(nil, []config.UsenetProvider{provider}, nil)
			m.openConnection = func(context.Context, config.UsenetProvider) (*Connection, error) {
				if tc.openErr != nil {
					return nil, tc.openErr
				}
				return dateTestConnection(t, tc.dateCode), nil
			}
			health := m.performHealthCheck(context.Background(), provider)
			if health.Status != tc.wantStatus {
				t.Fatalf("status=%q error=%q detail=%q, want %q", health.Status, health.Error, health.Detail, tc.wantStatus)
			}
			if tc.wantDetail && health.Detail == "" {
				t.Fatal("expected unsupported DATE detail")
			}
			if tc.wantStatus == "healthy" && tc.dateCode == 111 && health.LatencyMs == nil {
				t.Fatal("healthy DATE response did not record latency")
			}
		})
	}
}

func TestSanitizeProviderErrorRedactsCredentials(t *testing.T) {
	provider := config.UsenetProvider{Username: "secret-user", Password: "secret-pass"}
	got := sanitizeProviderError(errors.New("secret-user\nsecret-pass"), provider)
	if strings.Contains(got, provider.Username) || strings.Contains(got, provider.Password) || strings.Contains(got, "\n") {
		t.Fatalf("error was not sanitized: %q", got)
	}
}

func yencTestBody(data []byte) string {
	var encoded strings.Builder
	encoded.WriteString(fmt.Sprintf("=ybegin line=128 size=%d name=test.bin\r\n", len(data)))
	for _, value := range data {
		b := value + 42
		if b == 0 || b == '\n' || b == '\r' || b == '=' || b == '\t' || b == ' ' || b == '.' {
			encoded.WriteByte('=')
			encoded.WriteByte(b + 64)
		} else {
			encoded.WriteByte(b)
		}
	}
	encoded.WriteString(fmt.Sprintf("\r\n=yend size=%d\r\n.\r\n", len(data)))
	return encoded.String()
}

func bodyTestConnection(t *testing.T, payload []byte, usage *atomic.Int64) *Connection {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	reader := bufio.NewReader(clientSide)
	conn := &Connection{
		conn: clientSide, reader: reader, text: textproto.NewReader(reader),
		writer: bufio.NewWriter(clientSide), providerKey: "provider",
		recordUsage: func(_ string, bytes int64) { usage.Add(bytes) },
		logger:      zerolog.Nop(),
	}
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	go func() {
		commandReader := bufio.NewReader(serverSide)
		_, _ = commandReader.ReadString('\n')
		_, _ = io.WriteString(serverSide, "222 0 <test> body follows\r\n"+yencTestBody(payload))
	}()
	return conn
}

func missingBodyTestConnection(t *testing.T) *Connection {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	reader := bufio.NewReader(clientSide)
	conn := &Connection{
		conn: clientSide, reader: reader, text: textproto.NewReader(reader),
		writer: bufio.NewWriter(clientSide), logger: zerolog.Nop(),
	}
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})
	go func() {
		defer serverSide.Close()
		commandReader := bufio.NewReader(serverSide)
		_, _ = commandReader.ReadString('\n')
		_, _ = io.WriteString(serverSide, "430 no such article\r\n")
	}()
	return conn
}

func TestMeasureProviderSpeedRequiresBodyAndFallsBackToAnotherCandidate(t *testing.T) {
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	payload := bytes.Repeat([]byte("speed-test-body"), 64)
	var usage atomic.Int64
	var opens atomic.Int32
	opener := func(context.Context, config.UsenetProvider) (*Connection, error) {
		if opens.Add(1) == 1 {
			return missingBodyTestConnection(t), nil
		}
		return bodyTestConnection(t, payload, &usage), nil
	}
	result := measureProviderSpeed(
		context.Background(), provider, []string{"missing", "available"}, time.Now(), 51, opener,
	)
	if result.Error != "" || result.SpeedMBps <= 0 || result.BytesRead <= 0 || result.LatencyMs != 51 {
		t.Fatalf("unexpected speed result: %+v", result)
	}
	if opens.Load() != 2 {
		t.Fatalf("connection attempts = %d, want 2", opens.Load())
	}

	withoutBody := measureProviderSpeed(context.Background(), provider, nil, time.Now(), 51, opener)
	if withoutBody.Error == "" || withoutBody.SpeedMBps != 0 {
		t.Fatalf("latency-only test incorrectly succeeded: %+v", withoutBody)
	}
}

func TestScheduledHealthRefreshAlsoStoresSpeed(t *testing.T) {
	provider := config.UsenetProvider{Host: "news.example.com", Port: 563, SSL: true}
	client := &Client{speedTestResults: xsync.NewMap[string, SpeedTestResult]()}
	m := newProviderMonitor(client, []config.UsenetProvider{provider}, nil)
	m.testSegments = func() []string { return []string{"available"} }
	var usage atomic.Int64
	var opens atomic.Int32
	m.openConnection = func(context.Context, config.UsenetProvider) (*Connection, error) {
		if opens.Add(1) == 1 {
			return dateTestConnection(t, 111), nil
		}
		return bodyTestConnection(t, bytes.Repeat([]byte("scheduled-speed"), 64), &usage), nil
	}

	health := m.performHealthCheck(context.Background(), provider)
	result, ok := client.GetSpeedTestResult(provider.Host)
	if health.Status != "healthy" || !ok || result.SpeedMBps <= 0 || result.LatencyMs < 0 {
		t.Fatalf("health=%+v speed=%+v present=%v", health, result, ok)
	}
}

type failAfterWriter struct {
	remaining int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("writer failed")
	}
	if len(p) > w.remaining {
		n := w.remaining
		w.remaining = 0
		return n, errors.New("writer failed")
	}
	w.remaining -= len(p)
	return len(p), nil
}

func TestStreamBodyCountsSuccessfulAndPartialDecodedBytes(t *testing.T) {
	payload := bytes.Repeat([]byte("decoded-body-"), 32)
	var usage atomic.Int64
	conn := bodyTestConnection(t, payload, &usage)
	var output bytes.Buffer
	n, err := conn.StreamBody("test", &output)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) || usage.Load() != int64(len(payload)) || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("success n=%d usage=%d output=%d want=%d", n, usage.Load(), output.Len(), len(payload))
	}

	usage.Store(0)
	conn = bodyTestConnection(t, payload, &usage)
	writer := &failAfterWriter{remaining: 17}
	n, err = conn.StreamBody("test", writer)
	if err == nil {
		t.Fatal("expected partial writer error")
	}
	if n != 17 || usage.Load() != 17 {
		t.Fatalf("partial n=%d usage=%d, want 17", n, usage.Load())
	}
}

func TestDecodedAndMetadataBodyPathsCountButSpeedTestRawBodyDoesNot(t *testing.T) {
	payload := bytes.Repeat([]byte("body"), 64)
	var usage atomic.Int64
	conn := bodyTestConnection(t, payload, &usage)
	decoded, err := conn.GetDecodedBody("test")
	if err != nil || !bytes.Equal(decoded, payload) || usage.Load() != int64(len(payload)) {
		t.Fatalf("decoded path err=%v bytes=%d usage=%d", err, len(decoded), usage.Load())
	}

	usage.Store(0)
	conn = bodyTestConnection(t, payload, &usage)
	if _, err := conn.GetHeaderPrefix("test", 8); err != nil {
		t.Fatal(err)
	}
	if usage.Load() != int64(len(payload)) {
		t.Fatalf("metadata BODY usage=%d, want %d", usage.Load(), len(payload))
	}

	usage.Store(0)
	conn = bodyTestConnection(t, payload, &usage)
	if _, err := conn.GetBody("test"); err != nil {
		t.Fatal(err)
	}
	if usage.Load() != 0 {
		t.Fatalf("raw speed-test BODY was counted: %d", usage.Load())
	}
}

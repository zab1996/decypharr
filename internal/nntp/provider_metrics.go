package nntp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
)

const (
	providerHealthInterval  = time.Hour
	providerUsageFlushEvery = 30 * time.Second
	providerUsageDays       = 30
)

type ProviderHealth struct {
	Status    string `json:"status"`
	CheckedAt string `json:"checked_at,omitempty"`
	LatencyMs *int64 `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type ProviderUsage struct {
	TodayBytes     int64  `json:"today_bytes"`
	Period30dBytes int64  `json:"period_30d_bytes"`
	WindowStart    string `json:"window_start"`
	WindowEnd      string `json:"window_end"`
}

type persistedHealth struct {
	ProviderKey string         `json:"provider_key"`
	Health      ProviderHealth `json:"health"`
}

type persistedUsage struct {
	ProviderKey string `json:"provider_key"`
	Date        string `json:"date"`
	Bytes       int64  `json:"bytes"`
}

type persistedUsagePeriod struct {
	Start string `json:"start"`
}

type persistedSpeedTest struct {
	ProviderKey string          `json:"provider_key"`
	Result      SpeedTestResult `json:"result"`
}

type providerMonitor struct {
	client    *Client
	providers []config.UsenetProvider
	store     *hybrid.Store

	mu          sync.RWMutex
	health      map[string]ProviderHealth
	usage       map[string]map[string]int64
	dirty       map[string]map[string]struct{}
	periodStart string

	now            func() time.Time
	healthInterval time.Duration
	flushInterval  time.Duration
	checkProvider  func(context.Context, config.UsenetProvider) ProviderHealth
	openConnection func(context.Context, config.UsenetProvider) (*Connection, error)

	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	checksActive atomic.Bool
	closed       atomic.Bool
}

func canonicalProviderKey(provider config.UsenetProvider) string {
	host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(provider.Host), "."))
	endpoint := net.JoinHostPort(host, fmt.Sprintf("%d", provider.Port))
	mode := "plain"
	if provider.SSL {
		mode = "tls"
	}
	return endpoint + ":" + mode
}

func providerStoreKey(prefix, providerKey string, suffix ...string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(providerKey))
	parts := []string{prefix, encoded}
	parts = append(parts, suffix...)
	return strings.Join(parts, "/")
}

func newProviderMonitor(client *Client, providers []config.UsenetProvider, store *hybrid.Store) *providerMonitor {
	return newProviderMonitorWithClock(client, providers, store, utils.Now)
}

func newProviderMonitorWithClock(client *Client, providers []config.UsenetProvider, store *hybrid.Store, now func() time.Time) *providerMonitor {
	m := &providerMonitor{
		client:         client,
		providers:      append([]config.UsenetProvider(nil), providers...),
		store:          store,
		health:         make(map[string]ProviderHealth),
		usage:          make(map[string]map[string]int64),
		dirty:          make(map[string]map[string]struct{}),
		now:            now,
		healthInterval: providerHealthInterval,
		flushInterval:  providerUsageFlushEvery,
	}
	m.checkProvider = m.performHealthCheck
	if client != nil {
		m.openConnection = client.createConnection
	}
	m.load()
	return m
}

func (m *providerMonitor) load() {
	if m.store == nil {
		return
	}
	if err := m.store.ForEach(func(key string, value []byte) error {
		switch {
		case key == "usage_period/start":
			var period persistedUsagePeriod
			if err := json.Unmarshal(value, &period); err == nil {
				m.periodStart = period.Start
			}
		case strings.HasPrefix(key, "health/"):
			var record persistedHealth
			if err := json.Unmarshal(value, &record); err == nil && record.ProviderKey != "" {
				m.health[record.ProviderKey] = record.Health
			}
		case strings.HasPrefix(key, "usage/"):
			var record persistedUsage
			if err := json.Unmarshal(value, &record); err == nil && record.ProviderKey != "" && record.Date != "" {
				if m.usage[record.ProviderKey] == nil {
					m.usage[record.ProviderKey] = make(map[string]int64)
				}
				m.usage[record.ProviderKey][record.Date] = record.Bytes
			}
		case strings.HasPrefix(key, "speed/"):
			var record persistedSpeedTest
			if err := json.Unmarshal(value, &record); err == nil && record.ProviderKey != "" && record.Result.Provider != "" && m.client != nil {
				m.client.speedTestResults.Store(record.Result.Provider, record.Result)
			}
		}
		return nil
	}); err != nil && m.client != nil {
		m.client.logger.Warn().Err(err).Msg("Failed to load provider metrics")
	}
	m.ensureUsagePeriod()
	m.prune(true)
}

func (m *providerMonitor) setSpeedTest(provider config.UsenetProvider, result SpeedTestResult) {
	if m.store == nil {
		return
	}
	providerKey := canonicalProviderKey(provider)
	data, err := json.Marshal(persistedSpeedTest{ProviderKey: providerKey, Result: result})
	if err == nil {
		err = m.store.Put(providerStoreKey("speed", providerKey), data, &hybrid.EntryMeta{
			Category: "provider_speed_test", Provider: providerKey,
		})
	}
	if err != nil && m.client != nil {
		m.client.logger.Warn().Err(err).Str("provider", providerKey).Msg("Failed to persist provider speed test")
	}
}

func (m *providerMonitor) ensureUsagePeriod() {
	today := m.now().In(time.Local)
	todayText := today.Format("2006-01-02")

	m.mu.Lock()
	startText := m.periodStart
	if startText == "" {
		// Preserve already-collected usage when upgrading from the original
		// rolling-window implementation by anchoring at its earliest date.
		for _, dates := range m.usage {
			for date := range dates {
				if startText == "" || date < startText {
					startText = date
				}
			}
		}
		if startText == "" {
			startText = todayText
		}
	}
	start, err := time.ParseInLocation("2006-01-02", startText, time.Local)
	if err != nil || today.Before(start) {
		start = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.Local)
	}
	for !today.Before(start.AddDate(0, 0, providerUsageDays)) {
		start = start.AddDate(0, 0, providerUsageDays)
	}
	newStart := start.Format("2006-01-02")
	changed := newStart != m.periodStart
	m.periodStart = newStart
	m.mu.Unlock()

	if changed && m.store != nil {
		if data, marshalErr := json.Marshal(persistedUsagePeriod{Start: newStart}); marshalErr == nil {
			if putErr := m.store.Put("usage_period/start", data, &hybrid.EntryMeta{Category: "provider_usage_period"}); putErr != nil && m.client != nil {
				m.client.logger.Warn().Err(putErr).Msg("Failed to persist provider usage period")
			}
		}
	}
}

func (m *providerMonitor) usageWindow() (string, string) {
	m.ensureUsagePeriod()
	m.mu.RLock()
	startText := m.periodStart
	m.mu.RUnlock()
	start, err := time.ParseInLocation("2006-01-02", startText, time.Local)
	if err != nil {
		start = m.now().In(time.Local)
		startText = start.Format("2006-01-02")
	}
	return startText, start.AddDate(0, 0, providerUsageDays-1).Format("2006-01-02")
}

func (m *providerMonitor) Start() {
	if m.cancel != nil {
		return
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.wg.Add(2)
	go m.healthLoop()
	go m.flushLoop()
}

func (m *providerMonitor) healthLoop() {
	defer m.wg.Done()
	m.runHealthChecks(m.ctx)
	ticker := time.NewTicker(m.healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.runHealthChecks(m.ctx)
		}
	}
}

func (m *providerMonitor) flushLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.Flush()
		}
	}
}

func (m *providerMonitor) runHealthChecks(ctx context.Context) {
	if !m.checksActive.CompareAndSwap(false, true) {
		return
	}
	defer m.checksActive.Store(false)
	for _, provider := range m.providers {
		if ctx.Err() != nil {
			return
		}
		m.setHealth(canonicalProviderKey(provider), m.checkProvider(ctx, provider))
	}
}

func (m *providerMonitor) performHealthCheck(ctx context.Context, provider config.UsenetProvider) ProviderHealth {
	checkedAt := m.now()
	checkedAtText := checkedAt.Format(time.RFC3339)
	if m.openConnection == nil {
		return ProviderHealth{Status: "unknown", CheckedAt: checkedAtText, Error: "health checker is unavailable"}
	}
	conn, err := m.openConnection(ctx, provider)
	if err != nil {
		return ProviderHealth{
			Status: "unhealthy", CheckedAt: checkedAtText,
			Error: sanitizeProviderError(err, provider),
		}
	}
	defer func() { _ = conn.Close() }()

	pingStarted := time.Now()
	if err := conn.ping(); err != nil {
		message := sanitizeProviderError(err, provider)
		if strings.Contains(strings.ToLower(message), "unexpected date response") {
			return ProviderHealth{
				Status: "healthy", CheckedAt: checkedAtText,
				Detail: "Authenticated successfully; DATE is not supported by this server",
			}
		}
		return ProviderHealth{Status: "unknown", CheckedAt: checkedAtText, Error: message}
	}
	latency := time.Since(pingStarted).Milliseconds()
	return ProviderHealth{Status: "healthy", CheckedAt: checkedAtText, LatencyMs: &latency}
}

func sanitizeProviderError(err error, provider config.UsenetProvider) string {
	if err == nil {
		return ""
	}
	message := strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\r", " "), "\n", " ")
	for _, secret := range []string{provider.Username, provider.Password} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	if len(message) > 240 {
		message = message[:240]
	}
	return message
}

func (m *providerMonitor) setHealth(providerKey string, health ProviderHealth) {
	m.mu.Lock()
	m.health[providerKey] = health
	m.mu.Unlock()
	if m.store == nil {
		return
	}
	data, err := json.Marshal(persistedHealth{ProviderKey: providerKey, Health: health})
	if err == nil {
		err = m.store.Put(providerStoreKey("health", providerKey), data, &hybrid.EntryMeta{
			Category: "provider_health", Provider: providerKey, Status: health.Status,
		})
	}
	if err != nil && m.client != nil {
		m.client.logger.Warn().Err(err).Str("provider", providerKey).Msg("Failed to persist provider health")
	}
}

func (m *providerMonitor) RecordUsage(providerKey string, bytes int64) {
	if bytes <= 0 || providerKey == "" {
		return
	}
	m.ensureUsagePeriod()
	date := m.now().In(time.Local).Format("2006-01-02")
	m.mu.Lock()
	if m.usage[providerKey] == nil {
		m.usage[providerKey] = make(map[string]int64)
	}
	if m.dirty[providerKey] == nil {
		m.dirty[providerKey] = make(map[string]struct{})
	}
	m.usage[providerKey][date] += bytes
	m.dirty[providerKey][date] = struct{}{}
	m.mu.Unlock()
}

func (m *providerMonitor) Flush() {
	m.prune(true)
	if m.store == nil {
		return
	}

	type record struct {
		providerKey string
		date        string
		bytes       int64
	}
	var records []record
	m.mu.RLock()
	for providerKey, dates := range m.dirty {
		for date := range dates {
			records = append(records, record{providerKey: providerKey, date: date, bytes: m.usage[providerKey][date]})
		}
	}
	m.mu.RUnlock()

	for _, record := range records {
		data, err := json.Marshal(persistedUsage{
			ProviderKey: record.providerKey, Date: record.date, Bytes: record.bytes,
		})
		if err == nil {
			err = m.store.Put(
				providerStoreKey("usage", record.providerKey, record.date), data,
				&hybrid.EntryMeta{Category: "provider_usage", Provider: record.providerKey, AddedOn: m.now().Unix()},
			)
		}
		if err != nil {
			if m.client != nil {
				m.client.logger.Warn().Err(err).Str("provider", record.providerKey).Str("date", record.date).Msg("Failed to persist provider usage")
			}
			continue
		}
		m.mu.Lock()
		if dates := m.dirty[record.providerKey]; dates != nil && m.usage[record.providerKey][record.date] == record.bytes {
			delete(dates, record.date)
			if len(dates) == 0 {
				delete(m.dirty, record.providerKey)
			}
		}
		m.mu.Unlock()
	}
}

func (m *providerMonitor) prune(deletePersisted bool) {
	windowStart, windowEnd := m.usageWindow()
	type staleRecord struct{ providerKey, date string }
	var stale []staleRecord
	m.mu.Lock()
	for providerKey, dates := range m.usage {
		for date := range dates {
			if date < windowStart || date > windowEnd {
				delete(dates, date)
				if dirtyDates := m.dirty[providerKey]; dirtyDates != nil {
					delete(dirtyDates, date)
				}
				stale = append(stale, staleRecord{providerKey, date})
			}
		}
		if len(dates) == 0 {
			delete(m.usage, providerKey)
		}
	}
	m.mu.Unlock()
	if !deletePersisted || m.store == nil {
		return
	}
	for _, record := range stale {
		key := providerStoreKey("usage", record.providerKey, record.date)
		if m.store.Exists(key) {
			_ = m.store.Delete(key)
		}
	}
}

func (m *providerMonitor) Stats(provider config.UsenetProvider) (ProviderHealth, ProviderUsage) {
	providerKey := canonicalProviderKey(provider)
	now := m.now().In(time.Local)
	today := now.Format("2006-01-02")
	windowStart, windowEnd := m.usageWindow()
	health := ProviderHealth{Status: "unknown"}
	usage := ProviderUsage{WindowStart: windowStart, WindowEnd: windowEnd}
	m.mu.RLock()
	if current, ok := m.health[providerKey]; ok {
		health = current
	}
	for date, bytes := range m.usage[providerKey] {
		if date >= windowStart && date <= windowEnd {
			usage.Period30dBytes += bytes
			if date == today {
				usage.TodayBytes = bytes
			}
		}
	}
	m.mu.RUnlock()
	return health, usage
}

func (m *providerMonitor) Close() {
	if m.closed.Swap(true) {
		m.Flush()
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	m.Flush()
}

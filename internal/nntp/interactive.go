package nntp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// interactiveReserveConfig holds pool-reserve tuning applied when interactive
// mode is active.
type interactiveReserveConfig struct {
	enabled bool
	percent int
	min     int
	max     int
}

// interactiveState tracks dynamic pool reservation for playback protection.
type interactiveState struct {
	mu sync.RWMutex

	cfg interactiveReserveConfig

	active bool

	reserved         int
	backgroundBudget int

	backgroundInUse int32

	startedAt time.Time

	lastEntry  string
	lastFile   string
	lastClient string

	throttleLoggedAt time.Time
}

func newInteractiveState(cfg *config.Config) *interactiveState {
	s := &interactiveState{}
	if cfg != nil {
		s.applyConfig(cfg)
	}
	return s
}

func (s *interactiveState) applyConfig(cfg *config.Config) {
	if s == nil || cfg == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = interactiveReserveConfig{
		enabled: cfg.Usenet.InteractivePoolReserveEnabled,
		percent: cfg.Usenet.InteractivePoolReservePercent,
		min:     cfg.Usenet.InteractivePoolReserveMin,
		max:     cfg.Usenet.InteractivePoolReserveMax,
	}
	if !s.cfg.enabled {
		s.active = false
		s.reserved = 0
		s.backgroundBudget = 0
		atomic.StoreInt32(&s.backgroundInUse, 0)
	}
}

func (s *interactiveState) reserveEnabled() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.enabled
}

func (s *interactiveState) setActive(active bool, totalConnections int, meta reserveMeta) (entered, exited bool, snapshot InteractiveSnapshot) {
	if s == nil {
		return false, false, InteractiveSnapshot{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.cfg.enabled {
		if s.active {
			s.active = false
			s.reserved = 0
			s.backgroundBudget = 0
			atomic.StoreInt32(&s.backgroundInUse, 0)
			exited = true
		}
		return false, exited, s.snapshotLocked()
	}

	wasActive := s.active
	if active {
		s.reserved = config.ComputeInteractiveReserve(totalConnections, s.cfg.percent, s.cfg.min, s.cfg.max)
		s.backgroundBudget = totalConnections - s.reserved
		if s.backgroundBudget < 0 {
			s.backgroundBudget = 0
		}
		if meta.Entry != "" {
			s.lastEntry = meta.Entry
		}
		if meta.File != "" {
			s.lastFile = meta.File
		}
		if meta.Client != "" {
			s.lastClient = meta.Client
		}
		if !wasActive {
			s.startedAt = time.Now()
			entered = true
		}
		s.active = true
	} else if wasActive {
		s.active = false
		s.reserved = 0
		s.backgroundBudget = 0
		atomic.StoreInt32(&s.backgroundInUse, 0)
		exited = true
	}

	return entered, exited, s.snapshotLocked()
}

func (s *interactiveState) snapshotLocked() InteractiveSnapshot {
	return InteractiveSnapshot{
		Enabled:          s.cfg.enabled,
		Active:           s.active,
		Reserved:         s.reserved,
		BackgroundBudget: s.backgroundBudget,
		BackgroundInUse:  int(atomic.LoadInt32(&s.backgroundInUse)),
		StartedAt:        s.startedAt,
		LastEntry:        s.lastEntry,
		LastFile:         s.lastFile,
		LastClient:       s.lastClient,
		ReservePercent:   s.cfg.percent,
		ReserveMin:       s.cfg.min,
		ReserveMax:       s.cfg.max,
	}
}

func (s *interactiveState) snapshot() InteractiveSnapshot {
	if s == nil {
		return InteractiveSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *interactiveState) acquireBackgroundSlot() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	enabled := s.cfg.enabled && s.active
	budget := s.backgroundBudget
	s.mu.RUnlock()
	if !enabled {
		return true
	}
	for {
		cur := atomic.LoadInt32(&s.backgroundInUse)
		if int(cur) >= budget {
			return false
		}
		if atomic.CompareAndSwapInt32(&s.backgroundInUse, cur, cur+1) {
			return true
		}
	}
}

func (s *interactiveState) releaseBackgroundSlot() {
	if s == nil {
		return
	}
	for {
		cur := atomic.LoadInt32(&s.backgroundInUse)
		if cur <= 0 {
			return
		}
		if atomic.CompareAndSwapInt32(&s.backgroundInUse, cur, cur-1) {
			return
		}
	}
}

type ReserveMeta struct {
	Entry  string
	File   string
	Client string
}

type reserveMeta struct {
	Entry  string
	File   string
	Client string
}

// InteractiveSnapshot exposes reserve state for stats/logging.
type InteractiveSnapshot struct {
	Enabled          bool      `json:"enabled"`
	Active           bool      `json:"active"`
	Reserved         int       `json:"reserved"`
	BackgroundBudget int       `json:"background_budget"`
	BackgroundInUse  int       `json:"background_in_use"`
	StartedAt        time.Time `json:"started_at,omitempty"`
	LastEntry        string    `json:"last_entry,omitempty"`
	LastFile         string    `json:"last_file,omitempty"`
	LastClient       string    `json:"last_client,omitempty"`
	ReservePercent   int       `json:"reserve_percent,omitempty"`
	ReserveMin       int       `json:"reserve_min,omitempty"`
	ReserveMax       int       `json:"reserve_max,omitempty"`
}

func (s *interactiveState) markThrottleLogged(now time.Time) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !now.After(s.throttleLoggedAt.Add(time.Minute)) {
		return false
	}
	s.throttleLoggedAt = now
	return true
}

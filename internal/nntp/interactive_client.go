package nntp

import (
	"context"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func (c *Client) logInteractiveConfig(cfg *config.Config) {
	if c == nil || c.interactive == nil || cfg == nil {
		return
	}
	total := c.TotalConnections()
	reserve := config.ComputeInteractiveReserve(total, cfg.Usenet.InteractivePoolReservePercent, cfg.Usenet.InteractivePoolReserveMin, cfg.Usenet.InteractivePoolReserveMax)
	if cfg.Usenet.InteractivePoolReserveEnabled {
		c.logger.Info().
			Bool("enabled", true).
			Int("reserve_percent", cfg.Usenet.InteractivePoolReservePercent).
			Int("reserve_min", cfg.Usenet.InteractivePoolReserveMin).
			Int("reserve_max", cfg.Usenet.InteractivePoolReserveMax).
			Int("total_connections", total).
			Int("computed_reserve", reserve).
			Msg("interactive pool reserve configured")
	} else {
		c.logger.Info().Bool("enabled", false).Msg("interactive pool reserve configured")
	}
}

// ConfigureInteractiveReserve reloads reserve settings from config.
func (c *Client) ConfigureInteractiveReserve(cfg *config.Config) {
	if c == nil || c.interactive == nil {
		return
	}
	c.interactive.applyConfig(cfg)
	c.logInteractiveConfig(cfg)
}

// SetInteractiveReserveActive toggles interactive reserve mode.
func (c *Client) SetInteractiveReserveActive(active bool, meta ReserveMeta, bytesInWindow int64, detectWindow time.Duration) {
	if c == nil || c.interactive == nil {
		return
	}
	total := c.TotalConnections()
	entered, exited, snap := c.interactive.setActive(active, total, reserveMeta(meta))
	if entered {
		if snap.BackgroundBudget <= 0 && snap.Reserved >= total && total > 0 {
			c.logger.Warn().
				Int("total_connections", total).
				Int("reserved", snap.Reserved).
				Msg("interactive pool reserve misconfigured: reserve >= total_connections, clamping background budget to 0")
		}
		c.logger.Info().
			Str("entry", meta.Entry).
			Str("file", meta.File).
			Str("client", meta.Client).
			Int64("bytes_in_window", bytesInWindow).
			Dur("window", detectWindow).
			Int("total_connections", total).
			Int("reserved", snap.Reserved).
			Int("background_budget", snap.BackgroundBudget).
			Msg("interactive pool reserve started")
		if c.repairPool != nil {
			c.repairPool.setInteractiveCap(c.effectiveRepairWorkers(snap))
		}
	}
	if exited {
		idleFor := time.Duration(0)
		if !snap.StartedAt.IsZero() {
			idleFor = time.Since(snap.StartedAt)
		}
		c.logger.Info().
			Str("reason", "idle_timeout").
			Dur("idle_for", idleFor).
			Str("last_entry", snap.LastEntry).
			Str("last_file", snap.LastFile).
			Str("last_client", snap.LastClient).
			Msg("interactive pool reserve ended")
		if c.repairPool != nil {
			c.repairPool.setInteractiveCap(0)
		}
	}
}

// InteractiveSnapshot returns current reserve state.
func (c *Client) InteractiveSnapshot() InteractiveSnapshot {
	if c == nil || c.interactive == nil {
		return InteractiveSnapshot{}
	}
	return c.interactive.snapshot()
}

func (c *Client) interactiveReserveActive() bool {
	if c == nil || c.interactive == nil {
		return false
	}
	snap := c.interactive.snapshot()
	return snap.Enabled && snap.Active
}

func (c *Client) waitBackgroundBudget(ctx context.Context) error {
	if c == nil || c.interactive == nil {
		return nil
	}
	if WorkClassFromContext(ctx) != WorkClassBackground {
		return nil
	}
	if !c.interactiveReserveActive() {
		return nil
	}
	for {
		if c.interactive.acquireBackgroundSlot() {
			return nil
		}
		if c.interactive.markThrottleLogged(time.Now()) {
			snap := c.interactive.snapshot()
			c.logger.Debug().
				Str("class", "background").
				Int("background_in_use", snap.BackgroundInUse).
				Int("background_budget", snap.BackgroundBudget).
				Bool("reserve_active", snap.Active).
				Msg("interactive pool reserve throttling background work")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c *Client) releaseConnectionBudget(conn *Connection) {
	if c == nil || conn == nil || c.interactive == nil {
		return
	}
	if conn.backgroundBudgetHeld.Swap(false) {
		c.interactive.releaseBackgroundSlot()
	}
}

func (c *Client) markConnectionBudget(conn *Connection, ctx context.Context) {
	if conn == nil {
		return
	}
	if WorkClassFromContext(ctx) == WorkClassBackground && c.interactiveReserveActive() {
		conn.backgroundBudgetHeld.Store(true)
	}
}

func (c *Client) releaseBackgroundBudgetReservation(ctx context.Context) {
	if c == nil || c.interactive == nil {
		return
	}
	if WorkClassFromContext(ctx) == WorkClassBackground {
		c.interactive.releaseBackgroundSlot()
	}
}

func (c *Client) checkoutFromPool(ctx context.Context, pp *ProviderPool, provider config.UsenetProvider) (*Connection, config.UsenetProvider, error) {
	if err := c.waitBackgroundBudget(ctx); err != nil {
		return nil, provider, err
	}

	select {
	case pp.slots <- struct{}{}:
		conn, err := c.getOrCreateFromPool(ctx, pp, provider)
		if err != nil {
			<-pp.slots
			c.releaseBackgroundBudgetReservation(ctx)
			return nil, provider, err
		}
		c.markConnectionBudget(conn, ctx)
		return conn, provider, nil
	case <-ctx.Done():
		c.releaseBackgroundBudgetReservation(ctx)
		return nil, provider, ctx.Err()
	}
}

func (c *Client) effectiveRepairWorkers(snap InteractiveSnapshot) int {
	if !snap.Active || snap.BackgroundBudget <= 0 {
		return 0
	}
	cap := snap.BackgroundBudget / 2
	if cap < repairPoolMinWorkers {
		cap = repairPoolMinWorkers
	}
	return cap
}

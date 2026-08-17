package config

import "testing"

func TestRepairConfigMediaProbeEnabledDefaults(t *testing.T) {
	var r RepairConfig // MediaProbeUsenet/MediaProbeDebrid unset, as on any config saved before this toggle existed
	if !r.MediaProbeEnabled(true) {
		t.Error("usenet media probe must default to enabled to preserve pre-toggle behavior")
	}
	if r.MediaProbeEnabled(false) {
		t.Error("debrid media probe must default to disabled — it's new, opt-in behavior")
	}
}

func TestRepairConfigMediaProbeEnabledExplicit(t *testing.T) {
	r := RepairConfig{
		MediaProbeUsenet: boolPtr(false),
		MediaProbeDebrid: boolPtr(true),
	}
	if r.MediaProbeEnabled(true) {
		t.Error("explicit false for usenet must be honored")
	}
	if !r.MediaProbeEnabled(false) {
		t.Error("explicit true for debrid must be honored")
	}
}

func TestRepairConfigIsZeroWithMediaProbeFields(t *testing.T) {
	var r RepairConfig
	if !r.IsZero() {
		t.Error("zero-value RepairConfig with unset media probe fields should be IsZero")
	}
	r.MediaProbeUsenet = boolPtr(false)
	if r.IsZero() {
		t.Error("an explicitly-set MediaProbeUsenet must make IsZero false")
	}
}

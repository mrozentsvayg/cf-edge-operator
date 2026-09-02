package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSetBuildInfo verifies the build-info gauge is published as a constant 1,
// labeled by the version and commit passed at startup.
func TestSetBuildInfo(t *testing.T) {
	SetBuildInfo("v1.2.3", "abc1234")
	if got := testutil.ToFloat64(buildInfoGauge.WithLabelValues("v1.2.3", "abc1234")); got != 1 {
		t.Fatalf("cf_edge_operator_build_info{version=v1.2.3,commit=abc1234} = %v, want 1", got)
	}
}

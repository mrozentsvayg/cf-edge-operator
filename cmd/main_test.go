/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/mrozentsvayg/cf-edge-operator/internal/controller"
)

// Short aliases for the policy enum constants keep the test tables readable while
// staying coupled to the controller package's source of truth.
var (
	mgmtManage  = controller.ManagementPolicyManage
	mgmtCreate  = controller.ManagementPolicyCreate
	mgmtObserve = controller.ManagementPolicyObserve
	delAlways   = controller.DeletePolicyAlways
	delOwnOnly  = controller.DeletePolicyOwnOnly
	delNever    = controller.DeletePolicyNever
)

func TestResolvePolicy(t *testing.T) {
	tests := []struct {
		name       string
		perFeature string
		global     string
		want       string
	}{
		{"per-feature set overrides global", mgmtCreate, mgmtManage, mgmtCreate},
		{"per-feature empty inherits global (manage)", "", mgmtManage, mgmtManage},
		{"per-feature empty inherits global (observe)", "", mgmtObserve, mgmtObserve},
		{"per-feature set wins over differing global", mgmtManage, mgmtCreate, mgmtManage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolvePolicy(tt.perFeature, tt.global); got != tt.want {
				t.Errorf("resolvePolicy(%q, %q) = %q, want %q", tt.perFeature, tt.global, got, tt.want)
			}
		})
	}
}

func TestIsValidManagementPolicy(t *testing.T) {
	for _, v := range []string{mgmtManage, mgmtCreate, mgmtObserve} {
		if !isValidManagementPolicy(v) {
			t.Errorf("isValidManagementPolicy(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"", "bogus", "delete", "Manage", "own-only"} {
		if isValidManagementPolicy(v) {
			t.Errorf("isValidManagementPolicy(%q) = true, want false", v)
		}
	}
}

func TestIsValidDeletePolicy(t *testing.T) {
	for _, v := range []string{delAlways, delOwnOnly, delNever} {
		if !isValidDeletePolicy(v) {
			t.Errorf("isValidDeletePolicy(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"", "bogus", "own", "Always", "manage"} {
		if isValidDeletePolicy(v) {
			t.Errorf("isValidDeletePolicy(%q) = true, want false", v)
		}
	}
}

func TestValidatePerFeaturePolicyFlags(t *testing.T) {
	tests := []struct {
		name      string
		flags     perFeaturePolicyFlags
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "all empty -> valid (inherit globals)",
			flags: perFeaturePolicyFlags{},
		},
		{
			name: "all valid -> valid",
			flags: perFeaturePolicyFlags{
				customhostnameManagementPolicy: mgmtCreate,
				customhostnameDeletePolicy:     delOwnOnly,
				loadbalancingManagementPolicy:  mgmtManage,
				loadbalancingDeletePolicy:      delNever,
			},
		},
		{
			name:      "invalid customhostname management names the flag",
			flags:     perFeaturePolicyFlags{customhostnameManagementPolicy: "bogus"},
			wantErr:   true,
			errSubstr: "--customhostname-management-policy",
		},
		{
			name:      "invalid loadbalancing management names the flag",
			flags:     perFeaturePolicyFlags{loadbalancingManagementPolicy: "bogus"},
			wantErr:   true,
			errSubstr: "--loadbalancing-management-policy",
		},
		{
			name:      "invalid customhostname delete names the flag",
			flags:     perFeaturePolicyFlags{customhostnameDeletePolicy: "bogus"},
			wantErr:   true,
			errSubstr: "--customhostname-delete-policy",
		},
		{
			name:      "invalid loadbalancing delete names the flag",
			flags:     perFeaturePolicyFlags{loadbalancingDeletePolicy: "bogus"},
			wantErr:   true,
			errSubstr: "--loadbalancing-delete-policy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePerFeaturePolicyFlags(tt.flags)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not name flag %q", err.Error(), tt.errSubstr)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestRegisterPerFeaturePolicyFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var v perFeaturePolicyFlags
	registerPerFeaturePolicyFlags(fs, &v)

	// All four per-feature flags default to empty (inherit the matching global). The
	// single-owner "manage" default for load balancing lives in the Helm chart values,
	// not the operator binary.
	defaults := map[string]string{
		"loadbalancing-management-policy":  "",
		"customhostname-management-policy": "",
		"customhostname-delete-policy":     "",
		"loadbalancing-delete-policy":      "",
	}
	for name, want := range defaults {
		f := fs.Lookup(name)
		if f == nil {
			t.Fatalf("flag %q not registered", name)
		}
		if f.DefValue != want {
			t.Errorf("flag %q default = %q, want %q", name, f.DefValue, want)
		}
	}

	// Parsing threads each value into its own struct field. Distinct values per flag so
	// a mis-wired StringVar (pointing at the wrong field) is caught.
	if err := fs.Parse([]string{
		"--customhostname-management-policy=observe",
		"--customhostname-delete-policy=never",
		"--loadbalancing-management-policy=create",
		"--loadbalancing-delete-policy=own-only",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if v.customhostnameManagementPolicy != mgmtObserve {
		t.Errorf("customhostnameManagementPolicy = %q, want %q", v.customhostnameManagementPolicy, mgmtObserve)
	}
	if v.customhostnameDeletePolicy != delNever {
		t.Errorf("customhostnameDeletePolicy = %q, want %q", v.customhostnameDeletePolicy, delNever)
	}
	if v.loadbalancingManagementPolicy != mgmtCreate {
		t.Errorf("loadbalancingManagementPolicy = %q, want %q", v.loadbalancingManagementPolicy, mgmtCreate)
	}
	if v.loadbalancingDeletePolicy != delOwnOnly {
		t.Errorf("loadbalancingDeletePolicy = %q, want %q", v.loadbalancingDeletePolicy, delOwnOnly)
	}
}

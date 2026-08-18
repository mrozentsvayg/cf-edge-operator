/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudflare/cloudflare-go/v6"
)

// guardRes is a stand-in resource type for exercising the generic
// cfCreateGuarded helper without pulling in a real CF SDK response type.
type guardRes struct{ id string }

func TestCFCreateGuarded_FirstAttemptSucceeds(t *testing.T) {
	ctx := context.Background()
	findCalls := 0
	res, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 1,
		func() (*guardRes, error) { findCalls++; return nil, nil },
		func() (*guardRes, error) { return &guardRes{id: "new"}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if adopted {
		t.Error("should not be adopted on first-attempt success")
	}
	if attempts != 1 {
		t.Errorf("attempts=%d want 1", attempts)
	}
	if res == nil || res.id != "new" {
		t.Errorf("unexpected result: %+v", res)
	}
	if findCalls != 0 {
		t.Errorf("find must not run when the first create succeeds; calls=%d", findCalls)
	}
}

func TestCFCreateGuarded_AdoptsAfterTimeout(t *testing.T) {
	// The create "times out" (error) but the resource actually landed on CF; the
	// retry's find discovers it and adopts instead of creating a duplicate.
	ctx := context.Background()
	createCalls := 0
	res, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return &guardRes{id: "landed"}, nil },
		func() (*guardRes, error) {
			createCalls++
			return nil, context.DeadlineExceeded
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !adopted {
		t.Error("expected adoption of the timed-out-but-created resource")
	}
	if res == nil || res.id != "landed" {
		t.Errorf("unexpected result: %+v", res)
	}
	if createCalls != 1 {
		t.Errorf("create must run once then adopt on retry; createCalls=%d", createCalls)
	}
}

func TestCFCreateGuarded_RetriesWhenNotCreated(t *testing.T) {
	// First create fails and the resource did NOT land (find returns nil), so the
	// guard retries the create, which then succeeds.
	ctx := context.Background()
	createCalls := 0
	res, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return nil, nil },
		func() (*guardRes, error) {
			createCalls++
			if createCalls == 1 {
				return nil, errors.New("transient")
			}
			return &guardRes{id: "retry-created"}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if adopted {
		t.Error("should not be adopted when find returns nil")
	}
	if res == nil || res.id != "retry-created" {
		t.Errorf("unexpected result: %+v", res)
	}
	if createCalls != 2 {
		t.Errorf("expected 2 create calls; got %d", createCalls)
	}
	if attempts != 2 {
		t.Errorf("attempts=%d want 2", attempts)
	}
}

func TestCFCreateGuarded_AbandonsWhenFindErrors(t *testing.T) {
	// If find can't confirm existence, abandon the retry (returning the original
	// create error) rather than risk a duplicate.
	ctx := context.Background()
	createCalls := 0
	origErr := errors.New("create failed")
	res, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return nil, errors.New("list failed") },
		func() (*guardRes, error) { createCalls++; return nil, origErr },
	)
	if !errors.Is(err, origErr) {
		t.Errorf("want the original create error, got %v", err)
	}
	if adopted || res != nil {
		t.Error("must not adopt or return a resource when find errors")
	}
	if createCalls != 1 {
		t.Errorf("create must run once then abandon; got %d", createCalls)
	}
}

func TestPreInitLoadBalancerMetrics(t *testing.T) {
	// Exercised only via main() behind --enable-loadbalancer, so cover it here.
	// It must not panic (all vectors are registered in init()) and must be safe
	// to call more than once (WithLabelValues is idempotent).
	PreInitLoadBalancerMetrics()
	PreInitLoadBalancerMetrics()
}

func TestCFCreateGuarded_No429Retry(t *testing.T) {
	ctx := context.Background()
	findCalls := 0
	createCalls := 0
	_, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 3,
		func() (*guardRes, error) { findCalls++; return &guardRes{id: "x"}, nil },
		func() (*guardRes, error) { createCalls++; return nil, &cloudflare.Error{StatusCode: 429} },
	)
	if err == nil {
		t.Fatal("expected the 429 error to propagate")
	}
	if adopted {
		t.Error("429 must not adopt")
	}
	if createCalls != 1 {
		t.Errorf("429 must not retry the create; createCalls=%d", createCalls)
	}
	if findCalls != 0 {
		t.Errorf("429 must not trigger find; findCalls=%d", findCalls)
	}
}

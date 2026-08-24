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

package controller

import (
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

// maxEventNote bounds the Event note; events.k8s.io/v1 rejects notes longer than
// 1024 characters, and Cloudflare error messages (which become the note) can be
// long multi-line JSON.
const maxEventNote = 1024

// Every controller emits Kubernetes Events on failure/degradation transitions, so the
// events.k8s.io grant is a shared, unconditional capability rather than feature-specific.
// A core "" events grant does not authorize the events.k8s.io group.
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// recordFailureEvent emits a Warning Event when obj's condType condition is
// transitioning into a failure state -- that is, it is not already False with
// this same reason. This keeps an ongoing failure (retried every reconcile) from
// re-emitting an Event each cycle, mirroring the transition-only side effects the
// controllers use for logging (readyReasonIs / init-once). It is a no-op when rec
// is nil, so a controller without a Recorder simply skips Events.
//
// conds MUST be the persisted conditions (read at the top of Reconcile, before
// the caller mutates them via SetStatusCondition), so the "was it already in this
// state?" check reflects the prior reconcile rather than the update in flight.
func recordFailureEvent(rec events.EventRecorder, obj runtime.Object, conds []metav1.Condition, condType, reason, message string) {
	if rec == nil {
		return
	}
	if cur := apimeta.FindStatusCondition(conds, condType); cur != nil &&
		cur.Status == metav1.ConditionFalse && cur.Reason == reason {
		return // already reported this failure; do not re-emit every reconcile
	}
	note := message
	if len(note) > maxEventNote {
		note = note[:maxEventNote-3] + "..."
	}
	// note is passed as an argument to the "%s" format, so a "%" in a Cloudflare
	// error can never be interpreted as a format directive.
	rec.Eventf(obj, nil, corev1.EventTypeWarning, reason, "Reconcile", "%s", note)
}

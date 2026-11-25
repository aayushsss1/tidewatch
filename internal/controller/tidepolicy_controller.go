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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

const (
	finalizer = "tidewatch.dev/cleanup"
	// resyncInterval re-resolves selectors periodically. It is what picks up
	// changes to workload kinds other than Deployments and StatefulSets, which
	// are not watched.
	resyncInterval = 30 * time.Second
)

// TidePolicyReconciler creates and deletes the TideWorkloads a TidePolicy selects.
type TidePolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// ScaleSupport is passed to the resolver; nil treats every kind as scalable.
	ScaleSupport workload.ScaleSupport
}

// +kubebuilder:rbac:groups=tidewatch.dev,resources=tidepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tidewatch.dev,resources=tidepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tidewatch.dev,resources=tidepolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=tidewatch.dev,resources=tideworkloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tidewatch.dev,resources=tideworkloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments/scale;statefulsets/scale,verbs=get;update
// +kubebuilder:rbac:groups="",resources=pods;events;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch

// Reconcile makes the TideWorkloads of one policy match what its selectors resolve to.
func (r *TidePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var policy v1.TidePolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	ref := v1.PolicyRef{Namespace: policy.Namespace, Name: policy.Name}

	if !policy.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&policy, finalizer) {
			return ctrl.Result{}, nil
		}
		existing, err := r.labeled(ctx, ref)
		if err != nil {
			return ctrl.Result{}, err
		}
		for i := range existing {
			if err := r.Delete(ctx, &existing[i]); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		}
		controllerutil.RemoveFinalizer(&policy, finalizer)
		return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, &policy))
	}
	if controllerutil.AddFinalizer(&policy, finalizer) {
		if err := r.Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
	}

	var all v1.TidePolicyList
	if err := r.List(ctx, &all); err != nil {
		return ctrl.Result{}, err
	}
	live := all.Items[:0]
	for _, p := range all.Items {
		if p.DeletionTimestamp.IsZero() {
			live = append(live, p)
		}
	}
	res, err := (&workload.Resolver{Client: r.Client, ScaleSupport: r.ScaleSupport}).ResolveAll(ctx, live)
	if err != nil {
		return ctrl.Result{}, err
	}

	targets := res.Targets[ref]
	wanted := map[types.NamespacedName]struct{}{}
	var conflicts []string
	for _, t := range targets {
		key := types.NamespacedName{Namespace: t.Namespace, Name: workload.TideWorkloadName(t.Ref)}
		wanted[key] = struct{}{}
		if t.Conflict != "" {
			conflicts = append(conflicts, fmt.Sprintf("%s/%s: %s", t.Namespace, t.Ref.Name, t.Conflict))
		}
		if err := r.ensureTideWorkload(ctx, &policy, t, res); err != nil {
			return ctrl.Result{}, err
		}
	}
	existing, err := r.labeled(ctx, ref)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range existing {
		if _, ok := wanted[client.ObjectKeyFromObject(&existing[i])]; ok {
			continue
		}
		log.Info("Deleting TideWorkload no longer selected", "tideworkload", client.ObjectKeyFromObject(&existing[i]))
		if err := r.Delete(ctx, &existing[i]); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.updatePolicyStatus(ctx, &policy, int32(len(targets)), conflicts, res.Issues[ref]); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: resyncInterval}, nil
}

func policyLabel(ref v1.PolicyRef) string { return ref.Namespace + "." + ref.Name }

func (r *TidePolicyReconciler) labeled(ctx context.Context, ref v1.PolicyRef) ([]v1.TideWorkload, error) {
	var list v1.TideWorkloadList
	if err := r.List(ctx, &list, client.MatchingLabels{v1.LabelPolicy: policyLabel(ref)}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// ensureTideWorkload creates or updates the TideWorkload for one target and
// writes its Conflict and Paused conditions.
func (r *TidePolicyReconciler) ensureTideWorkload(
	ctx context.Context, policy *v1.TidePolicy, t workload.Target, res workload.Resolution,
) error {
	ref := v1.PolicyRef{Namespace: policy.Namespace, Name: policy.Name}
	key := types.NamespacedName{Namespace: t.Namespace, Name: workload.TideWorkloadName(t.Ref)}
	labels := map[string]string{v1.LabelPolicy: policyLabel(ref)}
	owners := []metav1.OwnerReference{{
		APIVersion: t.Ref.APIVersion, Kind: t.Ref.Kind, Name: t.Ref.Name, UID: t.Object.GetUID(),
		Controller: new(false), BlockOwnerDeletion: new(false),
	}}
	spec := v1.TideWorkloadSpec{PolicyRef: ref, TargetRef: t.Ref}

	tw := &v1.TideWorkload{}
	err := r.Get(ctx, key, tw)
	switch {
	case apierrors.IsNotFound(err):
		tw = &v1.TideWorkload{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: labels, OwnerReferences: owners},
			Spec:       spec,
		}
		if err := r.Create(ctx, tw); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if other := tw.Spec.PolicyRef; other != ref && slices.ContainsFunc(res.Targets[other], func(o workload.Target) bool {
			return o.Namespace == t.Namespace && workload.TideWorkloadName(o.Ref) == key.Name
		}) {
			// Another live policy still selects this workload and owns the
			// TideWorkload; leave it be rather than fight over it.
			return nil
		}
		if tw.Labels[v1.LabelPolicy] != labels[v1.LabelPolicy] || !equality.Semantic.DeepEqual(tw.OwnerReferences, owners) ||
			tw.Spec != spec {
			if tw.Labels == nil {
				tw.Labels = map[string]string{}
			}
			tw.Labels[v1.LabelPolicy] = labels[v1.LabelPolicy]
			tw.OwnerReferences, tw.Spec = owners, spec
			if err := r.Update(ctx, tw); err != nil {
				return err
			}
		}
	}

	paused := policy.Annotations[v1.AnnoPaused] == "true" || t.Object.GetAnnotations()[v1.AnnoPaused] == "true"
	conflict := metav1.Condition{Type: v1.CondConflict, Status: metav1.ConditionFalse, Reason: "none"}
	if t.Conflict != "" {
		conflict.Status, conflict.Reason, conflict.Message = metav1.ConditionTrue, "conflict", t.Conflict
	}
	pausedCond := metav1.Condition{Type: v1.CondPaused, Status: metav1.ConditionFalse, Reason: "not_paused"}
	if paused {
		pausedCond.Status, pausedCond.Reason = metav1.ConditionTrue, "paused"
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1.TideWorkload{}
		if err := r.Get(ctx, key, fresh); err != nil {
			return client.IgnoreNotFound(err)
		}
		changed := meta.SetStatusCondition(&fresh.Status.Conditions, conflict)
		changed = meta.SetStatusCondition(&fresh.Status.Conditions, pausedCond) || changed
		if !changed {
			return nil
		}
		return r.Status().Update(ctx, fresh)
	})
}

func (r *TidePolicyReconciler) updatePolicyStatus(
	ctx context.Context, policy *v1.TidePolicy, selected int32, conflicts, issues []string,
) error {
	ready := metav1.Condition{Type: v1.CondReady, Status: metav1.ConditionTrue, Reason: "resolved"}
	if len(issues) > 0 {
		ready.Reason, ready.Message = "resolved_with_issues", strings.Join(issues, "; ")
	}
	conflict := metav1.Condition{Type: v1.CondConflict, Status: metav1.ConditionFalse, Reason: "none"}
	if len(conflicts) > 0 {
		conflict.Status, conflict.Reason, conflict.Message = metav1.ConditionTrue, "conflict", strings.Join(conflicts, "; ")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1.TidePolicy{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(policy), fresh); err != nil {
			return client.IgnoreNotFound(err)
		}
		orig := fresh.Status.DeepCopy()
		fresh.Status.ObservedGeneration = fresh.Generation
		fresh.Status.SelectedWorkloads = selected
		meta.SetStatusCondition(&fresh.Status.Conditions, ready)
		meta.SetStatusCondition(&fresh.Status.Conditions, conflict)
		if equality.Semantic.DeepEqual(orig, &fresh.Status) {
			return nil
		}
		return r.Status().Update(ctx, fresh)
	})
}

// allPolicies enqueues every TidePolicy; workload changes can affect any selector.
func (r *TidePolicyReconciler) allPolicies(ctx context.Context, _ client.Object) []reconcile.Request {
	var list v1.TidePolicyList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).Error(err, "Listing TidePolicies for workload event")
		return nil
	}
	reqs := make([]reconcile.Request, len(list.Items))
	for i := range list.Items {
		reqs[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])}
	}
	return reqs
}

// SetupWithManager registers the controller. Deployments and StatefulSets are
// watched; other kinds named in policies are picked up by the periodic requeue.
func (r *TidePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.TidePolicy{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.allPolicies)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.allPolicies)).
		Named("tidepolicy").
		Complete(r)
}

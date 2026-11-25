package engine

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

// stillSelected re-checks the policy's namespace and label selectors against
// the target as just read. Errors it cannot judge count as selected, so a
// transient API failure never stops scaling.
func (e *Engine) stillSelected(ctx context.Context, pol *v1.TidePolicy, t workload.Target) bool {
	sel := pol.Spec.Selector
	ls, err := metav1.LabelSelectorAsSelector(&sel.LabelSelector)
	if err != nil {
		return false // the resolver selects nothing for an invalid selector
	}
	if !ls.Matches(labels.Set(t.Object.GetLabels())) {
		return false
	}
	if len(sel.Namespaces) == 0 && sel.NamespaceSelector == nil {
		return t.Namespace == pol.Namespace
	}
	if slices.Contains(sel.Namespaces, t.Namespace) {
		return true
	}
	if sel.NamespaceSelector == nil {
		return false
	}
	nsSel, err := metav1.LabelSelectorAsSelector(sel.NamespaceSelector)
	if err != nil {
		return false
	}
	var ns corev1.Namespace
	if err := e.c.Get(ctx, client.ObjectKey{Name: t.Namespace}, &ns); err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		logf.Log.WithName("engine").V(1).Info("Checking namespace selector", "namespace", t.Namespace, "error", err.Error())
		return true
	}
	return nsSel.Matches(labels.Set(ns.Labels))
}

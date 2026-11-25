package workload

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ScaleState is a workload's replica counts as seen through its scale subresource.
type ScaleState struct {
	Replicas, Ready int32
	Selector        string
}

// ScaleClient reads and writes workload replicas.
type ScaleClient interface {
	// Get reports replicas; Ready comes from .status.readyReplicas, else
	// from counting Ready pods matching the scale selector.
	Get(ctx context.Context, t Target) (ScaleState, error)
	// Set updates replicas through the scale subresource.
	Set(ctx context.Context, t Target, replicas int32) error
}

type scaleClient struct{ c client.Client }

// NewScaleClient returns a ScaleClient backed by c. The client's scheme must
// include core/v1.
func NewScaleClient(c client.Client) ScaleClient { return &scaleClient{c: c} }

func newWorkloadObject(t Target) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.FromAPIVersionAndKind(t.Ref.APIVersion, t.Ref.Kind))
	u.SetNamespace(t.Namespace)
	u.SetName(t.Ref.Name)
	return u
}

// newScaleObject is an autoscaling/v1 Scale as unstructured, which is what the
// unstructured workload client understands as a subresource body.
func newScaleObject(t Target) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "autoscaling", Version: "v1", Kind: "Scale"})
	u.SetNamespace(t.Namespace)
	u.SetName(t.Ref.Name)
	return u
}

func (s *scaleClient) Get(ctx context.Context, t Target) (ScaleState, error) {
	obj := newWorkloadObject(t)
	if err := s.c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return ScaleState{}, fmt.Errorf("get %s %s/%s: %w", t.Ref.Kind, t.Namespace, t.Ref.Name, err)
	}
	scale := newScaleObject(t)
	if err := s.c.SubResource("scale").Get(ctx, obj, scale); err != nil {
		return ScaleState{}, fmt.Errorf("get scale of %s %s/%s: %w", t.Ref.Kind, t.Namespace, t.Ref.Name, err)
	}
	replicas, _, _ := unstructured.NestedInt64(scale.Object, "spec", "replicas")
	selector, _, _ := unstructured.NestedString(scale.Object, "status", "selector")
	st := ScaleState{Replicas: int32(replicas), Selector: selector} //nolint:gosec // replica counts fit in int32

	if ready, found, err := unstructured.NestedInt64(obj.Object, "status", "readyReplicas"); err == nil && found {
		st.Ready = int32(ready) //nolint:gosec // replica counts fit in int32
		return st, nil
	}
	ready, err := s.countReadyPods(ctx, t.Namespace, st.Selector)
	if err != nil {
		return ScaleState{}, err
	}
	st.Ready = ready
	return st, nil
}

func (s *scaleClient) countReadyPods(ctx context.Context, ns, selector string) (int32, error) {
	if selector == "" {
		return 0, nil
	}
	sel, err := labels.Parse(selector)
	if err != nil {
		return 0, fmt.Errorf("parse selector %q: %w", selector, err)
	}
	var pods corev1.PodList
	if err := s.c.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return 0, fmt.Errorf("list pods in %s: %w", ns, err)
	}
	var n int32
	for i := range pods.Items {
		for _, c := range pods.Items[i].Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				n++
				break
			}
		}
	}
	return n, nil
}

func (s *scaleClient) Set(ctx context.Context, t Target, replicas int32) error {
	obj := newWorkloadObject(t)
	scale := newScaleObject(t)
	if err := unstructured.SetNestedField(scale.Object, int64(replicas), "spec", "replicas"); err != nil {
		return err
	}
	if err := s.c.SubResource("scale").Update(ctx, obj, client.WithSubResourceBody(scale)); err != nil {
		return fmt.Errorf("set scale of %s %s/%s: %w", t.Ref.Kind, t.Namespace, t.Ref.Name, err)
	}
	return nil
}

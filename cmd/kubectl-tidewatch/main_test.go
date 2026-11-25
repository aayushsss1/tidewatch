package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

func tw(ns, name string, ids ...string) *v1.TideWorkload {
	w := &v1.TideWorkload{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	for _, id := range ids {
		w.Status.PendingApprovals = append(w.Status.PendingApprovals, v1.PendingApproval{
			IncidentID: id, Confidence: "0.8", Reason: "storm",
			Override: v1.Override{MinReplicas: 8},
			Expires:  metav1.NewTime(time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)),
		})
	}
	return w
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1.TideWorkload{}).Build()
}

func get(t *testing.T, c client.Client, ns, name string) *v1.TideWorkload {
	t.Helper()
	var w v1.TideWorkload
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &w); err != nil {
		t.Fatal(err)
	}
	return &w
}

func TestApprove_AnnotatesMatchingTideWorkloads(t *testing.T) {
	c := fakeClient(t,
		tw("a", "web", "inc-1"), tw("a", "api", "inc-1", "inc-2"), tw("a", "db", "inc-9"), tw("b", "web", "inc-1"))
	var out bytes.Buffer
	if err := runApprove(context.Background(), c, &out, "inc-1", "a", false, "alice"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"web", "api"} {
		if got := get(t, c, "a", n).Annotations[v1.AnnoApprove]; got != "inc-1@alice" {
			t.Fatalf("%s annotation=%q", n, got)
		}
		if !strings.Contains(out.String(), "a/"+n) {
			t.Fatalf("output %q missing %s", out.String(), n)
		}
	}
	if _, ok := get(t, c, "a", "db").Annotations[v1.AnnoApprove]; ok {
		t.Fatal("db must not be annotated")
	}
	if _, ok := get(t, c, "b", "web").Annotations[v1.AnnoApprove]; ok {
		t.Fatal("namespace b must not be annotated without -A")
	}
	// -A reaches namespace b
	if err := runApprove(context.Background(), c, &out, "inc-1", "", true, "alice"); err != nil {
		t.Fatal(err)
	}
	if get(t, c, "b", "web").Annotations[v1.AnnoApprove] != "inc-1@alice" {
		t.Fatal("-A should annotate b/web")
	}
}

func TestApprove_NoMatchExitsNonZero(t *testing.T) {
	c := fakeClient(t, tw("a", "web", "inc-1"))
	err := runApprove(context.Background(), c, &bytes.Buffer{}, "nope", "a", false, "alice")
	if err == nil {
		t.Fatal("want error when nothing matches")
	}
	if code := exitCode(err); code != 1 {
		t.Fatalf("exit code %d", code)
	}
}

func TestReject_SetsRejectAnnotation(t *testing.T) {
	c := fakeClient(t, tw("a", "web", "inc-1"))
	if err := runReject(context.Background(), c, &bytes.Buffer{}, "inc-1", "a", false, "bob"); err != nil {
		t.Fatal(err)
	}
	w := get(t, c, "a", "web")
	if w.Annotations[v1.AnnoReject] != "inc-1@bob" {
		t.Fatalf("%v", w.Annotations)
	}
	if _, ok := w.Annotations[v1.AnnoApprove]; ok {
		t.Fatal("approve must stay unset")
	}
}

func TestPending_ListsRows(t *testing.T) {
	c := fakeClient(t, tw("a", "web", "inc-1"), tw("a", "api"), tw("b", "db", "inc-2"))
	var out bytes.Buffer
	if err := runPending(context.Background(), c, &out, "", "a", true, ""); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{"NAMESPACE", "WORKLOAD", "INCIDENT", "inc-1", "inc-2", "0.8", "8", "storm"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in\n%s", want, s)
		}
	}
	if strings.Contains(s, "api") {
		t.Fatalf("api has no pending approvals:\n%s", s)
	}
	out.Reset()
	if err := runPending(context.Background(), c, &out, "", "a", false, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "inc-2") {
		t.Fatal("namespace filter ignored")
	}
}

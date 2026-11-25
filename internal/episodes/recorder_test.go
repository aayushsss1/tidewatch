package episodes

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

const (
	leadName = "Web_Front"
	ns1      = "rd1"
	ns2      = "rd2"
	fri      = "Fri"
)

func ctx() context.Context { return context.Background() }

func nsObj(n string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}}
}

func view(id string, start time.Time) *IncidentView {
	return &IncidentView{
		ID: id, Policy: v1.PolicyRef{Namespace: "rec", Name: "pol"}, Lead: leadName,
		Start: start, Peak: start.Add(10 * time.Minute), End: start.Add(30 * time.Minute),
		LeadSeries: rampSeries(60, func(i int) float64 { return float64(i + 1) }),
		PeakRatio:  4.8, ProposalApplied: true, Action: "prescaled",
	}
}

func manual(name, ns, policy, lead string, start time.Time) *v1.TideEpisode {
	return &v1.TideEpisode{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{v1.LabelPolicy: policy, LabelLead: lead}},
		Spec: v1.TideEpisodeSpec{
			PolicyRef: v1.PolicyRef{Namespace: ns, Name: "pol"}, Lead: lead, Source: "manual", PeakRatio: "3.0",
			Start: metav1.NewTime(start), PeakAt: metav1.NewTime(start), End: metav1.NewTime(start),
			Calendar: v1.EpisodeCalendar{Weekday: fri, Hour: 9},
		},
	}
}

var _ = Describe("Recorder", func() {
	It("records an episode and prunes the oldest recorded beyond 50, never manual", func() {
		r := &Recorder{C: k8sClient, R: k8sClient, Clock: clock.RealClock{}, MaxPerLead: 50}
		Expect(r.Notable(&IncidentView{LongestAnomaly: 5 * time.Minute})).To(BeTrue())
		Expect(r.Notable(&IncidentView{MaxReplicaChange: 0.5})).To(BeFalse())
		Expect(r.Notable(&IncidentView{MaxReplicaChange: 0.51})).To(BeTrue())

		oldManual := manual("old-manual", "rec", "rec.pol", leadName, t0.Add(-1000*time.Hour))
		Expect(k8sClient.Create(ctx(), oldManual)).To(Succeed())

		var first string
		for i := range 52 {
			ep := r.Build(view(fmt.Sprintf("inc-%d", i), t0.Add(time.Duration(i)*time.Hour)))
			Expect(ep.Name).To(MatchRegexp(`^web-front-2026\d{8}-[0-9a-f]{6}$`))
			Expect(len(ep.Name)).To(BeNumerically("<=", 63))
			Expect(ep.Labels).To(HaveKeyWithValue(v1.LabelPolicy, "rec.pol"))
			Expect(ep.Labels).To(HaveKeyWithValue(LabelLead, leadName))
			if i == 0 {
				first = ep.Name
			}
			Expect(r.Record(ctx(), ep)).To(Succeed())
		}
		var list v1.TideEpisodeList
		Expect(k8sClient.List(ctx(), &list, client.InNamespace("rec"))).To(Succeed())
		recorded, names := 0, map[string]bool{}
		for _, e := range list.Items {
			names[e.Name] = true
			if e.Spec.Source == "recorded" {
				recorded++
			}
		}
		Expect(recorded).To(Equal(50))
		Expect(names).To(HaveKey("old-manual"))
		Expect(names).NotTo(HaveKey(first))
		Expect(strings.Join(mapKeys(names), ",")).To(ContainSubstring(r.Build(view("inc-51", t0.Add(51*time.Hour))).Name))
	})
})

var _ = Describe("Reader", func() {
	It("Similar returns top 3 above 0.5 across the given namespaces using the uncached reader", func() {
		mk := func(name, ns, policy, wd string, hour int32) *v1.TideEpisode {
			e := manual(name, ns, policy, "lead", t0)
			e.Spec.Calendar = v1.EpisodeCalendar{Weekday: wd, Hour: hour}
			return e
		}
		for _, e := range []*v1.TideEpisode{
			mk("a-best", ns1, "rd1.p", fri, 9),
			mk("b-good", ns2, "rd1.p", fri, 10),
			mk("c-ok", ns1, "rd1.p", fri, 11),
			mk("d-ok", ns2, "rd1.p", fri, 12),
			mk("e-bad", ns1, "rd1.p", "Mon", 21),
			mk("f-other-policy", ns1, "rd1.other", fri, 9),
		} {
			Expect(k8sClient.Create(ctx(), e)).To(Succeed())
		}
		rd := &Reader{R: k8sClient}
		q := Query{Weekday: time.Friday, Hour: 9, Deploy: false}
		ms, err := rd.Similar(ctx(), "rd1.p", []string{ns1, ns2}, q, 3, 0.5)
		Expect(err).NotTo(HaveOccurred())
		got := make([]string, 0, len(ms))
		for _, m := range ms {
			got = append(got, m.Name)
			Expect(m.Score).To(BeNumerically(">=", 0.5))
			Expect(len([]rune(m.Summary))).To(BeNumerically("<=", 400))
		}
		Expect(got).To(Equal([]string{"a-best", "b-good", "c-ok"}))

		find := Finder(rd, func(w tools.WorkloadRef) (string, []string, Query, bool) {
			return "rd1.p", []string{ns1, ns2}, q, w.Name == "lead"
		})
		sums, err := find(ctx(), tools.WorkloadRef{Namespace: ns1, Name: "lead"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sums).To(HaveLen(3))
		none, _ := find(ctx(), tools.WorkloadRef{Name: "x"})
		Expect(none).To(BeEmpty())
	})
})

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aayushsss1/tidewatch/test/utils"
)

const (
	kubeContext  = "kind-tidewatch"
	promPod      = "prometheus-kps-kube-prometheus-stack-prometheus-0"
	workloadName = "frontend-deploy"
)

type decision struct {
	Time    time.Time `json:"time"`
	Source  string    `json:"source"`
	Desired int32     `json:"desired"`
	Final   int32     `json:"final"`
	Verdict string    `json:"verdict"`
}

type tideWorkload struct {
	Status struct {
		Decisions []decision `json:"decisions"`
	} `json:"status"`
}

func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", append([]string{"--context", kubeContext}, args...)...))
}

// readState parses bin/demo-state.env written by hack/demo.sh.
func readState(root string) map[string]string {
	raw, err := os.ReadFile(filepath.Join(root, "bin", "demo-state.env"))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	out := map[string]string{}
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			out[k] = v
		}
	}
	return out
}

// promQuery runs an instant query inside the Prometheus pod and returns the result vector length.
func promQuery(q string) (int, error) {
	u := "http://localhost:9090/api/v1/query?query=" + url.QueryEscape(q)
	out, err := kubectl("-n", "monitoring", "exec", promPod, "-c", "prometheus", "--", "wget", "-qO-", u)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		return 0, fmt.Errorf("decode %q: %w", out, err)
	}
	if resp.Status != "success" {
		return 0, fmt.Errorf("query status %q", resp.Status)
	}
	return len(resp.Data.Result), nil
}

var _ = Describe("demo act 1", Ordered, func() {
	var (
		root string
		peak time.Time
	)

	BeforeAll(func() {
		var err error
		root, err = utils.GetProjectDir()
		Expect(err).NotTo(HaveOccurred())

		By("running hack/demo.sh")
		cmd := exec.Command("bash", "hack/demo.sh")
		cmd.Dir = root
		cmd.Stdout = GinkgoWriter
		cmd.Stderr = GinkgoWriter
		Expect(cmd.Run()).To(Succeed(), "hack/demo.sh failed")

		peak, err = time.Parse(time.RFC3339, readState(root)["PEAK"])
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if CurrentSpecReport().Failed() {
			out, _ := kubectl("-n", "tidewatch-system", "logs", "deploy/tidewatch-controller-manager", "--tail=200")
			_, _ = fmt.Fprintf(GinkgoWriter, "operator logs:\n%s\n", out)
			out, _ = kubectl("get", "tideworkloads", "-n", "shop", "-o", "yaml")
			_, _ = fmt.Fprintf(GinkgoWriter, "tideworkloads:\n%s\n", out)
		}
	})

	It("prescales frontend before the peak without exceeding ready capacity", func() {
		By("waiting for a baseline scale-up decision before the peak")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "tideworkload", workloadName, "-n", "shop", "-o", "json")
			g.Expect(err).NotTo(HaveOccurred())
			var tw tideWorkload
			g.Expect(json.Unmarshal([]byte(out), &tw)).To(Succeed())
			found := false
			for _, d := range tw.Status.Decisions {
				if d.Source == "baseline" && (d.Verdict == "applied" || d.Verdict == "clamped") &&
					d.Final > 1 && d.Time.Before(peak) {
					found = true
				}
			}
			g.Expect(found).To(BeTrue(), "no baseline applied/clamped scale-up before %s in %s", peak, out)
		}, 7*time.Minute, 10*time.Second).Should(Succeed())

		By("waiting for the peak to pass")
		time.Sleep(time.Until(peak.Add(30 * time.Second)))

		By("asserting load never exceeded ready capacity over the ramp")
		q := `max_over_time(tidewatch_capacity_ratio{workload="` + workloadName + `"}[6m]) < 1.0`
		Eventually(func(g Gomega) {
			n, err := promQuery(q)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(n).To(BeNumerically(">", 0), "capacity ratio reached >= 1.0 (or no series): "+strconv.Quote(q))
		}, 2*time.Minute, 10*time.Second).Should(Succeed())
	})
})

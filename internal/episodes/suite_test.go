package episodes

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestEpisodesSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Episodes Suite")
}

var _ = BeforeSuite(func() {
	Expect(v1.AddToScheme(scheme.Scheme)).To(Succeed())
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	base := filepath.Join("..", "..", "bin", "k8s")
	if entries, err := os.ReadDir(base); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				testEnv.BinaryAssetsDirectory = filepath.Join(base, e.Name())
				break
			}
		}
	}
	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	for _, ns := range []string{nsRec, ns1, ns2} {
		Expect(k8sClient.Create(ctx(), nsObj(ns))).To(Succeed())
	}
})

var _ = AfterSuite(func() { Expect(testEnv.Stop()).To(Succeed()) })

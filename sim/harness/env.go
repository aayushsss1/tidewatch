package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// env is the process-wide envtest API server shared by every run.
var env struct {
	mu     sync.Mutex
	e      *envtest.Environment
	cfg    *rest.Config
	scheme *k8sruntime.Scheme
}

// repoRoot locates the repository from this source file's path.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// firstAssetsDir finds envtest binaries under <repo>/bin/k8s, as `make setup-envtest`
// installs them.
func firstAssetsDir() string {
	base := filepath.Join(repoRoot(), "bin", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}

// newClient starts envtest on first use (assets: the argument, else KUBEBUILDER_ASSETS,
// else <repo>/bin/k8s/*) and returns a fresh direct client for it. The API server is
// reused by later calls in the same process.
func newClient(assets string) (client.Client, *k8sruntime.Scheme, error) {
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.e == nil {
		sch := k8sruntime.NewScheme()
		if err := clientgoscheme.AddToScheme(sch); err != nil {
			return nil, nil, err
		}
		if err := v1.AddToScheme(sch); err != nil {
			return nil, nil, err
		}
		e := &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join(repoRoot(), "config", "crd", "bases")},
			ErrorIfCRDPathMissing: true,
		}
		switch {
		case assets != "":
			e.BinaryAssetsDirectory = assets
		case os.Getenv("KUBEBUILDER_ASSETS") == "":
			e.BinaryAssetsDirectory = firstAssetsDir()
		}
		cfg, err := e.Start()
		if err != nil {
			return nil, nil, fmt.Errorf("starting envtest (run `make setup-envtest`): %w", err)
		}
		env.e, env.cfg, env.scheme = e, cfg, sch
	}
	c, err := client.New(env.cfg, client.Options{Scheme: env.scheme})
	return c, env.scheme, err
}

// Shutdown stops the shared envtest API server, if one was started. Call it once at
// process exit (for example from TestMain).
func Shutdown() error {
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.e == nil {
		return nil
	}
	err := env.e.Stop()
	env.e, env.cfg, env.scheme = nil, nil, nil
	return err
}

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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"sync/atomic"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	tidewatchv1alpha1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/agent"
	"github.com/aayushsss1/tidewatch/internal/controller"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/anthropic"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(tidewatchv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

const (
	providerNone      = "none"
	providerAnthropic = "anthropic"
	apiKeyEnv         = "TIDEWATCH_LLM_API_KEY" //nolint:gosec // env var name, not a credential
	webhookSecretKey  = "webhookUrl"
)

// lateSink forwards triggers to the agent runner, which is built after the
// engine it needs. Nothing is submitted before the manager starts.
type lateSink struct{ r atomic.Pointer[agent.Runner] }

func (s *lateSink) Submit(t signal.Trigger) {
	if r := s.r.Load(); r != nil {
		r.Submit(t)
	}
}

// newProvider returns the LLM provider, or nil (agent disabled) when none is
// configured or its API key is missing.
func newProvider(name, model string) llm.Provider {
	switch name {
	case providerNone:
		return nil
	case providerAnthropic:
		key := os.Getenv(apiKeyEnv)
		if key == "" {
			setupLog.Error(nil, "LLM provider requires an API key; agent disabled", "provider", name, "env", apiKeyEnv)
			return nil
		}
		return anthropic.New(key, model)
	}
	setupLog.Error(nil, "Unknown --llm-provider; agent disabled", "provider", name)
	return nil
}

// webhookURL reads the Slack webhook once from the named Secret in the
// operator namespace. Failure disables Slack only.
func webhookURL(r client.Reader, secret string) string {
	ns := os.Getenv("POD_NAMESPACE")
	if secret == "" || ns == "" {
		return ""
	}
	var sec corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: secret}, &sec); err != nil {
		setupLog.Error(err, "Reading Slack webhook secret; Slack disabled", "secret", secret)
		return ""
	}
	return string(sec.Data[webhookSecretKey])
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var webhookPort int
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var workers int
	var dryRun bool
	var defaultTimeToReady time.Duration
	var llmProvider, llmModel, slackWebhookSecret string
	var agentWorkers, tokenBudgetPerHour int
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port the webhook server listens on. "+
		"Defaults to 9443. Set -1 to disable the webhook server.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&workers, "workers", 8, "Number of parallel workers the engine uses to evaluate workloads.")
	flag.BoolVar(&dryRun, "dry-run", false, "If set, the engine never writes to the scale subresource.")
	flag.DurationVar(&defaultTimeToReady, "default-time-to-ready", 60*time.Second,
		"Time-to-ready assumed for a workload until enough pod startups have been observed.")
	flag.StringVar(&llmProvider, "llm-provider", providerNone,
		"LLM provider of the investigation agent: none or anthropic. "+
			"The API key is read from the "+apiKeyEnv+" environment variable.")
	flag.StringVar(&llmModel, "llm-model", "claude-sonnet-5-5", "Model the investigation agent uses.")
	flag.IntVar(&agentWorkers, "agent-workers", 4, "Number of parallel agent investigations.")
	flag.IntVar(&tokenBudgetPerHour, "agent-token-budget-per-hour", 200000, "LLM tokens the agent may spend per hour.")
	flag.StringVar(&slackWebhookSecret, "slack-webhook-secret", "",
		"Name of the Secret in the operator namespace whose '"+webhookSecretKey+"' key holds a Slack webhook URL.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
		Port:    webhookPort,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "b41aad7f.dev",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "Failed to create discovery client")
		os.Exit(1)
	}
	if err := (&controller.TidePolicyReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		ScaleSupport: workload.NewDiscoveryScaleSupport(dc),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "tidepolicy")
		os.Exit(1)
	}
	store := overrides.NewStore()
	sink := &lateSink{}
	eng := engine.New(mgr.GetClient(), engine.Options{
		Workers:            workers,
		DryRun:             dryRun,
		DefaultTimeToReady: defaultTimeToReady,
		Overrides:          store,
		APIReader:          mgr.GetAPIReader(),
		Triggers:           sink,
	})
	if err := mgr.Add(eng); err != nil {
		setupLog.Error(err, "Failed to add engine")
		os.Exit(1)
	}
	// The Notifier uses the classic record.EventRecorder API.
	recorder := mgr.GetEventRecorderFor("tidewatch") //nolint:staticcheck // see above
	runner := agent.NewRunner(mgr.GetClient(), mgr.GetAPIReader(), eng, store, recorder,
		agent.RunnerOptions{
			Workers: agentWorkers, TokenBudgetPerHour: tokenBudgetPerHour,
			Provider: newProvider(llmProvider, llmModel), WebhookURL: webhookURL(mgr.GetAPIReader(), slackWebhookSecret),
		})
	sink.r.Store(runner)
	if err := mgr.Add(runner); err != nil {
		setupLog.Error(err, "Failed to add agent runner")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

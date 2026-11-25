// Package workload resolves per-workload configuration.
package workload

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"text/template"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const (
	defaultHeadroom    = 0.8
	defaultMinReplicas = 1
)

// EffectiveConfig is the policy spec merged with a workload's annotations.
type EffectiveConfig struct {
	Query                    string // rendered: {{ .Namespace }}, {{ .Name }}, {{ .Kind }}
	TargetPerReplica         float64
	Headroom                 float64
	MinReplicas, MaxReplicas int32
	Paused                   bool
	Warnings                 []string // ignored/loosening annotations
}

// Effective merges policy and workload annotations. Annotations may only tighten guardrails.
func Effective(
	spec v1.TidePolicySpec, policyPaused bool, target metav1.Object, kind string,
) (EffectiveConfig, error) {
	annos := target.GetAnnotations()
	cfg := EffectiveConfig{
		Headroom:    defaultHeadroom,
		MinReplicas: defaultMinReplicas,
		MaxReplicas: spec.Guardrails.MaxReplicas,
		Paused:      policyPaused || annos[v1.AnnoPaused] == "true",
	}
	if spec.Guardrails.MinReplicas != nil {
		cfg.MinReplicas = *spec.Guardrails.MinReplicas
	}
	if spec.Load.Headroom != "" {
		h, err := parseDecimal(spec.Load.Headroom)
		if err != nil || h > 1 {
			return cfg, fmt.Errorf("invalid headroom %q: must be a decimal in (0, 1]", spec.Load.Headroom)
		}
		cfg.Headroom = h
	}

	name := spec.Load.DefaultProfile
	if p, ok := annos[v1.AnnoLoadProfile]; ok {
		name = p
	}
	profile, ok := spec.Load.Profiles[name]
	if !ok {
		return cfg, fmt.Errorf("load profile %q not found in policy", name)
	}
	tpr, err := parseDecimal(profile.TargetPerReplica)
	if err != nil {
		return cfg, fmt.Errorf("invalid targetPerReplica %q in profile %q: %w", profile.TargetPerReplica, name, err)
	}
	cfg.TargetPerReplica = tpr
	if v, ok := annos[v1.AnnoTargetPerRep]; ok {
		f, err := parseDecimal(v)
		if err != nil {
			return cfg, fmt.Errorf("annotation %s must be a decimal > 0, got %q", v1.AnnoTargetPerRep, v)
		}
		cfg.TargetPerReplica = f
	}

	// max first, so the min check sees the effective max and the pair can never invert.
	policyMin := cfg.MinReplicas
	if v, ok := annos[v1.AnnoMaxReplicas]; ok {
		switch n, err := strconv.ParseInt(v, 10, 32); {
		case err != nil:
			cfg.Warnings = append(cfg.Warnings, fmt.Sprintf("max-replicas %q is not an integer; ignored", v))
		case n < 1 || int32(n) < policyMin || int32(n) > cfg.MaxReplicas:
			cfg.Warnings = append(cfg.Warnings, fmt.Sprintf(
				"max-replicas %d outside allowed range [%d, %d]; ignored", n, max(policyMin, 1), cfg.MaxReplicas))
		default:
			cfg.MaxReplicas = int32(n)
		}
	}
	if v, ok := annos[v1.AnnoMinReplicas]; ok {
		switch n, err := strconv.ParseInt(v, 10, 32); {
		case err != nil:
			cfg.Warnings = append(cfg.Warnings, fmt.Sprintf("min-replicas %q is not an integer; ignored", v))
		case int32(n) < policyMin || int32(n) > cfg.MaxReplicas:
			cfg.Warnings = append(cfg.Warnings, fmt.Sprintf(
				"min-replicas %d outside allowed range [%d, %d]; ignored", n, policyMin, cfg.MaxReplicas))
		default:
			cfg.MinReplicas = int32(n)
		}
	}

	tmpl, err := template.New("query").Option("missingkey=error").Parse(profile.Query)
	if err != nil {
		return cfg, fmt.Errorf("parsing query template for profile %q: %w", name, err)
	}
	var buf bytes.Buffer
	data := map[string]string{"Namespace": target.GetNamespace(), "Name": target.GetName(), "Kind": kind}
	if err := tmpl.Execute(&buf, data); err != nil {
		return cfg, fmt.Errorf("rendering query for profile %q: %w", name, err)
	}
	cfg.Query = buf.String()
	return cfg, nil
}

var decimalRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// parseDecimal parses a plain decimal (matching the CRD pattern) that is finite and > 0.
func parseDecimal(s string) (float64, error) {
	if !decimalRE.MatchString(s) {
		return 0, fmt.Errorf("%q is not a plain decimal", s)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) || f <= 0 {
		return 0, fmt.Errorf("%q must be a finite number > 0", s)
	}
	return f, nil
}

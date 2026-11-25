package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

// Proposal is a validated propose_scaling call.
type Proposal struct {
	Overrides    []ProposedOverride
	Reason       string
	Confidence   float64
	TrustedModel string
	EpisodeRefs  []string
}

// ProposedOverride is one proposed override. A zero Start means "now" and is
// defaulted by the investigator.
type ProposedOverride struct {
	Workload    string
	MinReplicas int32
	Start, End  time.Time
}

// NoAction is a validated no_action call.
type NoAction struct {
	Reason     string
	Confidence float64
}

func terminal(name, desc string, props map[string]any, req ...string) llm.Tool {
	return &tool{
		def:      llm.ToolDef{Name: name, Description: desc, InputSchema: schema(props, req...)},
		terminal: true,
		run:      func(context.Context, json.RawMessage) (string, error) { return "ok", nil },
	}
}

func proposeTool() llm.Tool {
	num01 := map[string]any{keyType: "number", keyMinimum: 0, "maximum": 1}
	return terminal("propose_scaling", "Terminal. Propose scale-up overrides for in-scope workloads.", map[string]any{
		"overrides": map[string]any{keyType: "array", "items": map[string]any{
			keyType: "object",
			"properties": map[string]any{
				keyWorkload:   str("TideWorkload name"),
				"minReplicas": map[string]any{keyType: "integer", keyMinimum: 0},
				"start":       str("RFC3339; optional, defaults to now"),
				"end":         str("RFC3339"),
			},
			"required": []string{keyWorkload, "minReplicas", "end"},
		}},
		"reason":       str("why"),
		"confidence":   num01,
		"trustedModel": str("optional forecast model relied on"),
		"episodeRefs":  map[string]any{keyType: "array", "items": map[string]any{keyType: "string"}},
	}, "overrides", "reason", "confidence")
}

func noActionTool() llm.Tool {
	return terminal("no_action", "Terminal. Conclude that no scaling action is needed.", map[string]any{
		"reason":     str("why"),
		"confidence": map[string]any{keyType: "number", keyMinimum: 0, "maximum": 1},
	}, "reason", "confidence")
}

func checkCommon(reason string, conf float64) error {
	if reason == "" {
		return errors.New("reason is required")
	}
	if !(conf >= 0 && conf <= 1) {
		return errors.New("confidence must be within 0..1")
	}
	return nil
}

// ParseFinal validates a terminal tool call.
func ParseFinal(call llm.ToolCall) (p *Proposal, noAction *NoAction, err error) {
	switch call.Name {
	case "no_action":
		var a struct {
			Reason     string  `json:"reason"`
			Confidence float64 `json:"confidence"`
		}
		if err := json.Unmarshal(call.Input, &a); err != nil {
			return nil, nil, fmt.Errorf("invalid no_action input: %w", err)
		}
		if err := checkCommon(a.Reason, a.Confidence); err != nil {
			return nil, nil, err
		}
		return nil, &NoAction{Reason: a.Reason, Confidence: a.Confidence}, nil
	case "propose_scaling":
		var a struct {
			Overrides []struct {
				Workload    string     `json:"workload"`
				MinReplicas int32      `json:"minReplicas"`
				Start       *time.Time `json:"start"`
				End         *time.Time `json:"end"`
			} `json:"overrides"`
			Reason       string   `json:"reason"`
			Confidence   float64  `json:"confidence"`
			TrustedModel string   `json:"trustedModel"`
			EpisodeRefs  []string `json:"episodeRefs"`
		}
		if err := json.Unmarshal(call.Input, &a); err != nil {
			return nil, nil, fmt.Errorf("invalid propose_scaling input: %w", err)
		}
		if err := checkCommon(a.Reason, a.Confidence); err != nil {
			return nil, nil, err
		}
		if len(a.Overrides) == 0 {
			return nil, nil, errors.New("at least one override is required")
		}
		out := &Proposal{Reason: a.Reason, Confidence: a.Confidence, TrustedModel: a.TrustedModel, EpisodeRefs: a.EpisodeRefs}
		for i, o := range a.Overrides {
			if o.Workload == "" {
				return nil, nil, fmt.Errorf("override %d: workload is required", i)
			}
			if o.MinReplicas < 0 {
				return nil, nil, fmt.Errorf("override %d: minReplicas must be >= 0", i)
			}
			po := ProposedOverride{Workload: o.Workload, MinReplicas: o.MinReplicas}
			if o.Start != nil {
				po.Start = *o.Start
			}
			if o.End != nil {
				po.End = *o.End
			}
			if o.Start != nil && o.End != nil && !o.End.After(*o.Start) {
				return nil, nil, fmt.Errorf("override %d: end must be after start", i)
			}
			out.Overrides = append(out.Overrides, po)
		}
		return out, nil, nil
	}
	return nil, nil, fmt.Errorf("not a terminal tool: %q", call.Name)
}

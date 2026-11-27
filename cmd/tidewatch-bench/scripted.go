package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

// scriptedProvider is the deterministic, free stand-in for the model used by CI. For an
// incident brief it proposes, for every TideWorkload that has a trigger, a floor of twice its
// current replicas (clamped to its max); with no trigger it answers no_action. The end time
// is far in the future, so the engine clamps the window to its maximum.
type scriptedProvider struct{}

const scriptedModel = "scripted"

func (scriptedProvider) Name() string  { return "scripted" }
func (scriptedProvider) Model() string { return scriptedModel }

// brief line: "- frontend-deploy: replicas 5 (ready 5, max 40), load 12.3 ...; triggers: ContextDue(...)".
var reBrief = regexp.MustCompile(`(?m)^- (\S+): replicas (\d+) \(ready \d+, max (\d+)\).*triggers: `)

func (scriptedProvider) Turn(_ context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	var text string
	if len(req.Messages) > 0 {
		text = req.Messages[0].Text
	}
	var overrides []map[string]any
	for _, m := range reBrief.FindAllStringSubmatch(text, -1) {
		replicas, _ := strconv.Atoi(m[2])
		maxR, _ := strconv.Atoi(m[3])
		floor := min(max(2*replicas, 2), maxR)
		overrides = append(overrides, map[string]any{
			"workload": m[1], "minReplicas": floor, "end": "2099-01-01T00:00:00Z",
		})
	}
	name, in := "no_action", map[string]any{"reason": "no workload has a trigger", "confidence": 0.9}
	if len(overrides) > 0 {
		name, in = "propose_scaling", map[string]any{
			"overrides":  overrides,
			"reason":     "scripted: double " + strings.Join(workloadNames(overrides), ", "),
			"confidence": 0.9,
		}
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return llm.TurnResponse{}, err
	}
	return llm.TurnResponse{
		Message: llm.Message{Role: llm.RoleAssistant,
			ToolCall: &llm.ToolCall{ID: "scripted", Name: name, Input: raw}},
		InputTokens: 1000, OutputTokens: 100,
	}, nil
}

func workloadNames(os []map[string]any) []string {
	out := make([]string, len(os))
	for i, o := range os {
		out[i] = fmt.Sprint(o["workload"])
	}
	return out
}

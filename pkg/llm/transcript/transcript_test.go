package transcript_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/llmtest"
	"github.com/aayushsss1/tidewatch/pkg/llm/transcript"
)

type tool struct {
	name     string
	terminal bool
}

func (t tool) Def() llm.ToolDef { return llm.ToolDef{Name: t.name, InputSchema: json.RawMessage(`{}`)} }
func (t tool) Terminal() bool   { return t.terminal }
func (t tool) Run(context.Context, json.RawMessage) (string, error) {
	return "result-body", nil
}

func call(name string, in, out int) llm.TurnResponse {
	return llm.TurnResponse{
		Message: llm.Message{Role: llm.RoleAssistant,
			ToolCall: &llm.ToolCall{ID: "id", Name: name, Input: json.RawMessage(`{"q":1}`)}},
		InputTokens: in, OutputTokens: out,
	}
}

func TestWriter_OneJSONLinePerTurnTaggedWithIncident(t *testing.T) {
	var buf bytes.Buffer
	w := transcript.NewWriter(&buf)
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call("query", 10, 2), call("no_action", 20, 3)}}
	tools := []llm.Tool{tool{name: "query"}, tool{name: "no_action", terminal: true}}
	_, _, err := llm.Run(context.Background(), p, "SYS", "brief", tools,
		llm.Limits{MaxToolCalls: 5, Observer: w.Observer("inc-1")})
	if err != nil || w.Err() != nil {
		t.Fatal(err, w.Err())
	}
	var recs []transcript.Record
	sc := bufio.NewScanner(&buf)
	for sc.Scan() {
		var r transcript.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	r0, r1 := recs[0], recs[1]
	if r0.Incident != "inc-1" || r0.Turn != 1 || r0.System != "SYS" ||
		len(r0.Messages) != 1 || r0.Messages[0].Text != "brief" {
		t.Errorf("turn 1 = %+v", r0)
	}
	if r0.Response == nil || r0.Response.ToolCall == nil || r0.Response.ToolCall.Name != "query" ||
		string(r0.Response.ToolCall.Input) != `{"q":1}` || r0.InputTokens != 10 || r0.OutputTokens != 2 {
		t.Errorf("turn 1 response = %+v", r0)
	}
	if r0.ToolResult == nil || r0.ToolResult.Content != llm.WrapData("query", "result-body") || r0.Exit != "" {
		t.Errorf("turn 1 tool result = %+v", r0.ToolResult)
	}
	if r1.System != "" || len(r1.Messages) != 3 || r1.Exit != llm.ExitTerminal || r1.InputTokens != 20 {
		t.Errorf("turn 2 = %+v", r1)
	}
}

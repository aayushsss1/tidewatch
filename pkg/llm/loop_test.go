package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/llmtest"
)

type fakeTool struct {
	name     string
	terminal bool
	out      string
	err      error
	runs     int
}

func (f *fakeTool) Def() llm.ToolDef {
	return llm.ToolDef{Name: f.name, InputSchema: json.RawMessage(`{}`)}
}
func (f *fakeTool) Terminal() bool { return f.terminal }
func (f *fakeTool) Run(context.Context, json.RawMessage) (string, error) {
	f.runs++
	return f.out, f.err
}

func call(name string) llm.TurnResponse {
	return llm.TurnResponse{Message: llm.Message{Role: llm.RoleAssistant,
		ToolCall: &llm.ToolCall{ID: "id-" + name, Name: name, Input: json.RawMessage(`{"a":1}`)}}}
}

func tokens(r llm.TurnResponse, in, out int) llm.TurnResponse {
	r.InputTokens, r.OutputTokens = in, out
	return r
}

var (
	lim      = llm.Limits{MaxToolCalls: 5, MaxTokens: 100000}
	twoTurns = llm.Limits{MaxToolCalls: 2, MaxTokens: 1000000}
	capped   = llm.Limits{MaxToolCalls: 10, MaxTokens: 30000}
	soft     = llm.Limits{MaxToolCalls: 10, MaxTokens: 60000}
)

const (
	toolQuery = "query"
	toolEnd   = "no_action"
)

func TestRun_TerminalOnFirstTurn(t *testing.T) {
	q := &fakeTool{name: toolQuery}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolEnd)}}
	out, trace, err := llm.Run(context.Background(), p, "sys", "brief", []llm.Tool{q, end}, lim)
	if err != nil || out.Exit != llm.ExitTerminal || out.Final.Name != "no_action" || q.runs != 0 || len(trace) != 0 {
		t.Fatalf("out=%+v trace=%v err=%v", out, trace, err)
	}
}

func TestRun_ToolThenTerminal(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: "hello"}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolQuery), call(toolEnd)}}
	out, trace, _ := llm.Run(context.Background(), p, "sys", "brief", []llm.Tool{q, end}, lim)
	if out.Exit != llm.ExitTerminal || len(trace) != 1 || trace[0].Tool != "query" || trace[0].Args != `{"a":1}` {
		t.Fatalf("out=%+v trace=%+v", out, trace)
	}
	msgs := p.Calls[1].Messages
	last := msgs[len(msgs)-1]
	if last.ToolResult == nil || last.ToolResult.CallID != "id-query" ||
		last.ToolResult.Content != llm.WrapData("query", "hello") {
		t.Fatalf("last message %+v", last)
	}
}

func TestRun_ForcedFinalOnLastTurn(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: "x"}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolQuery), call(toolEnd)}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, twoTurns)
	r := p.Calls[1]
	if out.Exit != "forced_final" || r.Choice != "final" || len(r.Tools) != 1 || r.Tools[0].Name != "no_action" {
		t.Fatalf("out=%+v req=%+v", out, r)
	}
	if p.Calls[0].Choice != "auto" || len(p.Calls[0].Tools) != 2 {
		t.Fatalf("first req %+v", p.Calls[0])
	}
}

func TestRun_DeadlineCancelsHungProvider(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Delay: 5 * time.Second, Turns: []llm.TurnResponse{call(toolEnd)}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	out, _, _ := llm.Run(ctx, p, "s", "b", []llm.Tool{end}, lim)
	if out.Exit != "deadline" || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("exit=%s after %v", out.Exit, time.Since(start))
	}
}

func TestRun_Cancelled(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolEnd)}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, _, _ := llm.Run(ctx, p, "s", "b", []llm.Tool{end}, lim)
	if out.Exit != "cancelled" {
		t.Fatalf("exit=%s", out.Exit)
	}
}

func TestRun_ProviderError(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Errs: []error{errors.New("boom")}}
	out, _, err := llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, lim)
	if out.Exit != "provider_error" || err == nil {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestRun_TokenCap(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: "x"}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{
		tokens(call(toolQuery), 30000, 5000), call(toolEnd)}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, capped)
	if out.Exit != "token_cap" || len(p.Calls) != 1 || out.InputTokens != 30000 || out.OutputTokens != 5000 {
		t.Fatalf("out=%+v calls=%d", out, len(p.Calls))
	}
}

func TestRun_ProjectedOverrunForcesFinal(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: "x"}
	end := &fakeTool{name: toolEnd, terminal: true}
	// Input grows 17k on turn 2; used(38000)+growth(17000)+8192 > 60000, so
	// turn 3 must be the forced final turn even though used < cap.
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{
		tokens(call(toolQuery), 10000, 500), tokens(call(toolQuery), 27000, 500), call(toolEnd)}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, soft)
	if out.Exit != llm.ExitForcedFinal || len(p.Calls) != 3 {
		t.Fatalf("out=%+v calls=%d", out, len(p.Calls))
	}
	last := p.Calls[2]
	if last.Choice != llm.ChoiceFinal || len(last.Tools) != 1 || p.Calls[1].Choice != llm.ChoiceAuto {
		t.Fatalf("calls[1]=%s last=%+v", p.Calls[1].Choice, last)
	}
}

func TestRun_ToolErrorIsResultNotFailure(t *testing.T) {
	q := &fakeTool{name: toolQuery, err: errors.New("bad input")}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolQuery), call(toolEnd)}}
	out, _, err := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, lim)
	msgs := p.Calls[1].Messages
	res := msgs[len(msgs)-1].ToolResult
	if err != nil || out.Exit != llm.ExitTerminal || res == nil || !res.IsError ||
		!strings.Contains(res.Content, "bad input") {
		t.Fatalf("out=%+v err=%v res=%+v", out, err, res)
	}
}

func TestRun_TruncatesLargeToolOutput(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: strings.Repeat("é", 5000)} // 10KB
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{call(toolQuery), call(toolEnd)}}
	_, trace, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, lim)
	msgs := p.Calls[1].Messages
	content := msgs[len(msgs)-1].ToolResult.Content
	if len(content) > 4096+100 || !strings.Contains(content, "…[truncated]") || trace[0].ResultBytes != len(content) {
		t.Fatalf("len=%d", len(content))
	}
}

func TestRun_NoToolCall(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{text("hi"), text("still no")}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, lim)
	if out.Exit != "no_tool_call" || out.Final != nil {
		t.Fatalf("out=%+v", out)
	}
}

func text(s string) llm.TurnResponse {
	return llm.TurnResponse{Message: llm.Message{Role: llm.RoleAssistant, Text: s}}
}

func TestRun_NudgeThenTerminal(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{text("thinking"), call(toolEnd)}}
	out, _, err := llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, lim)
	if err != nil || out.Exit != llm.ExitTerminal || out.Final == nil {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	msgs := p.Calls[1].Messages
	if len(msgs) != 3 || msgs[1].Role != llm.RoleAssistant || msgs[1].Text != "thinking" ||
		msgs[2].Role != llm.RoleUser || msgs[2].Text != llm.NudgeText {
		t.Fatalf("messages after nudge: %+v", msgs)
	}
}

func TestRun_NudgeOnlyOnce(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{text("a"), text("b"), call(toolEnd)}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, lim)
	if out.Exit != llm.ExitNoToolCall || len(p.Calls) != 2 {
		t.Fatalf("out=%+v calls=%d", out, len(p.Calls))
	}
}

func TestRun_NudgeCountsTowardMaxToolCalls(t *testing.T) {
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{text("a"), call(toolEnd)}}
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, twoTurns)
	if out.Exit != llm.ExitForcedFinal {
		t.Fatalf("exit = %s, want forced_final (the nudge used turn 1 of 2)", out.Exit)
	}
	if p.Calls[1].Choice != llm.ChoiceFinal {
		t.Fatalf("turn after nudge choice = %s, want final", p.Calls[1].Choice)
	}
	// A text-only reply on the forced final turn is not nudged again.
	p = &llmtest.Scripted{Turns: []llm.TurnResponse{text("a"), text("b")}}
	out, _, _ = llm.Run(context.Background(), p, "s", "b", []llm.Tool{end}, twoTurns)
	if out.Exit != llm.ExitNoToolCall || len(p.Calls) != 2 {
		t.Fatalf("out=%+v calls=%d", out, len(p.Calls))
	}
}

func TestRun_ObserverSeesEveryTurn(t *testing.T) {
	q := &fakeTool{name: toolQuery, out: "hello"}
	end := &fakeTool{name: toolEnd, terminal: true}
	p := &llmtest.Scripted{Turns: []llm.TurnResponse{
		tokens(text("hm"), 10, 1), tokens(call(toolQuery), 20, 2), tokens(call(toolEnd), 30, 3)}}
	var turns []llm.Turn
	l := lim
	l.Observer = func(t llm.Turn) { turns = append(turns, t) }
	out, _, _ := llm.Run(context.Background(), p, "s", "b", []llm.Tool{q, end}, l)
	if out.Exit != llm.ExitTerminal || len(turns) != 3 {
		t.Fatalf("out=%+v turns=%d", out, len(turns))
	}
	if !turns[0].Nudged || turns[0].Exit != "" || turns[0].Response.InputTokens != 10 {
		t.Errorf("turn 1 = %+v", turns[0])
	}
	if r := turns[1].ToolResult; r == nil || r.Content != llm.WrapData("query", "hello") || turns[1].Response.Message.ToolCall.Name != toolQuery {
		t.Errorf("turn 2 = %+v", turns[1])
	}
	if turns[2].Exit != llm.ExitTerminal || turns[2].ToolResult != nil || turns[2].N != 3 {
		t.Errorf("turn 3 = %+v", turns[2])
	}
}

func TestTruncate_RuneSafe(t *testing.T) {
	got := llm.Truncate("ééé", 3) // 3 bytes would split the second rune
	if got != "é…[truncated]" {
		t.Fatalf("got %q", got)
	}
}

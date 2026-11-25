package anthropic_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/anthropic"
)

const (
	toolName = "query_prometheus"
	prevID   = "toolu_0"
)

const toolUseReply = `{"id":"msg_1","type":"message","role":"assistant","model":"claude-test",
"content":[{"type":"text","text":"checking"},
{"type":"tool_use","id":"toolu_1","name":"query_prometheus","input":{"q":"up"}}],
"stop_reason":"tool_use","usage":{"input_tokens":123,"output_tokens":45}}`

// fake serves reply (status code) and captures the decoded request body.
func fake(t *testing.T, status int, reply string) (llm.Provider, *map[string]any) {
	t.Helper()
	body := map[string]any{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("bad request json: %v", err)
		}
		if r.Header.Get("x-api-key") != "test-key" {
			t.Errorf("api key header = %q", r.Header.Get("x-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	return anthropic.New("test-key", "claude-test", anthropic.WithBaseURL(srv.URL)), &body
}

var queryTool = llm.ToolDef{
	Name:        toolName,
	Description: "run a query",
	InputSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`),
}

func TestTurn_SendsToolsAndParsesToolUse(t *testing.T) {
	p, body := fake(t, 200, toolUseReply)
	if p.Name() != "anthropic" || p.Model() != "claude-test" {
		t.Fatalf("name/model = %s/%s", p.Name(), p.Model())
	}
	resp, err := p.Turn(context.Background(), llm.TurnRequest{
		System:    "be careful",
		Messages:  []llm.Message{{Role: llm.RoleUser, Text: "go"}},
		Tools:     []llm.ToolDef{queryTool},
		Choice:    llm.ChoiceAuto,
		MaxTokens: 777,
	})
	if err != nil {
		t.Fatal(err)
	}
	tc := resp.Message.ToolCall
	if resp.Message.Role != llm.RoleAssistant || tc == nil || tc.ID != "toolu_1" || tc.Name != toolName ||
		string(tc.Input) != `{"q":"up"}` || resp.Message.Text != "checking" {
		t.Fatalf("message = %+v", resp.Message)
	}
	b := *body
	if b["model"] != "claude-test" || b["max_tokens"] != float64(777) {
		t.Fatalf("body = %v", b)
	}
	sys := b["system"].([]any)[0].(map[string]any)
	if sys["text"] != "be careful" {
		t.Fatalf("system = %v", b["system"])
	}
	tool := b["tools"].([]any)[0].(map[string]any)
	schema := tool["input_schema"].(map[string]any)
	if tool["name"] != toolName || tool["description"] != "run a query" ||
		schema["type"] != "object" || schema["properties"] == nil || schema["required"].([]any)[0] != "q" {
		t.Fatalf("tool = %v", tool)
	}
	if b["tool_choice"].(map[string]any)["type"] != "auto" {
		t.Fatalf("tool_choice = %v", b["tool_choice"])
	}
}

func TestTurn_FinalChoiceIsAutoWithSingleToolUse(t *testing.T) {
	p, body := fake(t, 200, toolUseReply)
	_, err := p.Turn(context.Background(), llm.TurnRequest{
		Messages: []llm.Message{{Role: llm.RoleUser, Text: "go"}},
		Tools:    []llm.ToolDef{queryTool}, Choice: llm.ChoiceFinal, MaxTokens: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := *body
	tc := b["tool_choice"].(map[string]any)
	if tc["type"] != "auto" || tc["disable_parallel_tool_use"] != true || len(b["tools"].([]any)) != 1 {
		t.Fatalf("body = %v", b)
	}
	if b["output_config"].(map[string]any)["effort"] != "medium" {
		t.Fatalf("output_config = %v", b["output_config"])
	}
}

func TestTurn_MaxTokensStopDropsToolCall(t *testing.T) {
	reply := `{"id":"msg_2","type":"message","role":"assistant","model":"claude-test",
"content":[{"type":"text","text":"partial"},
{"type":"tool_use","id":"toolu_2","name":"query_prometheus","input":{}}],
"stop_reason":"max_tokens","usage":{"input_tokens":1,"output_tokens":2}}`
	p, _ := fake(t, 200, reply)
	resp, err := p.Turn(context.Background(), llm.TurnRequest{
		Messages: []llm.Message{{Role: llm.RoleUser, Text: "go"}},
		Tools:    []llm.ToolDef{queryTool}, Choice: llm.ChoiceAuto, MaxTokens: 10,
	})
	if err != nil || resp.Message.ToolCall != nil || resp.Message.Text != "partial" {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
}

func TestTurn_MapsToolResults(t *testing.T) {
	p, body := fake(t, 200, toolUseReply)
	_, err := p.Turn(context.Background(), llm.TurnRequest{
		Messages: []llm.Message{
			{Role: llm.RoleUser, Text: "go"},
			{Role: llm.RoleAssistant, Text: "checking", ToolCall: &llm.ToolCall{
				ID: prevID, Name: toolName, Input: json.RawMessage(`{"q":"up"}`)}},
			{Role: llm.RoleTool, ToolResult: &llm.ToolResult{CallID: prevID, Content: "42", IsError: true}},
		},
		Tools: []llm.ToolDef{queryTool}, Choice: llm.ChoiceAuto, MaxTokens: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs := (*body)["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages = %v", msgs)
	}
	asst := msgs[1].(map[string]any)
	use := asst["content"].([]any)[1].(map[string]any)
	if asst["role"] != "assistant" || use["type"] != "tool_use" || use["id"] != prevID ||
		use["name"] != toolName || use["input"].(map[string]any)["q"] != "up" {
		t.Fatalf("assistant = %v", asst)
	}
	user := msgs[2].(map[string]any)
	res := user["content"].([]any)[0].(map[string]any)
	if user["role"] != "user" || res["type"] != "tool_result" || res["tool_use_id"] != prevID ||
		res["is_error"] != true {
		t.Fatalf("tool result = %v", user)
	}
	if c, _ := json.Marshal(res["content"]); !json.Valid(c) || string(c) == "null" {
		t.Fatalf("content = %v", res["content"])
	}
}

func TestTurn_ReportsUsage(t *testing.T) {
	p, _ := fake(t, 200, toolUseReply)
	resp, err := p.Turn(context.Background(), llm.TurnRequest{
		Messages: []llm.Message{{Role: llm.RoleUser, Text: "go"}}, Choice: llm.ChoiceAuto, MaxTokens: 10,
	})
	if err != nil || resp.InputTokens != 123 || resp.OutputTokens != 45 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
}

func TestTurn_HTTPErrorIsError(t *testing.T) {
	for _, code := range []int{429, 529} {
		p, _ := fake(t, code, `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`)
		_, err := p.Turn(context.Background(), llm.TurnRequest{
			Messages: []llm.Message{{Role: llm.RoleUser, Text: "go"}}, Choice: llm.ChoiceAuto, MaxTokens: 10,
		})
		if err == nil {
			t.Fatalf("status %d: expected error", code)
		}
	}
}

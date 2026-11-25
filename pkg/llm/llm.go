// Package llm is a small provider-agnostic agent layer: a message model, a
// Provider interface and a bounded tool-calling loop.
package llm

import (
	"context"
	"encoding/json"
)

// Role is the author of a Message.
type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is one conversation entry. An assistant message may carry a
// ToolCall; a tool message carries the ToolResult.
type Message struct {
	Role       Role
	Text       string
	ToolCall   *ToolCall
	ToolResult *ToolResult
}

// ToolCall is a model request to run a tool.
type ToolCall struct {
	ID, Name string
	Input    json.RawMessage
}

// ToolResult is the outcome of a ToolCall, addressed by CallID.
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// ToolChoice constrains whether the model must call a tool.
type ToolChoice string

const (
	// ChoiceAuto lets the model decide.
	ChoiceAuto ToolChoice = "auto"
	// ChoiceFinal forces a call to one of the offered tools.
	ChoiceFinal ToolChoice = "final"
)

// ToolDef describes a tool to the model.
type ToolDef struct {
	Name, Description string
	InputSchema       json.RawMessage
}

// TurnRequest is one model invocation.
type TurnRequest struct {
	System    string
	Messages  []Message
	Tools     []ToolDef
	Choice    ToolChoice
	MaxTokens int
}

// TurnResponse is the model's reply and its token usage.
type TurnResponse struct {
	Message                   Message
	InputTokens, OutputTokens int
}

// Provider is an LLM backend.
type Provider interface {
	Name() string
	Model() string
	Turn(ctx context.Context, req TurnRequest) (TurnResponse, error)
}

// Tool is something the agent can call. Terminal tools end the loop: the
// call itself is the agent's answer and is never executed.
type Tool interface {
	Def() ToolDef
	Terminal() bool
	Run(ctx context.Context, input json.RawMessage) (string, error)
}

// Package anthropic implements llm.Provider on the Anthropic Messages API.
package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

type provider struct {
	client sdk.Client
	model  string
}

// Option customises New.
type Option func(*[]option.RequestOption)

// WithBaseURL points the client at another endpoint (used by tests).
func WithBaseURL(url string) Option {
	return func(o *[]option.RequestOption) { *o = append(*o, option.WithBaseURL(url)) }
}

// New returns a provider for the given model. Client-side retries are off:
// the agent loop owns the time budget, and a failed turn is reported as such.
func New(apiKey, model string, opts ...Option) llm.Provider {
	ro := []option.RequestOption{option.WithAPIKey(apiKey), option.WithMaxRetries(0)}
	for _, o := range opts {
		o(&ro)
	}
	return &provider{client: sdk.NewClient(ro...), model: model}
}

func (p *provider) Name() string  { return "anthropic" }
func (p *provider) Model() string { return p.model }

func (p *provider) Turn(ctx context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	params := sdk.MessageNewParams{
		Model:     p.model,
		MaxTokens: int64(req.MaxTokens),
	}
	if req.System != "" {
		params.System = []sdk.TextBlockParam{{Text: req.System}}
	}
	for _, m := range req.Messages {
		mp, err := toMessage(m)
		if err != nil {
			return llm.TurnResponse{}, err
		}
		params.Messages = append(params.Messages, mp)
	}
	for _, t := range req.Tools {
		tp, err := toTool(t)
		if err != nil {
			return llm.TurnResponse{}, err
		}
		params.Tools = append(params.Tools, sdk.ToolUnionParam{OfTool: &tp})
	}
	if len(req.Tools) > 0 {
		if req.Choice == llm.ChoiceFinal {
			params.ToolChoice = sdk.ToolChoiceUnionParam{OfAny: &sdk.ToolChoiceAnyParam{}}
		} else {
			params.ToolChoice = sdk.ToolChoiceUnionParam{OfAuto: &sdk.ToolChoiceAutoParam{}}
		}
	}

	resp, err := p.client.Messages.New(ctx, params)
	if err != nil {
		return llm.TurnResponse{}, fmt.Errorf("anthropic: %w", err)
	}

	out := llm.TurnResponse{
		Message:      llm.Message{Role: llm.RoleAssistant},
		InputTokens:  int(resp.Usage.InputTokens),
		OutputTokens: int(resp.Usage.OutputTokens),
	}
	for _, b := range resp.Content {
		switch b.Type {
		case "text":
			out.Message.Text += b.Text
		case "tool_use":
			if out.Message.ToolCall == nil { // one call per turn
				tu := b.AsToolUse()
				out.Message.ToolCall = &llm.ToolCall{ID: tu.ID, Name: tu.Name, Input: tu.Input}
			}
		}
	}
	return out, nil
}

func toMessage(m llm.Message) (sdk.MessageParam, error) {
	switch m.Role {
	case llm.RoleUser:
		return sdk.NewUserMessage(sdk.NewTextBlock(m.Text)), nil
	case llm.RoleAssistant:
		var blocks []sdk.ContentBlockParamUnion
		if m.Text != "" {
			blocks = append(blocks, sdk.NewTextBlock(m.Text))
		}
		if m.ToolCall != nil {
			in := m.ToolCall.Input
			if len(in) == 0 {
				in = json.RawMessage(`{}`)
			}
			blocks = append(blocks, sdk.NewToolUseBlock(m.ToolCall.ID, in, m.ToolCall.Name))
		}
		return sdk.NewAssistantMessage(blocks...), nil
	case llm.RoleTool:
		if m.ToolResult == nil {
			return sdk.MessageParam{}, errors.New("anthropic: tool message without result")
		}
		r := m.ToolResult
		return sdk.NewUserMessage(sdk.NewToolResultBlock(r.CallID, r.Content, r.IsError)), nil
	}
	return sdk.MessageParam{}, fmt.Errorf("anthropic: unknown role %q", m.Role)
}

func toTool(t llm.ToolDef) (sdk.ToolParam, error) {
	tp := sdk.ToolParam{Name: t.Name, Description: sdk.String(t.Description)}
	if len(t.InputSchema) == 0 {
		return tp, nil
	}
	var raw map[string]any
	if err := json.Unmarshal(t.InputSchema, &raw); err != nil {
		return tp, fmt.Errorf("anthropic: tool %s schema: %w", t.Name, err)
	}
	tp.InputSchema.Properties = raw["properties"]
	if req, ok := raw["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				tp.InputSchema.Required = append(tp.InputSchema.Required, s)
			}
		}
	}
	delete(raw, "properties")
	delete(raw, "required")
	delete(raw, "type")
	tp.InputSchema.ExtraFields = raw
	return tp, nil
}

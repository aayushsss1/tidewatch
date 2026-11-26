// Package transcript writes llm.Run turns as JSON lines, one object per turn.
package transcript

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

// Message is the JSON form of llm.Message.
type Message struct {
	Role       string          `json:"role"`
	Text       string          `json:"text,omitempty"`
	ToolCall   *ToolCall       `json:"toolCall,omitempty"`
	ToolResult *llm.ToolResult `json:"toolResult,omitempty"`
}

// ToolCall is the JSON form of llm.ToolCall.
type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input,omitempty"`
}

// Record is one JSON line: a single model turn of one investigation.
type Record struct {
	Incident string `json:"incident"`
	Turn     int    `json:"turn"`
	System   string `json:"system,omitempty"` // first turn only
	// Messages are the request messages of the turn, as sent.
	Messages     []Message       `json:"messages"`
	Response     *Message        `json:"response,omitempty"`
	ToolResult   *llm.ToolResult `json:"toolResult,omitempty"` // as sent back, truncated
	InputTokens  int             `json:"inputTokens"`
	OutputTokens int             `json:"outputTokens"`
	Nudged       bool            `json:"nudged,omitempty"`
	Exit         llm.Exit        `json:"exit,omitempty"` // last turn only
	Error        string          `json:"error,omitempty"`
}

// Writer serializes records from any number of investigations to one io.Writer.
type Writer struct {
	mu  sync.Mutex
	enc *json.Encoder
	err error
}

// NewWriter returns a Writer that emits one JSON object per line to w.
func NewWriter(w io.Writer) *Writer { return &Writer{enc: json.NewEncoder(w)} }

// Err returns the first write error, if any.
func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Observer returns an llm.Limits.Observer that tags every record with incident.
func (w *Writer) Observer(incident string) func(llm.Turn) {
	return func(t llm.Turn) { w.write(incident, t) }
}

func (w *Writer) write(incident string, t llm.Turn) {
	rec := Record{
		Incident: incident, Turn: t.N, Messages: convert(t.Request.Messages),
		InputTokens: t.Response.InputTokens, OutputTokens: t.Response.OutputTokens,
		ToolResult: t.ToolResult, Nudged: t.Nudged, Exit: t.Exit,
	}
	if t.N == 1 {
		rec.System = t.Request.System
	}
	if m := t.Response.Message; m.Text != "" || m.ToolCall != nil {
		c := convert([]llm.Message{m})
		rec.Response = &c[0]
	}
	if t.Err != nil {
		rec.Error = t.Err.Error()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(rec); err != nil && w.err == nil {
		w.err = err
	}
}

func convert(in []llm.Message) []Message {
	out := make([]Message, len(in))
	for i, m := range in {
		out[i] = Message{Role: string(m.Role), Text: m.Text, ToolResult: m.ToolResult}
		if m.ToolCall != nil {
			out[i].ToolCall = &ToolCall{ID: m.ToolCall.ID, Name: m.ToolCall.Name, Input: m.ToolCall.Input}
		}
	}
	return out
}

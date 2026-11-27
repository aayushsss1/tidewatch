package llm

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Exit says why the loop stopped.
type Exit string

const (
	ExitTerminal      Exit = "terminal"
	ExitForcedFinal   Exit = "forced_final"
	ExitDeadline      Exit = "deadline"
	ExitTokenCap      Exit = "token_cap"
	ExitProviderError Exit = "provider_error"
	ExitCancelled     Exit = "cancelled"
	ExitNoToolCall    Exit = "no_tool_call"
)

const (
	maxToolResultBytes = 4096
	maxArgsBytes       = 200
	turnMaxTokens      = 8192
)

// Limits bounds the loop. The deadline comes from the context.
type Limits struct {
	MaxToolCalls int
	MaxTokens    int
	// Observer, when non-nil, is called synchronously once per model turn. It must not
	// retain or modify Turn.Request.Messages, which the loop keeps appending to.
	Observer func(Turn)
}

// NudgeText is the user message sent after a text-only reply that is not on the final turn.
const NudgeText = "Call propose_scaling or no_action now. Do not reply with text only."

// Turn is what Observer sees for one model turn.
type Turn struct {
	N        int // 1-based
	Request  TurnRequest
	Response TurnResponse // zero when the provider failed
	// ToolResult is the result appended for the turn's tool call, as sent back to the
	// model (wrapped and truncated). Nil for terminal calls, nudges and failed turns.
	ToolResult *ToolResult
	// Nudged is true when the turn was text-only and the loop appended NudgeText.
	Nudged bool
	// Exit is set only on the loop's last turn.
	Exit Exit
	// Err is the provider error, if any.
	Err error
}

// Outcome is the result of Run. Final is set only for terminal/forced_final.
type Outcome struct {
	Exit                      Exit
	Final                     *ToolCall
	InputTokens, OutputTokens int
}

// TraceEntry records one non-terminal tool invocation.
type TraceEntry struct {
	Tool        string
	Args        string
	ResultBytes int
	Latency     time.Duration
}

// Truncate cuts s to at most n bytes without splitting a rune, appending a
// marker when it cut anything.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && (s[cut]&0xC0) == 0x80 { // s[cut] is a continuation byte
		cut--
	}
	return s[:cut] + "…[truncated]"
}

// WrapData marks tool output as data, not instructions.
func WrapData(toolName, content string) string {
	return fmt.Sprintf("<tool_result name=%q>\n%s\n</tool_result>", toolName, content)
}

// Run drives the agent: up to lim.MaxToolCalls turns, the last of which only
// offers terminal tools. It returns the exit reason, the terminal call (if
// any) and a trace of tool runs. Only provider failures yield a non-nil error.
func Run(
	ctx context.Context, p Provider, system, brief string, tools []Tool, lim Limits,
) (Outcome, []TraceEntry, error) {
	var (
		out      Outcome
		trace    []TraceEntry
		byName   = map[string]Tool{}
		terminal []ToolDef
		all      = make([]ToolDef, 0, len(tools))
		msgs     = []Message{{Role: RoleUser, Text: brief}}
	)
	for _, t := range tools {
		d := t.Def()
		byName[d.Name] = t
		all = append(all, d)
		if t.Terminal() {
			terminal = append(terminal, d)
		}
	}

	observe := func(Turn) {}
	if lim.Observer != nil {
		observe = lim.Observer
	}
	nudged := false
	var lastIn, growth int
	for turn := 1; ; turn++ {
		used := out.InputTokens + out.OutputTokens
		if lim.MaxTokens > 0 && used > lim.MaxTokens {
			out.Exit = ExitTokenCap
			observe(Turn{N: turn, Exit: out.Exit})
			return out, trace, nil
		}
		final := turn >= lim.MaxToolCalls
		// Soft budget: if the next turn is projected to cross the cap (input
		// keeps growing by about what it grew last turn), make this the forced
		// final turn instead of running out of budget mid-investigation.
		if lim.MaxTokens > 0 && used+growth+turnMaxTokens > lim.MaxTokens {
			final = true
		}
		req := TurnRequest{System: system, Messages: msgs, Tools: all, Choice: ChoiceAuto, MaxTokens: turnMaxTokens}
		if final {
			req.Tools, req.Choice = terminal, ChoiceFinal
		}

		resp, err := p.Turn(ctx, req)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
				out.Exit = ExitDeadline
			case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
				out.Exit = ExitCancelled
			default:
				out.Exit = ExitProviderError
				observe(Turn{N: turn, Request: req, Exit: out.Exit, Err: err})
				return out, trace, err
			}
			observe(Turn{N: turn, Request: req, Exit: out.Exit, Err: err})
			return out, trace, nil
		}
		growth = max(resp.InputTokens-lastIn, 0)
		lastIn = resp.InputTokens
		out.InputTokens += resp.InputTokens
		out.OutputTokens += resp.OutputTokens

		call := resp.Message.ToolCall
		if call == nil {
			if final || nudged {
				out.Exit = ExitNoToolCall
				observe(Turn{N: turn, Request: req, Response: resp, Exit: out.Exit})
				return out, trace, nil
			}
			// One chance to recover from a text-only reply; the nudge costs a turn.
			nudged = true
			asst := resp.Message
			if asst.Text == "" {
				asst.Text = "(no response)" // providers reject empty assistant messages
			}
			msgs = append(msgs, asst, Message{Role: RoleUser, Text: NudgeText})
			observe(Turn{N: turn, Request: req, Response: resp, Nudged: true})
			continue
		}
		if t, ok := byName[call.Name]; ok && t.Terminal() {
			out.Final = call
			out.Exit = ExitTerminal
			if final {
				out.Exit = ExitForcedFinal
			}
			observe(Turn{N: turn, Request: req, Response: resp, Exit: out.Exit})
			return out, trace, nil
		}
		if final { // forced turn must end in a terminal call
			out.Exit = ExitNoToolCall
			observe(Turn{N: turn, Request: req, Response: resp, Exit: out.Exit})
			return out, trace, nil
		}

		res := ToolResult{CallID: call.ID}
		start := time.Now()
		t, ok := byName[call.Name]
		if !ok {
			res.Content, res.IsError = fmt.Sprintf("unknown tool %q", call.Name), true
		} else if s, err := t.Run(ctx, call.Input); err != nil {
			res.Content, res.IsError = err.Error(), true
		} else {
			res.Content = s
		}
		res.Content = WrapData(call.Name, Truncate(res.Content, maxToolResultBytes))
		trace = append(trace, TraceEntry{
			Tool:        call.Name,
			Args:        Truncate(string(call.Input), maxArgsBytes),
			ResultBytes: len(res.Content),
			Latency:     time.Since(start),
		})
		msgs = append(msgs, resp.Message, Message{Role: RoleTool, ToolResult: &res})
		observe(Turn{N: turn, Request: req, Response: resp, ToolResult: &res})
	}
}

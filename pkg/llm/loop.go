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
	turnMaxTokens      = 2048
)

// Limits bounds the loop. The deadline comes from the context.
type Limits struct {
	MaxToolCalls int
	MaxTokens    int
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

	for turn := 1; ; turn++ {
		if lim.MaxTokens > 0 && out.InputTokens+out.OutputTokens > lim.MaxTokens {
			out.Exit = ExitTokenCap
			return out, trace, nil
		}
		final := turn >= lim.MaxToolCalls
		req := TurnRequest{System: system, Messages: msgs, Tools: all, Choice: ChoiceAuto, MaxTokens: turnMaxTokens}
		if final {
			req.Tools, req.Choice = terminal, ChoiceFinal
		}

		resp, err := p.Turn(ctx, req)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
				out.Exit = ExitDeadline
				return out, trace, nil
			case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
				out.Exit = ExitCancelled
				return out, trace, nil
			}
			out.Exit = ExitProviderError
			return out, trace, err
		}
		out.InputTokens += resp.InputTokens
		out.OutputTokens += resp.OutputTokens

		call := resp.Message.ToolCall
		if call == nil {
			out.Exit = ExitNoToolCall
			return out, trace, nil
		}
		if t, ok := byName[call.Name]; ok && t.Terminal() {
			out.Final = call
			out.Exit = ExitTerminal
			if final {
				out.Exit = ExitForcedFinal
			}
			return out, trace, nil
		}
		if final { // forced turn must end in a terminal call
			out.Exit = ExitNoToolCall
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
	}
}

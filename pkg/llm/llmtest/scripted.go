// Package llmtest provides a scripted llm.Provider for tests.
package llmtest

import (
	"context"
	"errors"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

// Scripted replays Turns (or Errs, when set at the same index) in order and
// records every request in Calls. Delay is slept before each reply and is
// interrupted by ctx.
type Scripted struct {
	Turns []llm.TurnResponse
	Errs  []error
	Delay time.Duration
	Calls []llm.TurnRequest
}

func (s *Scripted) Name() string  { return "scripted" }
func (s *Scripted) Model() string { return "scripted-1" }

// Turn implements llm.Provider.
func (s *Scripted) Turn(ctx context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	i := len(s.Calls)
	s.Calls = append(s.Calls, req)
	if s.Delay > 0 {
		select {
		case <-time.After(s.Delay):
		case <-ctx.Done():
			return llm.TurnResponse{}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return llm.TurnResponse{}, err
	}
	if i < len(s.Errs) && s.Errs[i] != nil {
		return llm.TurnResponse{}, s.Errs[i]
	}
	if i >= len(s.Turns) {
		return llm.TurnResponse{}, errors.New("llmtest: script exhausted")
	}
	return s.Turns[i], nil
}

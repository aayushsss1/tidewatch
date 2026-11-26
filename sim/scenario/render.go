package scenario

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"
)

// RenderNote expands the template functions in a note's text against epoch, the absolute
// time that offset 0 maps to:
//
//	{{at "75h"}}       -> "03:00 UTC"              (epoch + offset, HH:MM in UTC)
//	{{rfc3339 "75h"}}  -> "2026-01-04T03:00:00Z"   (epoch + offset, RFC 3339 in UTC)
//
// Offsets are Go durations, like every other offset in a scenario file. Rendering happens
// once, when the note is published, and every configuration receives the same text.
func RenderNote(text string, epoch time.Time) (string, error) {
	if !strings.Contains(text, "{{") {
		return text, nil
	}
	offset := func(s string) (time.Time, error) {
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, fmt.Errorf("bad offset %q: %w", s, err)
		}
		return epoch.Add(d).UTC(), nil
	}
	t, err := template.New("note").Option("missingkey=error").Funcs(template.FuncMap{
		"at": func(s string) (string, error) {
			at, err := offset(s)
			return at.Format("15:04") + " UTC", err
		},
		"rfc3339": func(s string) (string, error) {
			at, err := offset(s)
			return at.Format(time.RFC3339), err
		},
	}).Parse(text)
	if err != nil {
		return "", fmt.Errorf("note template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, nil); err != nil {
		return "", fmt.Errorf("note template: %w", err)
	}
	return buf.String(), nil
}

// Command kubectl-tidewatch is the kubectl plugin humans use to approve or
// reject the agent's proposals in Manual mode.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const usage = `usage:
  kubectl tidewatch approve <incident-id> [-n namespace | -A] [--as-user name]
  kubectl tidewatch reject  <incident-id> [-n namespace | -A] [--as-user name]
  kubectl tidewatch pending [-n namespace | -A]
`

// errNoMatch is returned when no TideWorkload has the requested pending approval.
var errNoMatch = errors.New("no TideWorkload has that pending approval")

// exitError carries a process exit code.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := errors.AsType[*exitError](err); ok {
		return ee.code
	}
	return 1
}

type runFunc func(ctx context.Context, c client.Client, out io.Writer, id, ns string, all bool, user string) error

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	cmd := args[0]
	var fn runFunc
	switch cmd {
	case "approve":
		fn = runApprove
	case "reject":
		fn = runReject
	case "pending":
		fn = runPending
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n%s", cmd, usage)
		return 2
	}

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var ns, user string
	var all bool
	defNS := currentNamespace()
	fs.StringVar(&ns, "namespace", defNS, "namespace to search (default: current context)")
	fs.StringVar(&ns, "n", defNS, "namespace to search (shorthand)")
	fs.BoolVar(&all, "A", false, "search all namespaces")
	fs.BoolVar(&all, "all-namespaces", false, "search all namespaces")
	fs.StringVar(&user, "as-user", defaultUser(), "user recorded in the annotation")

	// Accept the incident id before or after the flags.
	var id string
	rest := args[1:]
	if len(rest) > 0 && len(rest[0]) > 0 && rest[0][0] != '-' {
		id, rest = rest[0], rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	if id == "" && fs.NArg() > 0 {
		id = fs.Arg(0)
	}
	if cmd != "pending" && id == "" {
		_, _ = fmt.Fprintf(stderr, "%s needs an incident id\n%s", cmd, usage)
		return 2
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "error: load kubeconfig:", err)
		return 1
	}
	c, err := client.New(cfg, client.Options{Scheme: newScheme()})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	if err := fn(ctx, c, stdout, id, ns, all, user); err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return exitCode(err)
	}
	return 0
}

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilMust(clientgoscheme.AddToScheme(s))
	utilMust(v1.AddToScheme(s))
	return s
}

func utilMust(err error) {
	if err != nil {
		panic(err)
	}
}

// currentNamespace returns the namespace of the current kubeconfig context.
func currentNamespace() string {
	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

func defaultUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "kubectl"
}

func runApprove(ctx context.Context, c client.Client, out io.Writer, id, ns string, all bool, user string) error {
	return annotate(ctx, c, out, id, ns, all, user, v1.AnnoApprove, "approved")
}

func runReject(ctx context.Context, c client.Client, out io.Writer, id, ns string, all bool, user string) error {
	return annotate(ctx, c, out, id, ns, all, user, v1.AnnoReject, "rejected")
}

func listWorkloads(ctx context.Context, c client.Client, ns string, all bool) ([]v1.TideWorkload, error) {
	var list v1.TideWorkloadList
	var opts []client.ListOption
	if !all {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("list TideWorkloads: %w", err)
	}
	return list.Items, nil
}

func annotate(
	ctx context.Context, c client.Client, out io.Writer, id, ns string, all bool, user, anno, verb string,
) error {
	if user == "" {
		user = defaultUser()
	}
	ws, err := listWorkloads(ctx, c, ns, all)
	if err != nil {
		return err
	}
	n := 0
	for i := range ws {
		w := &ws[i]
		if !hasPending(w, id) {
			continue
		}
		base := w.DeepCopy()
		if w.Annotations == nil {
			w.Annotations = map[string]string{}
		}
		w.Annotations[anno] = id + "@" + user
		if err := c.Patch(ctx, w, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("annotate %s/%s: %w", w.Namespace, w.Name, err)
		}
		_, _ = fmt.Fprintf(out, "%s %s/%s (incident %s)\n", verb, w.Namespace, w.Name, id)
		n++
	}
	if n == 0 {
		return &exitError{code: 1, err: fmt.Errorf("%w: %q (try -A to search all namespaces)", errNoMatch, id)}
	}
	return nil
}

func hasPending(w *v1.TideWorkload, id string) bool {
	for _, p := range w.Status.PendingApprovals {
		if p.IncidentID == id {
			return true
		}
	}
	return false
}

// runPending lists pending approvals. id and user are unused.
func runPending(ctx context.Context, c client.Client, out io.Writer, _, ns string, all bool, _ string) error {
	ws, err := listWorkloads(ctx, c, ns, all)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAMESPACE\tWORKLOAD\tINCIDENT\tMIN-REPLICAS\tCONFIDENCE\tEXPIRES\tREASON")
	for _, w := range ws {
		for _, p := range w.Status.PendingApprovals {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", w.Namespace, w.Name, p.IncidentID,
				p.Override.MinReplicas, p.Confidence, p.Expires.UTC().Format(time.RFC3339), p.Reason)
		}
	}
	return tw.Flush()
}

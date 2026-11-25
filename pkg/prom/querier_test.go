package prom

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func matrix(series ...string) string {
	return fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, strings.Join(series, ","))
}

func serve(t *testing.T, status int, body string) Querier {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query_range", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	q, err := New(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestRangeSeries_AlignsAndFillsMissingWithNaN(t *testing.T) {
	s := fmt.Sprintf(`{"metric":{},"values":[[%d,"1"],[%d,"3"]]}`, t0.Unix(), t0.Unix()+2)
	q := serve(t, 200, matrix(s))
	got, err := q.RangeSeries(context.Background(), "up", t0, t0.Add(2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Points) != 3 || got.Step != time.Second {
		t.Fatalf("got %+v", got)
	}
	if got.Points[0].V != 1 || !math.IsNaN(got.Points[1].V) || got.Points[2].V != 3 {
		t.Fatalf("got %+v", got.Points)
	}
	if !got.Points[1].T.Equal(t0.Add(time.Second)) {
		t.Fatalf("bad slot time %v", got.Points[1].T)
	}
}

func TestRangeSeries_ZeroSeriesIsAllNaN(t *testing.T) {
	q := serve(t, 200, matrix())
	got, err := q.RangeSeries(context.Background(), "up", t0, t0.Add(4*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Points) != 5 {
		t.Fatalf("len=%d", len(got.Points))
	}
	for _, p := range got.Points {
		if !math.IsNaN(p.V) {
			t.Fatalf("expected NaN, got %v", p.V)
		}
	}
}

func TestRangeSeries_MultipleSeriesErrors(t *testing.T) {
	a := fmt.Sprintf(`{"metric":{"a":"1"},"values":[[%d,"1"]]}`, t0.Unix())
	b := fmt.Sprintf(`{"metric":{"a":"2"},"values":[[%d,"1"]]}`, t0.Unix())
	q := serve(t, 200, matrix(a, b))
	_, err := q.RangeSeries(context.Background(), "up", t0, t0.Add(time.Second), time.Second)
	if !errors.Is(err, ErrMultipleSeries) || !strings.Contains(err.Error(), "got 2") {
		t.Fatalf("err=%v", err)
	}
}

func TestRangeSeries_HTTPErrorPropagates(t *testing.T) {
	q := serve(t, 500, `{"status":"error","errorType":"internal","error":"boom"}`)
	if _, err := q.RangeSeries(context.Background(), "up", t0, t0.Add(time.Second), time.Second); err == nil {
		t.Fatal("expected error")
	}
}

func TestReady(t *testing.T) {
	if err := serve(t, 200, "").Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := serve(t, 503, "").Ready(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRangeSeries_InfSampleIsNaN(t *testing.T) {
	s := fmt.Sprintf(`{"metric":{},"values":[[%d,"1"],[%d,"+Inf"],[%d,"NaN"]]}`, t0.Unix(), t0.Unix()+1, t0.Unix()+2)
	q := serve(t, 200, matrix(s))
	got, err := q.RangeSeries(context.Background(), "up", t0, t0.Add(2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Points[0].V != 1 || !math.IsNaN(got.Points[1].V) || !math.IsNaN(got.Points[2].V) {
		t.Fatalf("got %+v", got.Points)
	}
}

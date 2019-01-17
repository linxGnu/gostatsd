package statsd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ash2k/stager"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/atlassian/gostatsd"
)

func waitForDone(t *testing.T, ctx context.Context, ch <-chan error) {
	select {
	case <-ctx.Done():
		require.Fail(t, "timed out")
	case err := <-ch:
		require.NoError(t, err)
	}
}

func TestHttpHandlerMetrics(t *testing.T) {
	t.Parallel()

	ctxTest, testDone := testContext(t)
	defer testDone()

	chDone := make(chan error)
	hf := func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(200)
		resp.Write([]byte("{}"))
		chDone <- nil
	}
	h := http.Handler(http.HandlerFunc(hf))
	c := httptest.NewServer(h)

	hh, err := NewHttpHandler(
		logrus.StandardLogger(),
		c.URL,
		"tcp",
		1000,
		10,
		true,
		false,
		1*time.Second,
		1*time.Second,
		200*time.Millisecond,
	)
	require.NoError(t, err)

	stgr := stager.New()
	stgr.NextStageWithContext(ctxTest).StartWithContext(hh.Run)

	hh.DispatchMetric(ctxTest, &gostatsd.Metric{})

	waitForDone(t, ctxTest, chDone)
	stgr.Shutdown()
	c.Close()
}

func BenchmarkHttpForwarderTranslateAll(b *testing.B) {
	metrics := []*gostatsd.Metric{}

	for i := 0; i < b.N; i++ {
		metrics = append(metrics, &gostatsd.Metric{
			Name:        "bench.metric",
			Value:       123.456,
			StringValue: "123.456",
			Tags:        gostatsd.Tags{"tag1", "tag2"},
			Hostname:    "hostname",
			SourceIP:    "sourceip",
			Type:        gostatsd.MetricType(i % 4), // Use all types
		})
	}

	b.ResetTimer()
	translateToProtobuf(metrics)
}

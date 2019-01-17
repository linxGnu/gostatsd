package gostatsd

import (
	"context"
	"sync"
	"time"

	"github.com/tilinna/clock"
)

// metricConsolidator collects many metrics in a single buffer, and dispatches them when the slice fills up.  The
// purpose is to allow for consolidation of metrics from multiple goroutines in to one worker.  If Run is started
// a flush will occur periodically to protect against metrics not being emitted fast enough to trigger a flush in
// a timely fashion.
type metricConsolidator struct {
	flushInterval time.Duration
	sink          chan<- []*Metric

	// mu is assumed to be held while flushing, both to create back pressure and because flushing resets the slice.
	mu      sync.Mutex
	metrics []*Metric
}

// NewMetricConsolidator makes a new metricConsolidator which consolidates metrics to a slice and dispatches them to
// the provided channel.
func NewMetricConsolidator(metricCount uint, flushInterval time.Duration, sink chan<- []*Metric) *metricConsolidator {
	return &metricConsolidator{
		flushInterval: flushInterval,
		sink:          sink,

		metrics: make([]*Metric, 0, metricCount),
	}
}

// DispatchMetric collects any Metrics submitted to it, and flushes them to the channel if enough have been
// submitted.  Will propagate backpressure from the sink channel to the caller.  Thread safe.
func (mc *metricConsolidator) DispatchMetric(ctx context.Context, m *Metric) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics = append(mc.metrics, m)
	if len(mc.metrics) == cap(mc.metrics) {
		mc.flush(ctx)
	}
}

// Run is a long running function that periodically triggers a flush.
func (mc *metricConsolidator) Run(ctx context.Context) {
	t := clock.NewTicker(ctx, mc.flushInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			mc.mu.Lock()
			if len(mc.metrics) > 0 {
				mc.flush(ctx)
			}
			mc.mu.Unlock()
		}
	}
}

// flush will dispatch the current Metric buffer to the processor and prepare a new buffer.  Blocks if the
// channel is full.  Not thread safe.
func (mc *metricConsolidator) flush(ctx context.Context) {
	select {
	case mc.sink <- mc.metrics:
	case <-ctx.Done():
	}
	// We can't reuse the buffer, because we no longer own it.  Flushing should
	// be infrequent enough that we don't need to mitigate this.
	mc.metrics = make([]*Metric, 0, cap(mc.metrics))
}

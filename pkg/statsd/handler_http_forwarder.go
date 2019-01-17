package statsd

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"

	"github.com/ash2k/stager/wait"
	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultConsolidatorFlushInterval = 1 * time.Second
	defaultClientTimeout             = 10 * time.Second
	defaultCompress                  = true
	defaultEnableHttp2               = false
	defaultEndpoint                  = ""
	defaultMaxRequestElapsedTime     = 30 * time.Second
	defaultMaxRequests               = 1000
	defaultNetwork                   = "tcp"
	defaultMetricsPerBatch           = 1000
)

// HttpHandler is a PipelineHandler and EventHandler which sends metrics to another gostatsd instance
type HttpHandler struct {
	batchId               uint64
	eventId               uint64
	logger                logrus.FieldLogger
	apiEndpoint           string
	maxRequestElapsedTime time.Duration
	metricsSem            chan struct{}
	client                http.Client
	compress              bool
	consolidator          gostatsd.RawMetricHandler
	consolidatedMetrics   <-chan []*gostatsd.Metric
	eventWg               sync.WaitGroup
}

// NewHttpHandlerFromViper returns a new http API client.
func NewHttpHandlerFromViper(logger logrus.FieldLogger, v *viper.Viper) (*HttpHandler, error) {
	subViper := getSubViper(v, "http-transport")
	subViper.SetDefault("client-timeout", defaultClientTimeout)
	subViper.SetDefault("compress", defaultCompress)
	subViper.SetDefault("enable-http2", defaultEnableHttp2)
	subViper.SetDefault("endpoint", defaultEndpoint)
	subViper.SetDefault("max-requests", defaultMaxRequests)
	subViper.SetDefault("max-request-elapsed-time", defaultMaxRequestElapsedTime)
	subViper.SetDefault("metrics-per-batch", defaultMetricsPerBatch)
	subViper.SetDefault("flush-interval", defaultConsolidatorFlushInterval)
	subViper.SetDefault("network", defaultNetwork)

	return NewHttpHandler(
		logger,
		subViper.GetString("api-endpoint"),
		subViper.GetString("network"),
		uint(subViper.GetInt("metrics-per-batch")),
		uint(subViper.GetInt("max-requests")),
		subViper.GetBool("compress"),
		subViper.GetBool("enable-http2"),
		subViper.GetDuration("client-timeout"),
		subViper.GetDuration("max-request-elapsed-time"),
		subViper.GetDuration("flush-interval"),
	)
}

// NewHttpHandler returns a new handler which dispatches metrics over http to another gostatsd server.
func NewHttpHandler(logger logrus.FieldLogger, apiEndpoint, network string, metricsPerBatch, maxRequests uint, compress, enableHttp2 bool, clientTimeout, maxRequestElapsedTime time.Duration, flushInterval time.Duration) (*HttpHandler, error) {
	if apiEndpoint == "" {
		return nil, fmt.Errorf("api-endpoint is required")
	}
	if metricsPerBatch <= 0 {
		return nil, fmt.Errorf("metrics-per-batch must be positive")
	}
	if maxRequests <= 0 {
		return nil, fmt.Errorf("max-requests must be positive")
	}
	if clientTimeout <= 0 {
		return nil, fmt.Errorf("client-timeout must be positive")
	}
	if maxRequestElapsedTime <= 0 {
		return nil, fmt.Errorf("max-request-elapsed-time must be positive")
	}
	if flushInterval <= 0 {
		return nil, fmt.Errorf("max-request-elapsed-time must be positive")
	}

	logger.WithFields(logrus.Fields{
		"api-endpoint":             apiEndpoint,
		"client-timeout":           clientTimeout,
		"compress":                 compress,
		"enable-http2":             enableHttp2,
		"max-request-elapsed-time": maxRequestElapsedTime,
		"max-requests":             maxRequests,
		"metrics-per-batch":        metricsPerBatch,
		"network":                  network,
		"flush-interval":           flushInterval,
	}).Info("Created HttpHandler")

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 3 * time.Second,
		TLSClientConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
		DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			// replace the network with our own
			return dialer.DialContext(ctx, network, address)
		},
		MaxIdleConns:    50,
		IdleConnTimeout: 1 * time.Minute,
	}
	if !enableHttp2 {
		// A non-nil empty map used in TLSNextProto to disable HTTP/2 support in client.
		// https://golang.org/doc/go1.6#http2
		transport.TLSNextProto = map[string](func(string, *tls.Conn) http.RoundTripper){}
	}

	metricsSem := make(chan struct{}, maxRequests)
	for i := uint(0); i < maxRequests; i++ {
		metricsSem <- struct{}{}
	}

	ch := make(chan []*gostatsd.Metric)

	return &HttpHandler{
		logger:                logger.WithField("component", "http-handler"),
		apiEndpoint:           apiEndpoint,
		maxRequestElapsedTime: maxRequestElapsedTime,
		metricsSem:            metricsSem,
		compress:              compress,
		consolidator:          gostatsd.NewMetricConsolidator(metricsPerBatch, flushInterval, ch),
		consolidatedMetrics:   ch,
		client: http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
	}, nil
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

func (hh *HttpHandler) EstimatedTags() int {
	return 0
}

func (hh *HttpHandler) DispatchMetric(ctx context.Context, m *gostatsd.Metric) {
	hh.consolidator.DispatchMetric(ctx, m)
}

func (hh *HttpHandler) Run(ctx context.Context) {
	wg := wait.Group{}
	defer wg.Wait()
	if r, ok := hh.consolidator.(gostatsd.Runner); ok {
		wg.StartWithContext(ctx, r.Run)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case metrics := <-hh.consolidatedMetrics:
			if !hh.acquireSem(ctx) {
				return
			}
			go func(batchId uint64) {
				hh.postMetrics(ctx, metrics, batchId)
				hh.releaseSem()
			}(hh.batchId)
			hh.batchId++
		}
	}
}

func (hh *HttpHandler) acquireSem(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-hh.metricsSem:
		return true
	}
}

func (hh *HttpHandler) releaseSem() {
	hh.metricsSem <- struct{}{} // will never block
}

func translateToProtobuf(metrics []*gostatsd.Metric) []*pb.RawMetricV1 {
	pbMetrics := make([]*pb.RawMetricV1, len(metrics))

	for idx, metric := range metrics {
		pbm := &pb.RawMetricV1{
			Name:      metric.Name,
			Hostname:  metric.Hostname,
			Timestamp: 0,
			Tags:      metric.Tags,
		}
		pbMetrics[idx] = pbm
		switch metric.Type {
		case gostatsd.COUNTER:
			pbm.M = &pb.RawMetricV1_Counter{
				Counter: &pb.RawCounterV1{
					Value: int64(metric.Value),
				},
			}
		case gostatsd.GAUGE:
			pbm.M = &pb.RawMetricV1_Gauge{
				Gauge: &pb.RawGaugeV1{
					Value: metric.Value,
				},
			}
		case gostatsd.SET:
			pbm.M = &pb.RawMetricV1_Set{
				Set: &pb.RawSetV1{
					Value: metric.StringValue,
				},
			}
		case gostatsd.TIMER:
			pbm.M = &pb.RawMetricV1_Timer{
				Timer: &pb.RawTimerV1{
					Value: metric.Value,
				},
			}
		}
	}

	return pbMetrics
}

func (hh *HttpHandler) postMetrics(ctx context.Context, metrics []*gostatsd.Metric, batchId uint64) {
	message := &pb.RawMessageV1{
		RawMetrics: translateToProtobuf(metrics),
	}
	hh.post(ctx, message, batchId, "metrics", "/v1/raw")
}

func (hh *HttpHandler) post(ctx context.Context, message proto.Message, id uint64, endpointType, endpoint string) {
	logger := hh.logger.WithFields(logrus.Fields{
		"id":   id,
		"type": endpointType,
	})

	post, err := hh.constructPost(ctx, hh.apiEndpoint+endpoint, message)
	if err != nil {
		logger.WithError(err).Error("Failed to create post request")
		return
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = hh.maxRequestElapsedTime

	for {
		if err = post(); err == nil {
			return
		}

		next := b.NextBackOff()
		if next == backoff.Stop {
			logger.WithError(err).Error("Failed to send, giving up")
			return
		}

		logger.WithError(err).Warn("Failed to send, retrying")

		timer := time.NewTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (hh *HttpHandler) serialize(message proto.Message) ([]byte, error) {
	buf, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// debug rendering
/*
func (hh *HttpHandler) serializeText(message proto.Message) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := proto.MarshalText(buf, message)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
*/

func (hh *HttpHandler) serializeAndCompress(message proto.Message) ([]byte, error) {
	raw, err := hh.serialize(message)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	compressor, err := zlib.NewWriterLevel(buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}

	compressor.Write(raw)
	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (hh *HttpHandler) constructPost(ctx context.Context, path string, message proto.Message) (func() error /*doPost*/, error) {
	var body []byte
	var err error
	var encoding string

	if hh.compress {
		body, err = hh.serializeAndCompress(message)
		encoding = "deflate"
	} else {
		body, err = hh.serialize(message)
		encoding = "identity"
	}

	if err != nil {
		return nil, err
	}

	return func() error {
		headers := map[string]string{
			"Content-Type":     "application/x-protobuf",
			"Content-Encoding": encoding,
			"User-Agent":       "gostatsd (http forwarder)",
		}
		req, err := http.NewRequest("POST", path, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("unable to create http.Request: %v", err)
		}
		req = req.WithContext(ctx)
		for header, v := range headers {
			req.Header.Set(header, v)
		}
		resp, err := hh.client.Do(req)
		if err != nil {
			return fmt.Errorf("error POSTing: %v", err)
		}
		defer resp.Body.Close()
		body := io.LimitReader(resp.Body, 512)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusNoContent {
			b, _ := ioutil.ReadAll(body)
			hh.logger.WithFields(logrus.Fields{
				"status": resp.StatusCode,
				"body":   string(b),
			}).Info("Failed request")
			return fmt.Errorf("received bad status code %d", resp.StatusCode)
		}
		_, _ = io.Copy(ioutil.Discard, body)
		return nil
	}, nil
}

///////// Event processing

// Events are handled individually, because the context matters. If they're buffered through the consolidator, they'll
// be processed on a goroutine with a context which will be closed during shutdown.  Events should be rare enough that
// this isn't an issue.

func (hh *HttpHandler) DispatchEvent(ctx context.Context, e *gostatsd.Event) {
	hh.eventWg.Add(1)
	go hh.dispatchEvent(ctx, e)
}

func (hh *HttpHandler) dispatchEvent(ctx context.Context, e *gostatsd.Event) {
	eventId := atomic.AddUint64(&hh.eventId, 1) - 1

	message := &pb.EventV1{
		Title:          e.Title,
		Text:           e.Text,
		DateHappened:   e.DateHappened,
		Hostname:       e.Hostname,
		AggregationKey: e.AggregationKey,
		SourceTypeName: e.SourceTypeName,
		Tags:           e.Tags,
		SourceIP:       string(e.SourceIP),
	}

	switch e.Priority {
	case gostatsd.PriNormal:
		message.Priority = pb.EventV1_Normal
	case gostatsd.PriLow:
		message.Priority = pb.EventV1_Low
	}

	switch e.AlertType {
	case gostatsd.AlertInfo:
		message.Type = pb.EventV1_Info
	case gostatsd.AlertWarning:
		message.Type = pb.EventV1_Warning
	case gostatsd.AlertError:
		message.Type = pb.EventV1_Error
	case gostatsd.AlertSuccess:
		message.Type = pb.EventV1_Success
	}

	hh.post(ctx, message, eventId, "event", "/v1/event")

	defer hh.eventWg.Done()
}

func (hh *HttpHandler) WaitForEvents() {
	hh.eventWg.Wait()
}

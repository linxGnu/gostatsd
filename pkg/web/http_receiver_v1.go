package web

import (
	"bytes"
	"compress/zlib"
	"context"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pb"

	"github.com/atlassian/gostatsd/pkg/pool"
	"github.com/atlassian/gostatsd/pkg/stats"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

type rawHttpHandler struct {
	requestSuccess           uint64
	requestFailureRead       uint64
	requestFailureDecompress uint64
	requestFailureEncoding   uint64
	requestFailureUnmarshal  uint64
	metricsProcessed         uint64

	logger     logrus.FieldLogger
	handler    gostatsd.PipelineHandler
	serverName string
	pool       *pool.MetricPool
}

func newRawHttpHandlerV1(logger logrus.FieldLogger, serverName string, handler gostatsd.PipelineHandler) *rawHttpHandler {
	return &rawHttpHandler{
		logger:     logger,
		handler:    handler,
		serverName: serverName,
		pool:       pool.NewMetricPool(0), // tags will already be a slice, so we don't need to pre-allocate.
	}
}

func (rhh *rawHttpHandler) RunMetrics(ctx context.Context) {
	statser := stats.FromContext(ctx).WithTags([]string{"server-name:" + rhh.serverName})

	notify, cancel := statser.RegisterFlush()
	defer cancel()

	for {
		select {
		case <-notify:
			rhh.emitMetrics(statser)
		case <-ctx.Done():
			return
		}
	}
}

func (rhh *rawHttpHandler) emitMetrics(statser stats.Statser) {
	requestSuccess := atomic.SwapUint64(&rhh.requestSuccess, 0)
	requestFailureRead := atomic.SwapUint64(&rhh.requestFailureRead, 0)
	requestFailureDecompress := atomic.SwapUint64(&rhh.requestFailureDecompress, 0)
	requestFailureEncoding := atomic.SwapUint64(&rhh.requestFailureEncoding, 0)
	requestFailureUnmarshal := atomic.SwapUint64(&rhh.requestFailureUnmarshal, 0)
	metricsProcessed := atomic.SwapUint64(&rhh.metricsProcessed, 0)

	statser.Count("http.request", float64(requestSuccess), []string{"result:success"})
	statser.Count("http.request", float64(requestFailureRead), []string{"result:failure", "failure:read"})
	statser.Count("http.request", float64(requestFailureDecompress), []string{"result:failure", "failure:decompress"})
	statser.Count("http.request", float64(requestFailureEncoding), []string{"result:failure", "failure:encoding"})
	statser.Count("http.request", float64(requestFailureUnmarshal), []string{"result:failure", "failure:unmarshal"})
	statser.Count("http.metrics", float64(metricsProcessed), nil)
}

func (rhh *rawHttpHandler) RawHandler(w http.ResponseWriter, req *http.Request) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureRead, 1)
		rhh.logger.WithError(err).Info("failed reading body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req.Body.Close()

	encoding := req.Header.Get("Content-Encoding")
	switch encoding {
	case "deflate":
		b, err = decompress(b)
		if err != nil {
			atomic.AddUint64(&rhh.requestFailureDecompress, 1)
			rhh.logger.WithError(err).Info("failed decompressing body")
			w.WriteHeader(http.StatusBadRequest) // blame the client
			return
		}
	case "identity", "":
		// no action
	default:
		atomic.AddUint64(&rhh.requestFailureEncoding, 1)
		rhh.logger.Info("invalid encoding")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var msg pb.RawMessageV1
	err = proto.Unmarshal(b, &msg)
	if err != nil {
		atomic.AddUint64(&rhh.requestFailureUnmarshal, 1)
		rhh.logger.WithError(err).Error("failed to unmarshal")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for _, metric := range msg.RawMetrics {
		rm := rhh.pool.Get()

		rm.Name = metric.Name
		rm.Tags = metric.Tags
		rm.Hostname = metric.Hostname

		switch m := metric.M.(type) {
		case *pb.RawMetricV1_Counter:
			rm.Value = float64(m.Counter.Value)
			rm.Type = gostatsd.COUNTER
		case *pb.RawMetricV1_Gauge:
			rm.Value = m.Gauge.Value
			rm.Type = gostatsd.GAUGE
		case *pb.RawMetricV1_Set:
			rm.StringValue = m.Set.Value
			rm.Type = gostatsd.SET
		case *pb.RawMetricV1_Timer:
			rm.Value = m.Timer.Value
			rm.Type = gostatsd.TIMER
		default:
			continue
		}

		rhh.handler.DispatchMetric(req.Context(), rm)
	}

	atomic.AddUint64(&rhh.metricsProcessed, uint64(len(msg.RawMetrics)))
	atomic.AddUint64(&rhh.requestSuccess, 1)
	w.WriteHeader(http.StatusAccepted)
}

func decompress(input []byte) ([]byte, error) {
	decompressor, err := zlib.NewReader(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	defer decompressor.Close()

	var out bytes.Buffer
	if _, err = out.ReadFrom(decompressor); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

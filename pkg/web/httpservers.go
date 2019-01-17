package web

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type httpServer struct {
	logger  logrus.FieldLogger
	statser stats.Statser
	address string
	router  *mux.Router
}

func NewHttpServersFromViper(v *viper.Viper, logger logrus.FieldLogger, handler gostatsd.PipelineHandler) ([]gostatsd.Runnable, error) {
	httpServerNames := v.GetStringSlice("http-servers")
	httpServerList := make([]gostatsd.Runnable, 0, len(httpServerNames))
	for _, httpServerName := range httpServerNames {
		httpServer, err := newHttpServerFromViper(logger, v, httpServerName, handler)
		if err != nil {
			return nil, fmt.Errorf("failed to make http-server %s: %v", httpServerName, err)
		}
		httpServerList = append(httpServerList, httpServer.Run)
	}
	return httpServerList, nil
}

func newHttpServerFromViper(
	logger logrus.FieldLogger,
	vMain *viper.Viper,
	serverName string,
	handler gostatsd.PipelineHandler,
) (*httpServer, error) {
	vSub := getSubViper(vMain, "http."+serverName)
	vSub.SetDefault("address", "127.0.0.1:8080")
	vSub.SetDefault("enable-prof", false)
	vSub.SetDefault("enable-expvar", false)
	vSub.SetDefault("enable-ingestion", false)
	vSub.SetDefault("enable-healthcheck", true)

	return newHttpServer(
		logger.WithField("http-server", serverName),
		handler,
		serverName,
		vSub.GetString("address"),
		vSub.GetBool("enable-prof"),
		vSub.GetBool("enable-expvar"),
		vSub.GetBool("enable-ingestion"),
		vSub.GetBool("enable-healthcheck"),
	)
}

type route struct {
	path    string
	handler http.HandlerFunc
	method  string
	name    string
}

func newHttpServer(
	logger logrus.FieldLogger,
	handler gostatsd.PipelineHandler,
	serverName, address string,
	enableProf,
	enableExpVar,
	enableIngestion,
	enableHealthcheck bool,
) (*httpServer, error) {
	var routes []route

	if enableProf {
		profiler := &traceProfiler{}
		routes = append(routes,
			route{path: "/memprof", handler: profiler.MemProf, method: "POST", name: "profmem_post"},
			route{path: "/pprof", handler: profiler.PProf, method: "POST", name: "profpprof_post"},
			route{path: "/trace", handler: profiler.Trace, method: "POST", name: "proftrace_post"},
		)
	}

	if enableExpVar {
		routes = append(routes,
			route{path: "/expvar", handler: expvar.Handler().ServeHTTP, method: "GET", name: "expvar_get"},
		)
	}

	if enableIngestion {
		raw := newRawHttpHandlerV1(logger, serverName, handler)
		routes = append(routes,
			route{path: "/v1/raw", handler: raw.RawHandler, method: "POST", name: "raw_post"},
		)
	}

	if enableHealthcheck {
		hc := &healthChecker{logger}
		routes = append(routes,
			route{path: "/healthcheck", handler: hc.healthCheck, method: "GET", name: "healthcheck_get"},
			route{path: "/deepcheck", handler: hc.deepCheck, method: "GET", name: "deepcheck_get"},
		)
	}

	if len(routes) == 0 {
		return nil, fmt.Errorf("must enable at least one of prof, expvar, ingestion, or healthcheck")
	}

	router, err := createRoutes(routes)
	if err != nil {
		return nil, err
	}

	server := &httpServer{
		logger:  logger,
		statser: stats.NewNullStatser(), // Will be replaced in Run
		address: address,
		router:  router,
	}

	router.Use(server.logRequest)

	if err != nil {
		return nil, err
	}

	server.router = router

	logger.WithFields(logrus.Fields{
		"address":            address,
		"enable-pprof":       enableProf,
		"enable-expvar":      enableExpVar,
		"enable-ingestion":   enableIngestion,
		"enable-healthcheck": enableHealthcheck,
	}).Info("Created server")

	return server, nil
}

func createRoutes(routes []route) (*mux.Router, error) {
	router := mux.NewRouter()

	for _, route := range routes {
		r := router.HandleFunc(route.path, route.handler).Methods(route.method).Name(route.name)
		if err := r.GetError(); err != nil {
			return nil, fmt.Errorf("error creating route %s: %v", route.name, err)
		}
	}

	return router, nil
}

// TODO: This should probably log more than just the route.
func (hs *httpServer) logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var routeName string
		route := mux.CurrentRoute(req)
		if route == nil {
			routeName = "unknown"
		}
		hs.logger.WithFields(logrus.Fields{
			"route": routeName,
		}).Info("request")

		handler.ServeHTTP(w, req)
	})
}

func (hs *httpServer) Run(ctx context.Context) {
	hs.statser = stats.FromContext(ctx)

	server := &http.Server{
		Addr:    hs.address,
		Handler: hs.router,
	}

	chStopped := make(chan struct{}, 1)
	go hs.waitAndStop(ctx, server, chStopped)

	hs.logger.WithField("address", server.Addr).Info("Listening")

	err := server.ListenAndServe()
	if err != http.ErrServerClosed {
		hs.logger.WithError(err).Error("Web server failed")
		return
	}

	// Wait for graceful shutdown of existing connections

	select {
	case <-chStopped:
		// happy
	case <-time.After(6 * time.Second):
		hs.logger.Info("Timeout waiting for webserver to stop")
	}
}

// waitAndStop will gracefully shut down the Server when the Context passed is cancelled.  It signals
// on chStopped when it is done.  There is no guarantee that it will actually signal, if the server
// does not shutdown.
func (hs *httpServer) waitAndStop(ctx context.Context, server *http.Server, chStopped chan<- struct{}) {
	<-ctx.Done()

	hs.logger.Info("Shutting down web server")
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(timeoutCtx)
	if err != nil {
		hs.logger.WithError(err).Warn("Failed to stop web server")
	}
	chStopped <- struct{}{}
}

// TODO: Dedupe this
func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}

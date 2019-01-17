## HTTP endpoints

### `prof` endpoints
- `/memprof`, runs a trace profile for 30 seconds
- `/pprof`, runs a pprof profile for 30 seconds
- `/memprof`, runs a `heap` memory profile for 30 seconds

Only one prof will be allowed to run at any point, and requesting multiple will block until the previous has completed. 

### `expvar` endpoints
- `/expvar`, routes directly to the [expvar handler](https://golang.org/pkg/expvar/#Handler)

### `healthcheck` endpoints
- `/healthcheck`, reports if the server is internally healthy.  This is what should be used for health checking by an LB.
- `/deepcheck`, reports the status of downstream services.  This should not be used for system healthcheck, as a bad
  dependency should not cause an otherwise healthy server to cycle, because it will likely fail again. 

### `ingestion` endpoint
- `/vN/raw`, takes in protobuf formatted raw metrics.  This endpoint is intended for internal gostatsd communication.
  The compatibility guarantee is that gostatsd will accept **at most** N-1 and N.  A major application bump **may**
  remove handling of N-1.  Any such change will be documented in the [CHANGELOG.md]

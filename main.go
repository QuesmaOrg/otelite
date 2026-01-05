package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

var db *sql.DB

// Insert queue to serialize all database writes
type insertJob struct {
	jobType     string // "span" or "log"
	serviceName string
	data        map[string]interface{}
}

var insertQueue chan insertJob

// startInsertWorker starts a goroutine that processes all inserts sequentially
func startInsertWorker(ctx context.Context) {
	insertQueue = make(chan insertJob, 1000) // buffer for 1000 pending inserts
	go runInsertWorker(ctx)
}

func runInsertWorker(ctx context.Context) {
	log.Println("[insert-worker] started")
	for {
		select {
		case <-ctx.Done():
			log.Println("[insert-worker] context cancelled, draining queue...")
			// Drain remaining jobs before exiting
			for {
				select {
				case job := <-insertQueue:
					processInsertJob(job)
				default:
					log.Println("[insert-worker] stopped")
					return
				}
			}
		case job := <-insertQueue:
			processInsertJob(job)
		}
	}
}

func processInsertJob(job insertJob) {
	var err error
	switch job.jobType {
	case "span":
		err = doInsertSpan(job.serviceName, job.data)
	case "log":
		err = doInsertLog(job.serviceName, job.data)
	}
	if err != nil {
		log.Printf("[insert-worker] ERROR inserting %s for %s: %v", job.jobType, job.serviceName, err)
	} else {
		log.Printf("[insert-worker] inserted %s for %s", job.jobType, job.serviceName)
	}
}

func initDB(dbPath string) error {
	var err error
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS traces (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		trace_id TEXT,
		span_id TEXT,
		parent_span_id TEXT,
		service_name TEXT,
		span_name TEXT,
		kind INTEGER,
		start_time INTEGER,
		end_time INTEGER,
		status_code INTEGER,
		raw_json TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_trace_id ON traces(trace_id);
	CREATE INDEX IF NOT EXISTS idx_service_name ON traces(service_name);
	CREATE INDEX IF NOT EXISTS idx_timestamp ON traces(timestamp);

	CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		trace_id TEXT,
		span_id TEXT,
		service_name TEXT,
		severity_number INTEGER,
		severity_text TEXT,
		body TEXT,
		log_timestamp INTEGER,
		raw_json TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id);
	CREATE INDEX IF NOT EXISTS idx_logs_service_name ON logs(service_name);
	CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
	`

	_, err = db.Exec(schema)
	return err
}

func hexEncode(b []byte) string {
	return hex.EncodeToString(b)
}

func handleTraces(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	log.Printf("[%s] %s %s (Content-Type: %s)", r.RemoteAddr, r.Method, r.URL.Path, contentType)

	if r.Method != http.MethodPost {
		log.Printf("[%s] method not allowed: %s", r.RemoteAddr, r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	isJSON := strings.Contains(contentType, "json")
	isProtobuf := strings.Contains(contentType, "protobuf") || strings.Contains(contentType, "x-protobuf")

	if !isJSON && !isProtobuf {
		log.Printf("[%s] unsupported content type: %s", r.RemoteAddr, contentType)
		http.Error(w, "unsupported content type, expected application/json or application/x-protobuf", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[%s] failed to read body: %v", r.RemoteAddr, err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("[%s] received %d bytes", r.RemoteAddr, len(body))

	var count int
	if isProtobuf {
		count, err = handleProtobufTraces(r.RemoteAddr, body)
	} else {
		count, err = handleJSONTraces(r.RemoteAddr, body)
	}

	if err != nil {
		log.Printf("[%s] failed to process traces: %v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[%s] inserted %d spans", r.RemoteAddr, count)

	if isProtobuf {
		resp := &collectorpb.ExportTraceServiceResponse{}
		respBytes, _ := proto.Marshal(resp)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}
}

func handleJSONTraces(remoteAddr string, body []byte) (int, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("invalid JSON: %v", err)
	}

	count := 0
	resourceSpans, ok := payload["resourceSpans"].([]interface{})
	if !ok {
		log.Printf("[%s] no resourceSpans found", remoteAddr)
		return 0, nil
	}

	for _, rs := range resourceSpans {
		rsMap, ok := rs.(map[string]interface{})
		if !ok {
			continue
		}

		serviceName := extractServiceName(rsMap)

		scopeSpans, ok := rsMap["scopeSpans"].([]interface{})
		if !ok {
			continue
		}

		for _, ss := range scopeSpans {
			ssMap, ok := ss.(map[string]interface{})
			if !ok {
				continue
			}

			spans, ok := ssMap["spans"].([]interface{})
			if !ok {
				continue
			}

			for _, span := range spans {
				spanMap, ok := span.(map[string]interface{})
				if !ok {
					continue
				}

				if err := insertSpan(serviceName, spanMap); err != nil {
					log.Printf("[%s] failed to insert span: %v", remoteAddr, err)
					continue
				}
				count++
			}
		}
	}

	return count, nil
}

func handleProtobufTraces(remoteAddr string, body []byte) (int, error) {
	var req collectorpb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return 0, fmt.Errorf("invalid protobuf: %v", err)
	}

	count := 0
	for _, rs := range req.GetResourceSpans() {
		serviceName := ""
		if res := rs.GetResource(); res != nil {
			for _, attr := range res.GetAttributes() {
				if attr.GetKey() == "service.name" {
					serviceName = attr.GetValue().GetStringValue()
					break
				}
			}
		}

		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				spanData := map[string]interface{}{
					"traceId":           hexEncode(span.GetTraceId()),
					"spanId":            hexEncode(span.GetSpanId()),
					"parentSpanId":      hexEncode(span.GetParentSpanId()),
					"name":              span.GetName(),
					"kind":              float64(span.GetKind()),
					"startTimeUnixNano": fmt.Sprintf("%d", span.GetStartTimeUnixNano()),
					"endTimeUnixNano":   fmt.Sprintf("%d", span.GetEndTimeUnixNano()),
					"status": map[string]interface{}{
						"code": float64(span.GetStatus().GetCode()),
					},
				}

				if err := insertSpan(serviceName, spanData); err != nil {
					log.Printf("[%s] failed to insert span: %v", remoteAddr, err)
					continue
				}
				count++
			}
		}
	}

	return count, nil
}

func extractServiceName(rsMap map[string]interface{}) string {
	resource, ok := rsMap["resource"].(map[string]interface{})
	if !ok {
		return ""
	}

	attrs, ok := resource["attributes"].([]interface{})
	if !ok {
		return ""
	}

	for _, attr := range attrs {
		attrMap, ok := attr.(map[string]interface{})
		if !ok {
			continue
		}

		key, _ := attrMap["key"].(string)
		if key == "service.name" {
			if val, ok := attrMap["value"].(map[string]interface{}); ok {
				if strVal, ok := val["stringValue"].(string); ok {
					return strVal
				}
			}
		}
	}

	return ""
}

func insertSpan(serviceName string, spanMap map[string]interface{}) error {
	insertQueue <- insertJob{jobType: "span", serviceName: serviceName, data: spanMap}
	return nil
}

func doInsertSpan(serviceName string, spanMap map[string]interface{}) error {

	rawJSON, _ := json.Marshal(spanMap)

	traceID, _ := spanMap["traceId"].(string)
	spanID, _ := spanMap["spanId"].(string)
	parentSpanID, _ := spanMap["parentSpanId"].(string)
	spanName, _ := spanMap["name"].(string)

	var kind int
	if k, ok := spanMap["kind"].(float64); ok {
		kind = int(k)
	}

	var startTime, endTime int64
	if st, ok := spanMap["startTimeUnixNano"].(string); ok {
		json.Unmarshal([]byte(st), &startTime)
	}
	if et, ok := spanMap["endTimeUnixNano"].(string); ok {
		json.Unmarshal([]byte(et), &endTime)
	}

	var statusCode int
	if status, ok := spanMap["status"].(map[string]interface{}); ok {
		if code, ok := status["code"].(float64); ok {
			statusCode = int(code)
		}
	}

	_, err := db.Exec(`
		INSERT INTO traces (trace_id, span_id, parent_span_id, service_name, span_name, kind, start_time, end_time, status_code, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, traceID, spanID, parentSpanID, serviceName, spanName, kind, startTime, endTime, statusCode, string(rawJSON))

	return err
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	log.Printf("[%s] %s %s (Content-Type: %s)", r.RemoteAddr, r.Method, r.URL.Path, contentType)

	if r.Method != http.MethodPost {
		log.Printf("[%s] method not allowed: %s", r.RemoteAddr, r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	isJSON := strings.Contains(contentType, "json")
	isProtobuf := strings.Contains(contentType, "protobuf") || strings.Contains(contentType, "x-protobuf")

	if !isJSON && !isProtobuf {
		log.Printf("[%s] unsupported content type: %s", r.RemoteAddr, contentType)
		http.Error(w, "unsupported content type, expected application/json or application/x-protobuf", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[%s] failed to read body: %v", r.RemoteAddr, err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("[%s] received %d bytes", r.RemoteAddr, len(body))

	var count int
	if isProtobuf {
		count, err = handleProtobufLogs(r.RemoteAddr, body)
	} else {
		count, err = handleJSONLogs(r.RemoteAddr, body)
	}

	if err != nil {
		log.Printf("[%s] failed to process logs: %v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[%s] inserted %d log records", r.RemoteAddr, count)

	if isProtobuf {
		resp := &logspb.ExportLogsServiceResponse{}
		respBytes, _ := proto.Marshal(resp)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}
}

func handleJSONLogs(remoteAddr string, body []byte) (int, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("invalid JSON: %v", err)
	}

	count := 0
	resourceLogs, ok := payload["resourceLogs"].([]interface{})
	if !ok {
		log.Printf("[%s] no resourceLogs found", remoteAddr)
		return 0, nil
	}

	for _, rl := range resourceLogs {
		rlMap, ok := rl.(map[string]interface{})
		if !ok {
			continue
		}

		serviceName := extractServiceName(rlMap)

		scopeLogs, ok := rlMap["scopeLogs"].([]interface{})
		if !ok {
			continue
		}

		for _, sl := range scopeLogs {
			slMap, ok := sl.(map[string]interface{})
			if !ok {
				continue
			}

			logRecords, ok := slMap["logRecords"].([]interface{})
			if !ok {
				continue
			}

			for _, lr := range logRecords {
				lrMap, ok := lr.(map[string]interface{})
				if !ok {
					continue
				}

				if err := insertLog(serviceName, lrMap); err != nil {
					log.Printf("[%s] failed to insert log: %v", remoteAddr, err)
					continue
				}
				count++
			}
		}
	}

	return count, nil
}

func handleProtobufLogs(remoteAddr string, body []byte) (int, error) {
	var req logspb.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return 0, fmt.Errorf("invalid protobuf: %v", err)
	}

	count := 0
	for _, rl := range req.GetResourceLogs() {
		serviceName := ""
		if res := rl.GetResource(); res != nil {
			for _, attr := range res.GetAttributes() {
				if attr.GetKey() == "service.name" {
					serviceName = attr.GetValue().GetStringValue()
					break
				}
			}
		}

		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				logData := map[string]interface{}{
					"traceId":        hexEncode(lr.GetTraceId()),
					"spanId":         hexEncode(lr.GetSpanId()),
					"severityNumber": float64(lr.GetSeverityNumber()),
					"severityText":   lr.GetSeverityText(),
					"body":           lr.GetBody().GetStringValue(),
					"timeUnixNano":   fmt.Sprintf("%d", lr.GetTimeUnixNano()),
				}

				if err := insertLog(serviceName, logData); err != nil {
					log.Printf("[%s] failed to insert log: %v", remoteAddr, err)
					continue
				}
				count++
			}
		}
	}

	return count, nil
}

func insertLog(serviceName string, logMap map[string]interface{}) error {
	insertQueue <- insertJob{jobType: "log", serviceName: serviceName, data: logMap}
	return nil
}

func doInsertLog(serviceName string, logMap map[string]interface{}) error {
	rawJSON, _ := json.Marshal(logMap)

	traceID, _ := logMap["traceId"].(string)
	spanID, _ := logMap["spanId"].(string)
	severityText, _ := logMap["severityText"].(string)

	var severityNumber int
	if sn, ok := logMap["severityNumber"].(float64); ok {
		severityNumber = int(sn)
	}

	var body string
	if b, ok := logMap["body"].(string); ok {
		body = b
	} else if b, ok := logMap["body"].(map[string]interface{}); ok {
		if sv, ok := b["stringValue"].(string); ok {
			body = sv
		}
	}

	var logTimestamp int64
	if ts, ok := logMap["timeUnixNano"].(string); ok {
		json.Unmarshal([]byte(ts), &logTimestamp)
	}

	_, err := db.Exec(`
		INSERT INTO logs (trace_id, span_id, service_name, severity_number, severity_text, body, log_timestamp, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, traceID, spanID, serviceName, severityNumber, severityText, body, logTimestamp, string(rawJSON))

	return err
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] %s %s", r.RemoteAddr, r.Method, r.URL.Path)

	rows, err := db.Query(`
		SELECT id, timestamp, trace_id, span_id, parent_span_id, service_name, span_name, kind, start_time, end_time, status_code, raw_json
		FROM traces ORDER BY id DESC LIMIT 100
	`)
	if err != nil {
		log.Printf("[%s] query failed: %v", r.RemoteAddr, err)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var traces []map[string]interface{}
	for rows.Next() {
		var id int
		var timestamp time.Time
		var traceID, spanID, parentSpanID, serviceName, spanName, rawJSON string
		var kind, statusCode int
		var startTime, endTime int64

		if err := rows.Scan(&id, &timestamp, &traceID, &spanID, &parentSpanID, &serviceName, &spanName, &kind, &startTime, &endTime, &statusCode, &rawJSON); err != nil {
			continue
		}

		traces = append(traces, map[string]interface{}{
			"id":             id,
			"timestamp":      timestamp,
			"trace_id":       traceID,
			"span_id":        spanID,
			"parent_span_id": parentSpanID,
			"service_name":   serviceName,
			"span_name":      spanName,
			"kind":           kind,
			"start_time":     startTime,
			"end_time":       endTime,
			"status_code":    statusCode,
			"raw_json":       json.RawMessage(rawJSON),
		})
	}

	log.Printf("[%s] returning %d traces", r.RemoteAddr, len(traces))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"traces": traces,
		"count":  len(traces),
	})
}

func catchAllHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] %s %s", r.RemoteAddr, r.Method, r.URL.Path)

	if r.URL.Path == "/" {
		w.Write([]byte("otelite"))
		return
	}

	http.NotFound(w, r)
}

func runServer(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	port := fs.String("port", "4318", "port to listen on")
	dbPath := fs.String("db", "otel.db", "path to SQLite database")
	fs.Parse(args)

	log.Printf("initializing database: %s", *dbPath)
	if err := initDB(*dbPath); err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	defer db.Close()

	startInsertWorker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", handleTraces)
	mux.HandleFunc("/v1/logs", handleLogs)
	mux.HandleFunc("/traces", handleQuery)
	mux.HandleFunc("/", catchAllHandler)

	server := &http.Server{
		Addr:    ":" + *port,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("shutting down HTTP server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}()

	log.Printf("OTEL collector listening on :%s", *port)
	log.Printf("  POST /v1/traces - receive OTLP traces")
	log.Printf("  POST /v1/logs   - receive OTLP logs")
	log.Printf("  GET  /traces    - query stored traces")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
	log.Println("server stopped")
}

func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	dbPath := fs.String("db", "otel.db", "path to SQLite database")
	fs.Parse(args)

	if fs.NArg() < 1 {
		log.Fatal("usage: otelite query -db <path> <SQL query>")
	}

	query := fs.Arg(0)

	var err error
	db, err = sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	log.Printf("executing query: %s", query)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("failed to get columns: %v", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("scan error: %v", err)
			continue
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	log.Printf("found %d rows", len(results))

	if len(results) == 0 {
		fmt.Println("(no rows)")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, strings.Join(cols, "\t"))

	seps := make([]string, len(cols))
	for i, col := range cols {
		seps[i] = strings.Repeat("-", len(col))
	}
	fmt.Fprintln(w, strings.Join(seps, "\t"))

	for _, row := range results {
		vals := make([]string, len(cols))
		for i, col := range cols {
			val := fmt.Sprintf("%v", row[col])
			if len(val) > 50 {
				val = val[:47] + "..."
			}
			vals[i] = val
		}
		fmt.Fprintln(w, strings.Join(vals, "\t"))
	}

	w.Flush()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: otelite <command> [options]")
		fmt.Println("")
		fmt.Println("commands:")
		fmt.Println("  server  start the OTLP collector")
		fmt.Println("  query   run SQL query against the database")
		fmt.Println("")
		fmt.Println("examples:")
		fmt.Println("  otelite server -port 4318 -db otel.db")
		fmt.Println("  otelite query -db otel.db \"SELECT * FROM traces LIMIT 10\"")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		cancel()
	}()

	switch os.Args[1] {
	case "server":
		runServer(ctx, os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}
}

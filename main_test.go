package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) (string, context.CancelFunc, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "oteldb_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbPath := tmpFile.Name()
	tmpFile.Close()

	if err := initDB(dbPath); err != nil {
		os.Remove(dbPath)
		t.Fatalf("failed to init db: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startInsertWorker(ctx)

	return dbPath, cancel, func() {
		cancel()
		if db != nil {
			db.Close()
		}
		os.Remove(dbPath)
	}
}

func waitForInserts(t *testing.T, timeout time.Duration) {
	t.Helper()
	// Give the insert worker time to process
	time.Sleep(timeout)
}

func TestTraceIngestion(t *testing.T) {
	dbPath, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Create OTLP trace payload
	tracePayload := map[string]interface{}{
		"resourceSpans": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{
							"key": "service.name",
							"value": map[string]interface{}{
								"stringValue": "test-service",
							},
						},
					},
				},
				"scopeSpans": []interface{}{
					map[string]interface{}{
						"spans": []interface{}{
							map[string]interface{}{
								"traceId":           "0123456789abcdef0123456789abcdef",
								"spanId":            "0123456789abcdef",
								"parentSpanId":      "",
								"name":              "test-span",
								"kind":              float64(1),
								"startTimeUnixNano": "1700000000000000000",
								"endTimeUnixNano":   "1700000001000000000",
								"status": map[string]interface{}{
									"code": float64(0),
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := json.Marshal(tracePayload)
	if err != nil {
		t.Fatalf("failed to marshal trace payload: %v", err)
	}

	// Create request and response recorder
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Call handler
	handleTraces(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Wait for async insert
	waitForInserts(t, 100*time.Millisecond)

	// Query database to verify
	testDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer testDB.Close()

	var count int
	err = testDB.QueryRow("SELECT COUNT(*) FROM traces").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query traces: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 trace, got %d", count)
	}

	// Verify trace data
	var traceID, spanName, serviceName string
	err = testDB.QueryRow("SELECT trace_id, span_name, service_name FROM traces LIMIT 1").Scan(&traceID, &spanName, &serviceName)
	if err != nil {
		t.Fatalf("failed to query trace details: %v", err)
	}

	if traceID != "0123456789abcdef0123456789abcdef" {
		t.Errorf("expected traceId '0123456789abcdef0123456789abcdef', got '%s'", traceID)
	}
	if spanName != "test-span" {
		t.Errorf("expected span name 'test-span', got '%s'", spanName)
	}
	if serviceName != "test-service" {
		t.Errorf("expected service name 'test-service', got '%s'", serviceName)
	}
}

func TestLogIngestion(t *testing.T) {
	dbPath, _, cleanup := setupTestDB(t)
	defer cleanup()

	// Create OTLP log payload
	logPayload := map[string]interface{}{
		"resourceLogs": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": []interface{}{
						map[string]interface{}{
							"key": "service.name",
							"value": map[string]interface{}{
								"stringValue": "log-service",
							},
						},
					},
				},
				"scopeLogs": []interface{}{
					map[string]interface{}{
						"logRecords": []interface{}{
							map[string]interface{}{
								"traceId":        "abcdef0123456789abcdef0123456789",
								"spanId":         "abcdef01234567",
								"severityNumber": float64(9),
								"severityText":   "INFO",
								"body": map[string]interface{}{
									"stringValue": "Test log message",
								},
								"timeUnixNano": "1700000000500000000",
							},
						},
					},
				},
			},
		},
	}

	body, err := json.Marshal(logPayload)
	if err != nil {
		t.Fatalf("failed to marshal log payload: %v", err)
	}

	// Create request and response recorder
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Call handler
	handleLogs(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Wait for async insert
	waitForInserts(t, 100*time.Millisecond)

	// Query database to verify
	testDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer testDB.Close()

	var count int
	err = testDB.QueryRow("SELECT COUNT(*) FROM logs").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query logs: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 log, got %d", count)
	}

	// Verify log data
	var traceID, serviceName, severityText, logBody string
	var severityNumber int
	err = testDB.QueryRow("SELECT trace_id, service_name, severity_number, severity_text, body FROM logs LIMIT 1").Scan(&traceID, &serviceName, &severityNumber, &severityText, &logBody)
	if err != nil {
		t.Fatalf("failed to query log details: %v", err)
	}

	if traceID != "abcdef0123456789abcdef0123456789" {
		t.Errorf("expected traceId 'abcdef0123456789abcdef0123456789', got '%s'", traceID)
	}
	if serviceName != "log-service" {
		t.Errorf("expected service name 'log-service', got '%s'", serviceName)
	}
	if severityNumber != 9 {
		t.Errorf("expected severity number 9, got %d", severityNumber)
	}
	if severityText != "INFO" {
		t.Errorf("expected severity text 'INFO', got '%s'", severityText)
	}
	if logBody != "Test log message" {
		t.Errorf("expected body 'Test log message', got '%s'", logBody)
	}
}

// TestConcurrentReadWrite reproduces the pytest-vs-server race: a separate
// DB handle reads in parallel with server-side inserts. Without WAL +
// busy_timeout this fails with "database is locked".
func TestConcurrentReadWrite(t *testing.T) {
	dbPath, _, cleanup := setupTestDB(t)
	defer cleanup()

	reader, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&mode=ro")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	makePayload := func(i int) []byte {
		p := map[string]interface{}{
			"resourceSpans": []interface{}{map[string]interface{}{
				"resource": map[string]interface{}{"attributes": []interface{}{
					map[string]interface{}{"key": "service.name", "value": map[string]interface{}{"stringValue": "race-svc"}},
				}},
				"scopeSpans": []interface{}{map[string]interface{}{
					"spans": []interface{}{map[string]interface{}{
						"traceId":           fmt.Sprintf("%032x", i),
						"spanId":            fmt.Sprintf("%016x", i),
						"name":              "s",
						"kind":              float64(1),
						"startTimeUnixNano": "1700000000000000000",
						"endTimeUnixNano":   "1700000001000000000",
						"status":            map[string]interface{}{"code": float64(0)},
					}},
				}},
			}},
		}
		b, _ := json.Marshal(p)
		return b
	}

	const writers = 4
	const perWriter = 50
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: concurrent POSTs to the trace handler.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				req := httptest.NewRequest(http.MethodPost, "/v1/traces",
					bytes.NewReader(makePayload(w*perWriter+i)))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				handleTraces(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("write %d/%d: status %d", w, i, rec.Code)
					return
				}
			}
		}(w)
	}

	// Reader: hammer COUNT(*) until writers stop.
	var readErr error
	var reads int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var n int
			if err := reader.QueryRow("SELECT COUNT(*) FROM traces").Scan(&n); err != nil {
				readErr = err
				return
			}
			reads++
		}
	}()

	// Poll until the insert worker has drained everything.
	expected := writers * perWriter
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := reader.QueryRow("SELECT COUNT(*) FROM traces").Scan(&n); err != nil {
			t.Fatalf("reader query failed mid-test: %v", err)
		}
		if n >= expected {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	if readErr != nil {
		t.Fatalf("reader got error (expected WAL+busy_timeout to prevent this): %v", readErr)
	}
	if reads == 0 {
		t.Fatal("reader never completed a query")
	}

	var final int
	if err := reader.QueryRow("SELECT COUNT(*) FROM traces").Scan(&final); err != nil {
		t.Fatalf("final read: %v", err)
	}
	if final != expected {
		t.Errorf("expected %d rows, got %d", expected, final)
	}
	t.Logf("completed %d concurrent reads alongside %d writes", reads, expected)
}

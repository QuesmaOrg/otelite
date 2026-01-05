# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

otelite is a lightweight OpenTelemetry collector written in Go that stores traces and logs in SQLite. It accepts OTLP data via HTTP (both JSON and Protobuf formats) and provides a simple query interface.

## Build and Run Commands

```bash
# Build the binary
make build

# Run tests
go test -v ./...

# Start the server (background, uses otel.db)
make start

# Stop the server
make stop

# Clean build artifacts and database
make clean

# Manual server run with options
./otelite server -port 4318 -db otel.db

# Query the database directly
./otelite query -db otel.db "SELECT * FROM traces LIMIT 10"
```

## Architecture

Single-file Go application (`main.go`) with two subcommands:
- `server` - HTTP server accepting OTLP traces/logs
- `query` - CLI tool for running SQL queries against the database

### Key Components

- **Insert Worker**: All database writes are serialized through a buffered channel (`insertQueue`) processed by a single goroutine. This avoids SQLite write contention.
- **HTTP Handlers**: `/v1/traces` and `/v1/logs` accept OTLP data (JSON or Protobuf), `/traces` returns stored traces as JSON
- **Database**: SQLite with two tables (`traces`, `logs`) storing parsed span/log fields plus raw JSON

### Database Schema

- `traces` table: trace_id, span_id, parent_span_id, service_name, span_name, kind, start_time, end_time, status_code, raw_json
- `logs` table: trace_id, span_id, service_name, severity_number, severity_text, body, log_timestamp, raw_json

### Dependencies

- `modernc.org/sqlite` - Pure Go SQLite driver (CGO-free)
- `go.opentelemetry.io/proto/otlp` - OTLP Protobuf definitions

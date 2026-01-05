BINARY=otelite
PIDFILE=$(BINARY).pid

.PHONY:  start stop test

build: otelite

$(BINARY): main.go
	go build -o $(BINARY) main.go
	ls -l $(BINARY)

start: otelite
	./$(BINARY) server -port 4318 -db otel.db >otel.log 2>&1 & echo $$! > $(PIDFILE)

stop:
	@if [ -f $(PIDFILE) ]; then \
		kill $$(cat $(PIDFILE)) 2>/dev/null || true; \
		rm -f $(PIDFILE); \
	fi

clean:
	rm -f $(PIDFILE) otel.db ${BINARY}


test:
	@curl -s http://localhost:4318/ | grep -q 'otelite' && echo "otelite root: OK" || echo "otelite root: FAILED"



.PHONY: build start stop clean 
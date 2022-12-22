.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-default
run-default:
	go run cmd/httpd/main.go -use_default -port 8080 -doc_root test/testdata/htdocs

.PHONY: run-tritonhttp
run-tritonhttp:
	go run cmd/httpd/main.go -port 8080 -doc_root test/testdata/htdocs

.PHONY: unit-test
unit-test:
	go test -v ./pkg/...

.PHONY: e2e-test
e2e-test:
	rm -rf test/_bin
	GOBIN=$(PWD)/test/_bin go install ./...
	go test -v ./test/...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
	rm -f test/testdata/responses/*/*.dat
	go mod tidy

.PHONY: submission
submission: clean
	rm -f submission.zip
	zip -r submission.zip . -x /.git/*

-include Makefile.TA.mk

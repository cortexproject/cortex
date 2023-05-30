GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GO=CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) GO111MODULE=on go
GOTEST=CGO_ENABLED=1 GO111MODULE=on go test # go race detector requires cgo
GOBUILD=$(GO) build -ldflags '$(LDFLAGS)'

test:
	$(GOTEST) -timeout 600s -v -count=1 .
	$(GOTEST) --tags=stringlabels -timeout 600s -v -count=1 .

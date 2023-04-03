package importer

import (
	"context"
	"net/http"

	"github.com/cortexproject/cortex/pkg/importer/importerpb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type Importer struct {
	services.Service
}

func New() *Importer {
	i := &Importer{}
	i.Service = services.NewBasicService(i.starting, i.running, i.stopping)
	return i
}

func (i *Importer) SampleRPC(ctx context.Context, request *importerpb.SampleRequest) (*importerpb.SampleResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Importer) ImportHandler(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("hello world"))
	w.WriteHeader(http.StatusNoContent)
}

func (i *Importer) starting(ctx context.Context) error {
	return nil
}

func (i *Importer) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		}
	}
}

func (i *Importer) stopping(err error) error {
	return nil
}

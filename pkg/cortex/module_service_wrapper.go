package cortex

import (
	"context"
	"fmt"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/pstibrany/services"

	"github.com/cortexproject/cortex/pkg/util"
)

// This service wraps module service, and adds waiting for dependencies to start before starting,
// and dependant modules to stop before stopping this module service.
type moduleServiceWrapper struct {
	services.BasicService

	cortex    *Cortex
	module    moduleName
	service   services.Service
	startDeps []moduleName
	stopDeps  []moduleName
}

func newModuleServiceWrapper(cortex *Cortex, mod moduleName, modServ services.Service, startDeps []moduleName, stopDeps []moduleName) *moduleServiceWrapper {
	w := &moduleServiceWrapper{
		cortex:    cortex,
		module:    mod,
		service:   modServ,
		startDeps: startDeps,
		stopDeps:  stopDeps,
	}

	services.InitBasicService(&w.BasicService, w.start, w.run, w.stop)
	return w
}

func (w *moduleServiceWrapper) start(serviceContext context.Context) error {
	// wait until all startDeps are running
	for _, m := range w.startDeps {
		s := w.cortex.serviceMap[m]
		if s == nil {
			continue
		}

		level.Debug(util.Logger).Log("msg", "module waiting for initialization", "module", w.module, "waiting_for", m)

		err := s.AwaitRunning(serviceContext)
		if err != nil {
			return fmt.Errorf("failed to start %v, because dependant service %v has failed: %v", w.module, m, err)
		}
	}

	// we don't want to let service to stop until we stop all dependant services,
	// so we use new context
	level.Info(util.Logger).Log("msg", "initialising", "module", w.module)
	err := w.service.StartAsync(context.Background())
	if err != nil {
		return errors.Wrapf(err, "error starting module: %s", w.module)
	}

	return w.service.AwaitRunning(serviceContext)
}

func (w *moduleServiceWrapper) run(serviceContext context.Context) error {
	// wait until service stops, or context is canceled, whatever happens first.
	err := w.service.AwaitTerminated(serviceContext)
	if err == context.Canceled {
		return nil
	}
	return err
}

func (w *moduleServiceWrapper) stop() error {
	// wait until all stopDeps have stopped
	for _, m := range w.stopDeps {
		s := w.cortex.serviceMap[m]
		if s == nil {
			continue
		}

		_ = w.cortex.serviceMap[m].AwaitTerminated(context.Background())
	}

	level.Debug(util.Logger).Log("msg", "stopping", "module", w.module)

	w.service.StopAsync()
	err := w.service.AwaitTerminated(context.Background())
	if err != nil {
		level.Info(util.Logger).Log("msg", "error stopping", "module", w.module, "err", err)
	} else {
		level.Info(util.Logger).Log("msg", "module stopped", "module", w.module)
	}
	return err
}

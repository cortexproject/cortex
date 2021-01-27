package distributor

import (
	"context"
	"hash/fnv"

	"github.com/cortexproject/cortex/pkg/alertmanager"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/tls"
	"github.com/prometheus/client_golang/prometheus"
)

// AlertmanagerClient is the interface that should be implemented by any client used to read/write data to an alertmanager via GPRC.
type AlertmanagerClient interface {
	alertmanagerpb.AlertmanagerClient

	// RemoteAddress returns the address of the remote alertmanager and is used to uniquely
	// identify an alertmanager instance.
	RemoteAddress() string
}

type alertmanagerReplicationSet struct {
	services.Service

	alertmanagersRing ring.ReadRing
	clientsPool       *client.Pool

	// Subservices manager
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func newAlertmanagerReplicationSet(alertmanagersRing *ring.Ring, tlsCfg tls.ClientConfig, logger log.Logger, reg prometheus.Registerer) (*alertmanagerReplicationSet, error) {
	s := &alertmanagerReplicationSet{
		alertmanagersRing: alertmanagersRing,
		clientsPool:       newAlertmanagerClientPool(client.NewRingServiceDiscovery(alertmanagersRing), tlsCfg, logger, reg),
	}

	var err error
	s.subservices, err = services.NewManager(alertmanagersRing, s.clientsPool)
	if err != nil {

		return nil, err
	}

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)

	return s, nil
}

func (s *alertmanagerReplicationSet) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return errors.Wrap(err, "unable to start alertmanager replication set subservices")
	}

	return nil
}
func (s *alertmanagerReplicationSet) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.subservicesWatcher.Chan():
			return errors.Wrap(err, "alertmanager replication set subservice failed")
		}
	}
}
func (s *alertmanagerReplicationSet) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
}

func (s *alertmanagerReplicationSet) GetClientsFor(userID string) ([]AlertmanagerClient, error) {
	ringHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(userID))

	alertmanagers, err := s.alertmanagersRing.Get(ringHasher.Sum32(), alertmanager.RingOp, nil, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get alertmanager replication for the user #{userID}")
	}

	// Get the client for each of these alertmanagers
	addrs := alertmanagers.GetAddresses()
	clients := make([]AlertmanagerClient, 0, len(addrs))
	for _, addr := range addrs {
		c, err := s.clientsPool.GetClientFor(addr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get alertmanager client for #{addr}")
		}
		clients = append(clients, c.(AlertmanagerClient))
	}

	return clients, nil
}

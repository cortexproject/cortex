package e2ecortex

import "github.com/cortexproject/cortex/integration/e2e"

// CortexService represents a Cortex service with at least an HTTP and GRPC port exposed.
type CortexService struct {
	*e2e.HTTPService

	grpcPort int
}

func NewCortexService(
	name string,
	image string,
	command *e2e.Command,
	readiness e2e.ReadinessProbe,
	httpPort int,
	grpcPort int,
	otherPorts ...int,
) *CortexService {
	return &CortexService{
		HTTPService: e2e.NewHTTPService(name, image, command, readiness, httpPort, otherPorts...),
		grpcPort:    grpcPort,
	}
}

func (s *CortexService) GRPCEndpoint() string {
	return s.Endpoint(s.grpcPort)
}

func (s *CortexService) NetworkGRPCEndpoint() string {
	return s.NetworkEndpoint(s.grpcPort)
}

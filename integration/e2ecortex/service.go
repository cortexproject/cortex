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
		HTTPService: e2e.NewHTTPService(name, image, command, readiness, httpPort, append(otherPorts, grpcPort)...),
		grpcPort:    grpcPort,
	}
}

func (s *CortexService) GRPCEndpoint() string {
	return s.Endpoint(s.grpcPort)
}

func (s *CortexService) NetworkGRPCEndpoint() string {
	return s.NetworkEndpoint(s.grpcPort)
}

// CompositeCortexService abstract an higher-level service composed, under the hood,
// by 2+ CortexService.
type CompositeCortexService struct {
	*e2e.CompositeHTTPService
}

func NewCompositeCortexService(services ...*CortexService) *CompositeCortexService {
	var httpServices []*e2e.HTTPService
	for _, s := range services {
		httpServices = append(httpServices, s.HTTPService)
	}

	return &CompositeCortexService{
		CompositeHTTPService: e2e.NewCompositeHTTPService(httpServices...),
	}
}

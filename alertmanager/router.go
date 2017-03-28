package alertmanager

import (
	"sync"

	"github.com/weaveworks/mesh"
)

// Router is the interface we use for a mesh router.
type Router interface {
	NewGossip(string, mesh.Gossiper) mesh.Gossip
	GetPeers() *mesh.Peers
}

// gossipFactory allows safe creation of mesh.Gossips on a mesh.Router.
type gossipFactory struct {
	*mesh.Router
	gossips map[string]mesh.Gossip
	lock    sync.Mutex
}

// newGossipFactory makes a new router factory.
func newGossipFactory(router *mesh.Router) gossipFactory {
	return gossipFactory{
		Router:  router,
		gossips: map[string]mesh.Gossip{},
	}
}

// NewGossip makes a new Gossip with the given `id`. If a gossip with that ID
// already exists, that will be returned instead.
func (gf *gossipFactory) NewGossip(id string, g mesh.Gossiper) mesh.Gossip {
	gf.lock.Lock()
	defer gf.lock.Unlock()
	gossip, ok := gf.gossips[id]
	if ok {
		return gossip
	}
	gossip = gf.Router.NewGossip(id, g)
	gf.gossips[id] = gossip
	return gossip
}

// GetPeers returns the peers of a router.
func (gf *gossipFactory) GetPeers() *mesh.Peers {
	return gf.Router.Peers
}

package mapper

import (
	"flag"
	"os"

	"github.com/cortexproject/cortex/pkg/chunk"
	yaml "gopkg.in/yaml.v2"
)

// Config is used to configure a migrate mapper
type Config struct {
	MapConfig string
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.MapConfig, "mapper.config", "", "file name for reader mapping configs")
}

// Mapper is used to update and reencode chunks with new User Ids
type Mapper struct {
	Users map[string]string `yaml:"users,omitempty"`
}

// New returns a Mapper
func New(cfg Config) (*Mapper, error) {
	if cfg.MapConfig == "" {
		return nil, nil
	}
	return loadMapperConfig(cfg.MapConfig)
}

// MapChunks updates and maps values onto an array of chunks
func (u *Mapper) MapChunks(chks []chunk.Chunk) ([]chunk.Chunk, error) {
	remappedChunks := make([]chunk.Chunk, len(chks))
	for i, c := range chks {
		newID, ok := u.Users[c.UserID]
		if ok {
			c = chunk.NewChunk(newID, c.Fingerprint, c.Metric, c.Data, c.From, c.Through)
			_, err := c.Encode()
			if err != nil {
				return nil, err
			}
		}
		remappedChunks[i] = c
	}
	return remappedChunks, nil
}

func loadMapperConfig(filename string) (*Mapper, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)

	chunkMapper := &Mapper{}

	if err := decoder.Decode(chunkMapper); err != nil {
		return nil, err
	}

	return chunkMapper, nil
}

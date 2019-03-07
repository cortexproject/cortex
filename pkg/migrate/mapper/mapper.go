package mapper

import (
	"flag"
	"os"

	"github.com/cortexproject/cortex/pkg/chunk"
	yaml "gopkg.in/yaml.v2"
)

// Map Config File
// The map config file is a yaml file structed as:
/*
users:
  user_original: user_mapped
  ...
  <user_id_src>: <user_id_dst>

*/

// Config is used to configure a migrate mapper
type Config struct {
	MapConfigFile string
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.MapConfigFile, "mapper.config", "", "file name for reader mapping configs")
}

// Mapper is used to update and reencode chunks with new User Ids
// It can also serve as a struct to map other aspects of chunks in
// the future as more migration needs arise
// TODO: Add functionality to add/edit/drop labels
type Mapper struct {
	Users map[string]string `yaml:"users,omitempty"`
}

// New returns a Mapper
func New(cfg Config) (*Mapper, error) {
	if cfg.MapConfigFile == "" {
		return nil, nil
	}
	return loadMapperConfig(cfg.MapConfigFile)
}

// MapChunks updates and maps values onto an array of chunks
func (u *Mapper) MapChunks(chks []chunk.Chunk) ([]chunk.Chunk, error) {
	remappedChunks := make([]chunk.Chunk, len(chks))
	for i, c := range chks {
		newID, ok := u.Users[c.UserID]
		if ok {
			c = chunk.NewChunk(newID, c.Fingerprint, c.Metric, c.Data, c.From, c.Through)
			err := c.Encode()
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

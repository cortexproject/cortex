package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	modeDump      = "dump"
	modeGapSearch = "gaps"
)

const (
	keyOrderNormal  = "forward"
	keyOrderReverse = "reverse"
	keyOrderRandom  = "random"
)

var (
	address  string
	keyfile  string
	rate     time.Duration
	mode     string
	keyOrder string
)

func init() {
	flag.StringVar(&address, "address", "localhost:11211", "Hostname to connect to and dump values.")
	flag.StringVar(&keyfile, "keyfile", "keys.txt", "File to parse for keys.  Expected to be newline delimited.")
	flag.DurationVar(&rate, "rate", 500*time.Millisecond, "Query rate.")
	flag.StringVar(&mode, "mode", "dump", "Specify mode for memcached tool [dump, gaps].")
	flag.StringVar(&keyOrder, "key-order", "random", "Specify order to consider keys [forward, reverse, random].")
}

func main() {
	flag.Parse()

	keys, err := readKeys(keyfile, keyOrder)
	if err != nil {
		log.Fatalf("Failed to readfile: %v", err)
	}

	throttle := time.Tick(rate)

	mc := memcache.New(address)
	mc.Timeout = 2 * time.Second
	for _, key := range keys {
		item, err := mc.Get(key)
		if err != nil {
			log.Fatalf("Failed to get key: %v", err)
		}

		req := &queryrange.CachedResponse{}
		err = req.XXX_Unmarshal(item.Value)
		if err != nil {
			log.Fatalf("Failed to unmarshal from protobuf: %v", err)
		}

		bytes, err := json.Marshal(req)
		if err != nil {
			log.Fatalf("Failed to marshal to json: %v", err)
		}

		if mode == modeDump {
			fmt.Println(string(bytes))
		}

		<-throttle
	}
}

func readKeys(path string, order string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err != nil {
		return nil, err
	}

	if order == keyOrderRandom {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(lines), func(i, j int) { lines[i], lines[j] = lines[j], lines[i] })
	} else if order == keyOrderReverse {
		for left, right := 0, len(lines)-1; left < right; left, right = left+1, right-1 {
			lines[left], lines[right] = lines[right], lines[left]
		}
	}

	return lines, nil
}

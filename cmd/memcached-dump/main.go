package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/bradfitz/gomemcache/memcache"
)

var (
	address string
	keyfile string
	rate    time.Duration
)

func init() {
	flag.StringVar(&address, "address", "localhost:11211", "Hostname to connect to and dump values.")
	flag.StringVar(&keyfile, "keyfile", "keys.txt", "File to parse for keys.  Expected to be newline delimited.")
	flag.DurationVar(&rate, "rate", 500*time.Millisecond, "Query rate.")
}

func main() {
	flag.Parse()

	keys, err := readLines(keyfile)
	if err != nil {
		log.Fatalf("Failed to readfile: %v", err)
	}

	throttle := time.Tick(rate)

	mc := memcache.New(address)
	mc.Timeout = time.Second
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

		fmt.Println(string(bytes))

		<-throttle
	}
}

func readLines(path string) ([]string, error) {
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
	return lines, scanner.Err()
}

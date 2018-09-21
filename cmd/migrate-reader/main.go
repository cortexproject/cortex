package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/migrate"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		storageConfig storage.Config
		schemaConfig  chunk.SchemaConfig
		readerConfig  migrate.ReaderConfig
	)
	util.RegisterFlags(&schemaConfig, &storageConfig, &readerConfig)
	flag.Parse()

	storageOpts, err := storage.Opts(storageConfig, schemaConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	reader, err := migrate.NewReader(readerConfig, storageOpts[0].Client)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = reader.TransferData(context.Background())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// params := storageOpts[0].Client.NewStreamBatch()
	// fmt.Println(params)
	// params.Add("dev_chunks_2537", "331", 0, 0)
	// fmt.Println(params)
	// out := make(chan chunk.Chunk)
	// var count int
	// go func(ch chan chunk.Chunk) {
	// 	for c := range ch {
	// 		count++
	// 		if count%50 == 0 {
	// 			fmt.Println(count)
	// 			fmt.Println(c.ExternalKey())
	// 		}
	// 		continue
	// 	}
	// 	fmt.Println(count)
	// }(out)

	// err = storageOpts[0].Client.StreamChunks(context.Background(), params, out)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// close(out)
}

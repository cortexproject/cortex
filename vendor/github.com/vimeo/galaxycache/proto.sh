#!/bin/bash
protoc  --go-grpc_out=. --proto_path=galaxycachepb --go_out=galaxycachepb --go_opt=paths=source_relative galaxycache.proto

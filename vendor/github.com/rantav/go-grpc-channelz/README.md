# go-grpc-channelz

An in-process Channelz UI for gRPC in Golang

## What is Channelz?

Channelz is a gRPC spec for introspection into gRPC channels.
Channels in gRPC represent connections and sockets. Channelz provides introspection into the current active grpc connections, including incoming and outgoing connections.
The full spec can be found [here](https://github.com/grpc/proposal/blob/master/A14-channelz.md)

## What is `go-grpc-channelz`?

`go-grpc-channelz` provides a web UI to view the current start of all gRPC channels. For each channel you'd be able to look into the remote peer, sub-channels, load balancing stategies, number of calls, socket activity and events and so on.

You install go-grpc-channelz into your service and expose it's web page and that's it. All in all, about 2-5 lines of code.

## Screenshots

![Top Channels](doc/top-channels.png)

![Channel](doc/channel.png)

![Subchannel](doc/subchannel.png)

![Socket](doc/socket.png)

![Server](doc/server.png)

## Usage

Channelz is implemented as a gRPC service. This service is turned off by by default, so you have to turn it on as so:

```go
import (
	channelzservice "google.golang.org/grpc/channelz/service"
)

// Register the channelz gRPC service to grpcServer so that we can query it for this service.
channelzservice.RegisterChannelzServiceToServer(grpcServer)
```

In this example `grpcServer` is a grpc server that you create externally. In many cases this server already exists (you only need one) but if not then here's how to create it:

```go
import "google.golang.org/grpc"

grpcServer := grpc.NewServer()
```

Now you should register the channelz web handler:

```go
import channelz "github.com/rantav/go-grpc-channelz"

// Register the channelz handler and mount it to /foo.
// Resources will be available at /foo/channelz
http.Handle("/", channelz.CreateHandler("/foo", grpcBindAddress))
```

Where `grpcBindAddress` is the address to which `grpcServer` is bound. This could be for example `":8080"` or `"localhost:8080"` etc. This address is required because the channelz web service accesses the channelz grpc service in order to query it.

Lastly, launch that web listener in order to serve the web UI:

```go
// Listen and serve HTTP for the default serve mux
adminListener, err := net.Listen("tcp", ":8081")
if err != nil {
    log.Fatal(err)
}
go http.Serve(adminListener, nil)
```

Now the service will be available at `http://localhost:8081/foo/channelz`

A complete example:

```go
import (
    "google.golang.org/grpc"
    channelzservice "google.golang.org/grpc/channelz/service"
    channelz "github.com/rantav/go-grpc-channelz"
)

grpcServer := grpc.NewServer()

// Register the channelz handler
http.Handle("/", channelz.CreateHandler("/foo", grpcBindAddress))

// Register the channelz gRPC service to grpcServer so that we can query it for this service.
channelzservice.RegisterChannelzServiceToServer(grpcServer)

// Listen and serve HTTP for the default serve mux
adminListener, err := net.Listen("tcp", ":8081")
if err != nil {
    log.Fatal(err)
}
go http.Serve(adminListener, nil)
```

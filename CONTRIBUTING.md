# Contributing to Cortex

Welcome! We're excited that you're interested in contributing. Below are some basic guidelines.

## Workflow

Cortex follows a standard GitHub pull request workflow. If you're unfamiliar with this workflow, read the very helpful [Understanding the GitHub flow](https://guides.github.com/introduction/flow/) guide from GitHub.

## Developer Certificates of Origin (DCOs)

Before submitting your work in a pull request, make sure that *all* commits are signed off with a **Developer Certificate of Origin** (DCO). Here's an example:

```shell
git commit -s -m "Here is my signed commit"
```

You can find further instructions [here](https://github.com/probot/dco#how-it-works).

## Building Cortex

To build:
```
make
```

(By default, the build runs in a Docker container, using an image built
with all the tools required. The source code is mounted from where you
run `make` into the build container as a Docker volume.)

To run the test suite:
```
make test
```

## Playing in `minikube`

First, start `minikube`.

You may need to load the Docker images into your minikube environment. There is
a convenient rule in the Makefile to do this:

```
make prime-minikube
```

Then run Cortex in minikube:
```
kubectl apply -f ./k8s
```

(these manifests use `latest` tags, i.e. this will work if you have
just built the images and they are available on the node(s) in your
Kubernetes cluster)

Cortex will sit behind an nginx instance exposed on port 30080.  A job is deployed to scrape itself.  Try it:

http://192.168.99.100:30080/api/prom/api/v1/query?query=up

If that doesn't work, your Minikube might be using a different ip address. Check with `minikube status`.

### Dependency management

We uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies on external packages.
This requires a working Go environment with version 1.11 or greater, git and [bzr](http://wiki.bazaar.canonical.com/Download) installed.

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
go mod tidy
go mod vendor
git add go.mod go.sum vendor
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the pull request.

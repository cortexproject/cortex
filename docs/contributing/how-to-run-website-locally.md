---
title: "How to run the website locally"
linkTitle: "How to run the website locally"
weight: 3
slug: how-to-run-the-website-locally
---

The Cortex documentation is compiled into a website published at [cortexmetrics.io](https://cortexmetrics.io/). These instructions explain how to run the website locally, in order to have a quick feedback loop while contributing to the documentation or website styling.


## Initial setup

The following initial setup is required only once:

1. Install [Hugo](https://gohugo.io/) `v0.94.2` (**extended** version)
2. Install [Node.js](https://nodejs.org/en/) `v14` or above (alternatively via [`nvm`](https://github.com/nvm-sh/nvm))
3. Install required Node modules with:
   ```
   cd website && npm install && cd -
   ```
4. Install [embedmd](https://github.com/campoy/embedmd) `v1.0.0`:
   ```
   go install github.com/campoy/embedmd@v1.0.0
   ```
5. Run `make BUILD_IN_CONTAINER=false web-build`


## Run it

Once the initial setup is completed, you can run the website with the following command. The local website will run at [`http://localhost:1313/`](http://localhost:1313/)

```bash
# Keep this running
make web-serve
```

Whenever you change the content of `docs/` or markdown files in the repository root `/` you should run:

```bash
make BUILD_IN_CONTAINER=false web-pre
```

Whenever you change the config file or CLI flags in the Cortex code, you should rebuild the config file reference documentation:

```bash
make BUILD_IN_CONTAINER=false doc web-pre
```

---
title: "Generalize Modules Service to make it extensible"
linkTitle: "Generalize Modules Service to make it extensible"
weight: 1
slug: generalize-modules
---

- Author: @annanay25
- Reviewers: 
- Date: April 2020
- Status: Draft

## Overview

Cortex uses modules to start and operate services with dependencies. Inter-service dependencies are specified in a map and passed to a module manager which ensures that they are started in the right order of dependencies. While this works really well, the implementation is tied in specifically to the Cortex struct and is not flexible for use with other projects like Loki, which also require similar forms of dependency management. 
 
We would like to extend modules in cortex to a generic dependency management framework, that can be used by any project with no ties to cortex. 

## Specific goals

- Framework should allow for reusing cortex modules and allow us to:
  - Add new modules
  - Overwrite the implementation of a current module
  - Manage dependencies
- Framework should allow for building an application from scratch using the `modules` package, with no dependencies on Cortex. For ex: Remove code from Loki that was copied from `pkg/cortex/cortex.go`.



## Proposed Design

To make the modules package extensible, we need to abstract away any Cortex specific details from the module manager. The proposed design is to -

- Make a new component `moduleManager`, which is envisioned to be a central manager for all modules of the application. It stores modules & dependencies, and will be housed under a new package `pkg/util/modules`. The following is the interface for interacting with the `moduleManager`:
```
// Manager defines the interface for interaction with moduleManager.
type Manager interface {
   RegisterModule(name string, deps []string, svc service, options ...Option)
   AddDependency(fromModule string, toModule string) error
   ModuleServiceWrapper(name string, modServ services.Service)
   InitModuleServices(target string) (map[string]services.Service, error)
}
```

- Modules can be created by the application and registered with the `ModuleManager` using `RegisterModule`. The parameters are:
  - `name`: Name of the module
  - `deps`: A list of modules that this module depends on to run.
  - `svc`: A function that will be used to start the module.
  - `options`: Options are a variadic function parameter that can be used by the `moduleManager` to wrap the service function. In Cortex, we pass `Manager.ModuleServiceWrapper`, which wraps service to work with dependencies. 

- Dynamic dependencies between modules can be added using `AddDependency`. However, these dependencies need to be added before the call to `InitModuleServices`.

- The application can be initialized by running all the modules in the right order of dependencies by invoking `InitModuleServices`.

- `WrappedService` present in the current `module` design has been deprecated. To distinguish between `Service` and `WrappedService` in `RegisterModule`, the following options are available:
  - The simplest option is to pass a `boolean` parameter in that can default to `true` for a `WrappedService` since most services in Cortex are `WrappedServices`.
  - The second option is to wrap every `Service` into `WrappedService` if `options == nil` (default case), and for the special case where a service need not be wrapped, a dummy function can be passed.
  - A special function can be passed to the `ModuleManager` to wrap a `Service` into a `WrappedService` which waits on dependencies to start before it and stop after it. This parameter is `ModuleManager.ModuleServiceWrapper` referenced in the `Manager` interface.

- While the process of loading modules into `modules.Manager` should be remain as part of the `Cortex.New()` function, `InitModuleServices` should be part of `Cortex.Run()` and to enable this, `modules.Manager` would be made a member of the `Cortex` struct. 



## Usage

Following these changes, the Modules package will be a generic dependency management framework that can be used by any project.

#### To use the modules framework:
- Import the `pkg/util/modules` package, and initialize a new instance of the `Manager` using `modules.NewManager()`
- Create components in the system that implement the services interface (present in `pkg/util/services`).  
- Register each of these components as a module using `Manager.RegisterModule()` by passing name of the module, dependencies, initFn and any extra options that can be used to wrap the initFn.
- To add dynamic dependencies between modules, use `Manager.AddDependency()`
- Once all modules are added into `modules.Manager`, initialize the application by calling `Manager.InitModuleServices()` which initializes modules in the right order of dependencies.
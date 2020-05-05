---
title: "Generalize Modules Service to make it extensible"
linkTitle: "Generalize Modules Service to make it extensible"
weight: 1
slug: generalize-modules
---

- Author: @annanay25
- Reviewers: @jtlisi, @pstibrany, @cyriltovena, @pracucci
- Date: April 2020
- Status: Accepted

## Overview

Cortex uses modules to start and operate services with dependencies. Inter-service dependencies are specified in a map and passed to a module manager which ensures that they are initialised in the right order of dependencies. While this works really well, the implementation is tied in specifically to the Cortex struct and is not flexible for use with other projects like Loki, which also require similar forms of dependency management.

We would like to extend modules in cortex to a generic dependency management framework, that can be used by any project with no ties to cortex.

## Specific goals

- Framework should allow for reusing cortex modules and allow us to:
  - Add new modules
  - Overwrite the implementation of a current module
  - Manage dependencies
- Framework should allow for building an application from scratch using the `modules` package, with no dependencies on Cortex. For ex: Remove code from Loki that was copied from `pkg/cortex/cortex.go`.


## Proposed Design

### Modules package

To make the modules package extensible, we need to abstract away any Cortex specific details from the module manager. The proposed design is to:

- Make a new component `Manager`, which is envisioned to be a central manager for all modules of the application. It stores modules & dependencies, and will be housed under a new package `pkg/util/modules`. `Manager` has the following methods for interaction:
```
   func (m *Manager) RegisterModule(name string, initFn func() (Service, error))
   func (m *Manager) AddDependency(name string, dependsOn... string) error
   func (m *Manager) InitModuleServices(target string) (map[string]services.Service, error)
```

- Modules can be created by the application and registered with `modules.Manager` using `RegisterModule`. The parameters are:
  - `name`: Name of the module
  - `initFn`: A function that will be used to start the module. If it returns nil, and other modules depend on it, `InitModuleServices` will return an error.

- Dependencies between modules can be added using `AddDependency`. The parameters to the function are:
  - `name`: Name of the module
  - `dependsOn`: A variadic list of modules that the module depends on.

  These need to be added before the call to `InitModuleServices`.

- The application can be initialized by running `initFn`'s of all the modules in the right order of dependencies by invoking `InitModuleServices` with the target module name.


### Changes to `pkg/cortex`:

- `WrappedService` present in the current `module` design will be deprecated. All `initFn`'s will be wrapped into `WrappedService` by default.

- While the process of loading modules into `modules.Manager` should be remain as part of the `Cortex.New()` function, `InitModuleServices` should be part of `Cortex.Run()` and to enable this, `modules.Manager` would be made a member of the `Cortex` struct.


## Usage

Following these changes, the Modules package will be a generic dependency management framework that can be used by any project.

#### To use the modules framework:

- Import the `pkg/util/modules` package, and initialize a new instance of the `Manager` using `modules.NewManager()`
- Create components in the system that implement the services interface (present in `pkg/util/services`).
- Register each of these components as a module using `Manager.RegisterModule()` by passing name of the module and `initFn` for the module.
- To add dependencies between modules, use `Manager.AddDependency()`
- Once all modules are added into `modules.Manager`, initialize the application by calling `Manager.InitModuleServices()` which initializes modules in the right order of dependencies.


## Future work

- Extend the module manager to allow specifying multiple targets as opposed to a single target name supported currently.
- Factor out `Run()` method to make it independent of Cortex. This will help reduce replicated code in the Loki project as well as help manage `modules.Manager` outside of the Cortex struct.
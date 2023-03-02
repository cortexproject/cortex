// Copyright (c) The EfficientGo Authors.
// Licensed under the Apache License 2.0.

package extkingpin

// PathOrContent is a flag type that defines two flags to fetch bytes. Either from file (*-file flag) or content (* flag).
// Also returns content of YAML file with substituted environment variables.
// Follows K8s convention, i.e $(...), as mentioned here https://kubernetes.io/docs/tasks/inject-data-application/define-interdependent-environment-variables/.

// RegisterPathOrContent registers PathOrContent flag in kingpinCmdClause.

// Content returns the content of the file when given or directly the content that has been passed to the flag.
// It returns an error when:
// * The file and content flags are both not empty.
// * The file flag is not empty but the file can't be read.
// * The content is empty and the flag has been defined as required.

// Option is a functional option type for PathOrContent objects.
// WithRequired allows you to override default required option.
// WithEnvSubstitution allows you to override default envSubstitution option.

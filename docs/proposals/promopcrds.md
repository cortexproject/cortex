---
title: "Cortex AlertmanagerConfig and PrometheusRule CRD Integration"
linkTitle: "AlertmanagerConfig and PrometheusRule CRD Integration"
weight: 20
slug: alertmanagerconfig-prometheusrules-crd-integration
---

## Summary

This proposal explores the idea of enabling Cortex Alertmanager to read and integrate with PrometheusOperator CRD AlertmanagerConfig and PrometheusRule resources. The goal is to bridge the gap between Kubernetes-native alerting configuration management and Cortex's multi-tenant architecture, allowing users to manage both alerting rules and notification configurations using familiar Kubernetes resources.

## Problem Statement

Currently, Cortex Alertmanager configuration can be managed through:
1. Direct HTTP API calls to set/update configurations and rules (when enabled)
2. Command-line tool for configuration management (cortextool)
3. Manual change of the persisent storage
4. I might be missing somehting?

While these methods work, they have several limitations:
- No Kubernetes-native experience - Users familiar with PrometheusOperator cannot leverage their existing knowledge
- Manual configuration management - No automatic synchronization with Kubernetes resources
- Limited GitOps integration - Cannot use standard Kubernetes deployment pipelines
- Complex multi-tenant setup - Each tenant must manually manage their Alertmanager configuration and rules

## The Idea

What if Cortex could automatically read AlertmanagerConfig and PrometheusRule CRDs from user namespaces and integrate them into its multi-tenant Alertmanager system?

## Approaches Considered

### Option 1: Direct S3 Integration
Create a component in each user namespace that watches local CRDs and writes directly to the bucket storage that Cortex Alertmanager uses.

### Option 2: API Gateway Approach
Create a component in each user namespace that reads local CRDs and translates them to API calls to Cortex Alertmanager.

### Option 3: Two-Component Architecture
Deploy an operator in the user cluster that reads CRDs across all namespaces, and an aggregator in the Cortex namespace that receives and processes the configurations.

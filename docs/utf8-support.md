# Enabling UTF-8 Support in Cortex

This guide explains how to enable UTF-8 support in Cortex.

## Why UTF-8 Support?
UTF-8 ensures that all Unicode characters are correctly stored and displayed. This is essential for internationalization and working with non-ASCII data.

## Prerequisites
- Cortex v1.x or higher
- Environment set up for Cortex
- YAML/JSON configuration files

## Configuration Steps

1. Open your Cortex configuration file (e.g., `cortex-config.yaml`).  
2. Add or update the encoding section:

```yaml
server:
  encoding: UTF-8


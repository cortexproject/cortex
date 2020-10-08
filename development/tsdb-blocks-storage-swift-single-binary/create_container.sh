#!/bin/bash

swift --os-auth-url http://localhost:5000/v2.0 \
      --os-project-name admin --os-region-name RegionOne --os-username admin --os-password s3cr3t list
swift --os-auth-url http://localhost:5000/v2.0 \
      --os-project-name admin --os-region-name RegionOne --os-username admin --os-password s3cr3t post cortex-tsdb
swift --os-auth-url http://localhost:5000/v2.0 \
      --os-project-name admin --os-region-name RegionOne --os-username admin --os-password s3cr3t list
{
  global: {},
  components: {
    'cortex-lite': {
      cortex_args: {
        '-ingester.chunk-encoding': 2,
        '-ingester.join-after': '1s',
        '-consul.hostname': 'consul.default.svc.cluster.local:8500',
        '-memcached.hostname': 'memcached.default.svc.cluster.local',
        '-memcached.timeout': '150ms',
        '-memcached.service': 'memcached',
        '-server.http-write-timeout': '31s',
        '-server.http-read-timeout': '31s',
        '-distributor.replication-factor': 1,
        '-database.uri': 'memory://',
      },
      cortex_flags: [
        '-dynamodb.use-periodic-tables',
      ],
      containerPort: 80,
      image: 'quay.io/weaveworks/cortex-lite:latest',
      name: 'cortex-lite',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
    },
    consul: {
      args: [
        'agent',
        '-ui',
        '-server',
        '-client=0.0.0.0',
        '-bootstrap',
      ],
      serverPort: 8300,
      serfPort: 8301,
      clientPort: 8400,
      httpPort: 8500,
      image: 'consul:0.7.1',
      name: 'consul',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
    },
    memcached: {
      containerPort: 11211,
      image: 'memcached:1.5.4',
      name: 'memcached',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
      mem: 1024,
    },
    prometheus: {
      containerPort: 80,
      image: 'prom/prometheus:v2.2.1',
      name: 'prometheus',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
      config: |||
        # my global config
        global:
          scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
          evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
          # scrape_timeout is set to the global default (10s).

        remote_write:
          - url: http://nginx/api/prom/push

        # Alertmanager configuration
        alerting:
          alertmanagers:
          - static_configs:
            - targets:
              # - alertmanager:9093

        # Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
        rule_files:
          # - "first_rules.yml"
          # - "second_rules.yml"

        # A scrape configuration containing exactly one endpoint to scrape:
        # Here it's Prometheus itself.
        scrape_configs:
          # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
          - job_name: 'prometheus'

            # metrics_path defaults to '/metrics'
            # scheme defaults to 'http'.

            static_configs:
              - targets: ['localhost:9090']
      |||,
    },
    nginx: {
      containerPort: 80,
      image: 'nginx:1.13-alpine',
      name: 'nginx',
      replicas: 1,
      servicePort: 80,
      type: 'ClusterIP',
      config: |||
        worker_processes  5;  ## Default: 1
        error_log  /dev/stderr;
        pid        /var/run/nginx/nginx.pid;
        worker_rlimit_nofile 8192;

        events {
          worker_connections  4096;  ## Default: 1024
        }

        http {
          default_type application/octet-stream;
          log_format   main '$remote_addr - $remote_user [$time_local]  $status '
            '"$request" $body_bytes_sent "$http_referer" '
            '"$http_user_agent" "$http_x_forwarded_for"';
          access_log   /dev/stderr  main;
          sendfile     on;
          tcp_nopush   on;
          resolver kube-dns.kube-system.svc.cluster.local;

          server { # simple reverse-proxy
            listen 80;
            proxy_set_header X-Scope-OrgID 0;

            # pass requests for dynamic content to rails/turbogears/zope, et al
            location = /api/prom/push {
              proxy_pass      http://cortex-lite.default.svc.cluster.local$request_uri;
            }

            location ~ /api/prom/.* {
              proxy_pass      http://cortex-lite.default.svc.cluster.local$request_uri;
            }
          }
        }
      |||,
    },
  },
}

local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components.nginx;
local k = import 'k.libsonnet';
local deployment = k.apps.v1beta1.deployment;
local container = k.apps.v1beta1.deployment.mixin.spec.template.spec.containersType;
local containerPort = container.portsType;
local service = k.core.v1.service;
local configMap = k.core.v1.configMap;
local servicePort = k.core.v1.service.mixin.spec.portsType;

local targetPort = params.containerPort;
local labels = { app: params.name };

local appService =
  service.new(
    params.name,
    labels,
    servicePort.new(params.servicePort, targetPort)
  )
  .withType(params.type);

local cm =
  configMap.new(
    params.name
  )
  + configMap.withData({ 'nginx.conf': params.config });

local appDeployment =
  deployment.new(
    params.name,
    params.replicas,
    container
    .new(params.name, params.image)
    .withPorts(containerPort.new(targetPort))
    .withVolumeMounts([
      {
        name: 'nginx-cfg',
        mountPath: '/etc/nginx/',
        readOnly: true,
      },
      {
        name: 'nginx-run',
        mountPath: '/var/run/nginx/',
      },
    ]),
    labels
  )
  + deployment.mixin.spec.template.spec.withVolumes([
    {
      name: 'nginx-cfg',
      configMap: {
        name: params.name,
      },
    },
    {
      name: 'nginx-run',
      emptyDir: {},
    },
  ]);

k.core.v1.list.new([appService, appDeployment, cm])

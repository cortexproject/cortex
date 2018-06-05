local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components.consul;
local k = import 'k.libsonnet';
local deployment = k.apps.v1beta1.deployment;
local container = k.apps.v1beta1.deployment.mixin.spec.template.spec.containersType;
local containerPort = container.portsType;
local service = k.core.v1.service;
local servicePort = k.core.v1.service.mixin.spec.portsType;

local labels = { app: params.name };

local appService =
  service.new(
    params.name,
    labels,
    servicePort.new(params.httpPort, params.httpPort)
  )
  .withType(params.type);

local appDeployment =
  deployment.new(
    params.name,
    params.replicas,
    container
    .new(params.name, params.image)
    .withPorts(containerPort.new(params.serverPort))
    .withPortsMixin(containerPort.new(params.serfPort))
    .withPortsMixin(containerPort.new(params.clientPort))
    .withPortsMixin(containerPort.new(params.httpPort)),
    labels
  );

k.core.v1.list.new([appService, appDeployment])

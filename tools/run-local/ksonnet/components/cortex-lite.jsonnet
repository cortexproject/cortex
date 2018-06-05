local env = std.extVar('__ksonnet/environments');
local params = std.extVar('__ksonnet/params').components['cortex-lite'];
local k = import 'k.libsonnet';
local deployment = k.apps.v1beta1.deployment;
local container = k.apps.v1beta1.deployment.mixin.spec.template.spec.containersType;
local containerPort = container.portsType;
local secret = k.core.v1.secret;
local service = k.core.v1.service;
local servicePort = k.core.v1.service.mixin.spec.portsType;

local targetPort = params.containerPort;
local labels = { app: params.name };

local args = [x + '=' + params.cortex_args[x] for x in std.objectFields(params.cortex_args)];

local appService = service.new(
  params.name,
  labels,
  servicePort.new(params.servicePort, targetPort)
).withType(params.type);

local GCPKey =
  secret.new('gcp-key', { 'gcp.json': std.base64(std.manifestJsonEx(params.gcp_key, '  ')) });

local appDeployment =
  deployment.new(
    params.name,
    params.replicas,
    container.new(params.name, params.image)
    .withArgs(args + params.cortex_flags)
    .withPorts(containerPort.new(targetPort))
    .withImagePullPolicy('Never')
    .withVolumeMounts([{
      name: 'gcp-creds',
      mountPath: '/secret',
      readOnly: true,
    }])
    .withEnv([{
      name: 'GOOGLE_APPLICATION_CREDENTIALS',
      value: '/secret/gcp.json',
    }]),
    labels
  )
  + deployment.mixin.spec.template.spec.withVolumes([{
    name: 'gcp-creds',
    secret: {
      secretName: 'gcp-key',
    },
  }])
  + deployment.mixin.spec.template.spec.withTerminationGracePeriodSeconds(10);

k.core.v1.list.new([GCPKey, appService, appDeployment])

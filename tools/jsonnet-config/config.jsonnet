local cortex = import 'cortex.libsonnet';

local mapToArgs(map) = std.map(function(key) '--%s=%s' % [key, map[key]], std.objectFields(map));

// flattenWith is a generic `flatten` using the provided `sep` to join field paths.
local flattenWith(obj, sep) =
  assert std.isObject(obj) : 'argument must be an object';
  local aux(obj, path) =
    std.foldr(
      function(field, acc)
        if std.isObject(obj[field])
        then
          acc + aux(obj[field], path + [field])
        else
          acc {
            [std.join(sep, path + [field])]: obj[field],
          }
      ,
      std.objectFields(obj),
      {}
    )
  ;
  aux(obj, []);

// flatten returns an object where each key is the path to a field in `config`
// joined by periods and each value is the value of the field at that path.
// For example, flatten({ a: b: "c"}) -> { "a.b": "c" }.
local flatten(config) = flattenWith(config, '.');

// expandOn is the inverse of `flattenWith`.
local expandOn(obj, sep) =
  local aux(k, v) = std.foldr(function(field, acc) { [field]+: acc }, std.split(k, '.'), v);
  std.foldr(function(field, acc) acc + aux(field, obj[field]), std.objectFields(obj), {});

// expand is the inverse of `flatten`.
local expand(obj) = expandOn(obj, '.');

// configToFlags translates a configuration file to flag key-value pairs.
local configToFlags(config) =
  local flags = flatten(cortex.flags);
  local flattened = flatten(config);
  std.foldr(function(field, acc)
              // TODO: investigate why limits.metric_relabel_configs is odd.
              if flags[field] != '' then
                acc {
                  [flags[field]]: local v = flattened[field]; if std.isArray(v) then std.join(',', v) else v,
                }
              else acc,
            std.objectFields(flattened),
            {});

// changedFlags shows only the difference from default flags.
local changedFlags(flags) =
  local defaults = configToFlags(cortex.defaults);
  std.foldr(function(flag, acc)
    if flags[flag] != defaults[flag]
    then acc { [flag]: flags[flag] }
    else acc, std.objectFields(flags), {});

// changedConfig shows the difference from default config.
local changedConfig(config) =
  local aux(config, defaults, path) =
    std.foldr(
      function(field, acc)
        assert std.objectHas(defaults, field) : 'unknown field in config: %s' % field;
        assert std.type(config[field]) == std.type(defaults[field]) : 'config field is incorrect type: default=%s, config=%s' % [std.type(defaults[field]), std.type(config[field])];
        if std.isObject(config[field])
        then
          acc + aux(config[field], defaults[field], path + [field])
        else
          if config[field] != defaults[field] then
            acc + std.foldr(function(field, acc) { [field]+: acc }, path + [field], config[field])
          else
            acc,
      std.objectFields(config),
      {}
    );
  aux(config, cortex.defaults, []);

// missingFlags shows config fields that are missing a corresponding flag.
local missingFlags(config) =
  local flags = flatten(cortex.flags);
  local flattened = flatten(config);
  std.filter(function(field) flags[field] == '', std.objectFields(flattened));

// invert transposes the keys and values for an object with string fields and string field values.
local invert(obj) =
  std.foldr(function(field, acc)
    assert std.isString(obj[field]) : 'field values must be strings';
    acc { [obj[field]]: field }, std.objectFields(obj), {});

// flagsToConfig translates Cortex flags into a configuration file.
local flagsToConfig(flags) =
  local mapping = invert(flatten(cortex.flags));
  expand(std.foldr(function(field, acc) acc { [mapping[field]]: flags[field] }, std.objectFields(flags), {}));


local config = cortex.defaults {
  target: 'gateway',
  distributor+: {
    ring+: { instance_interface_names: ['eth1', 'en0'] },
    ha_tracker+: { enable_ha_tracker: true },
  },
};

mapToArgs(configToFlags(flagsToConfig(changedFlags(configToFlags(config)))))

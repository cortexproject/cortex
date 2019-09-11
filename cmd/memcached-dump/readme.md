# memcached-dump

This cli program dumps all values from the a Cortex frontend memcached server.  It requires as input an address, a list of keys and a duration.

## to use

- Install memcdump
  - `apt-get install libmemcached-tools`
- Port forward to a pod
  - `kubectl port-forward <memcached-pod> 11211:11211`
- Run memcdump
  - `memcdump --servers=localhost > keys.txt`
- Run memcached-dump to start dumping json versions of CachedResponses to console.  By default it will have a query rate of 2/sec.
  - `./memcached-dump > values.txt`

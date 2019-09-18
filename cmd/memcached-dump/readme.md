# memcached-dump

This cli program dumps all values from the a Cortex frontend memcached server.  It requires as input an address, a list of keys and a duration.

## to use

- Install memcdump
  - `apt-get install libmemcached-tools`
- Port forward to a pod
  - `kubectl port-forward <memcached-pod> 11211:11211`
- Run memcdump
  - `memcdump --servers=localhost > keys.txt`
  - Note that this pulls keys in slab order from smallest to largest.  This means the values for the keys at the start of the file are very small and the values for the keys at the end of the file are very large.
- Run memcached-dump to start dumping json versions of CachedResponses to console.  By default it will have a query rate of 2/sec.
  - `./memcached-dump > values.txt`

### gap analysis

Search for and print found gaps:

```
./main -mode=gaps -min-gap=30m
```

After finding a gap, kill the memcached key, requery and determine if there's a difference.

```
./main -mode=gaps-validate
```
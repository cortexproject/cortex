package users

import "hash/fnv"

func ShardByUser(userID string) uint32 {
	ringHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(userID))
	return ringHasher.Sum32()
}

package blocksconvert

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// v9 (hashValue, rangeRange -> value)
// <userID>:d<Index>:metricName, <seriesID>\0\0\0 "7" \0 -> "-"
// <userID>:d<Index>:metricName:labelName, sha256(labelValue)\0<seriesID>\0\0 "8" \0 -> labelValue
// <userID>:d<Index>:<seriesID>, throughBytes \0\0 chunkID \0 "3" \0 -> ""
//
// <seriesID> is base64 of SHA256(labels)

// v10 (hashValue, rangeValue -> value)
// <shard>:<userID>:d<Index>:metricName, <seriesID>\0\0\0 "7" \0 -> "-"
// <shard>:<userID>:d<Index>:metricName:labelName, sha256(labelValue)\0<seriesID>\0\0 "8" \0 -> labelValue
// <userID>:d<Index>:<seriesID>, throughBytes \0\0 chunkID \0 "3" \0 -> "-"
// v11 adds:
// <seriesID>, \0\0\0 '9' \0 -> JSON array with label values.

// Series to Chunk mapping uses \0 "3" \0 suffix of range value.
func IsSeriesToChunkMapping(RangeValue []byte) bool {
	return bytes.HasSuffix(RangeValue, []byte("\0003\000"))
}

// e.RangeValue is: "userID:d<Index>:base64(sha256(labels))". Index is integer, base64 doesn't contain ':'.
func GetSeriesToChunkMapping(HashValue string, RangeValue []byte) (user string, index int, seriesID string, chunkID string, err error) {
	s := bytes.Split(RangeValue, []byte("\000"))
	chunkID = string(s[2])

	parts := strings.Split(HashValue, ":")
	if len(parts) < 3 {
		err = fmt.Errorf("not enough parts: %d", len(parts))
		return
	}

	seriesID = parts[len(parts)-1]
	indexStr := parts[len(parts)-2]
	if !strings.HasPrefix(indexStr, "d") { // Schema v9 and later uses "day" buckets, prefixed with "d"
		err = fmt.Errorf("invalid index prefix")
		return
	}
	index, err = strconv.Atoi(indexStr[1:])
	if err != nil {
		err = fmt.Errorf("failed to parse index: %w", err)
		return
	}
	user = strings.Join(parts[:len(parts)-2], ":")
	return
}

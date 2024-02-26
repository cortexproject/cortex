package util

import (
	"math/rand"
	"strings"
)

var (
	randomChar    = "0123456789abcdef"
	RandomStrings = []string{}
)

func init() {
	sb := strings.Builder{}
	for i := 0; i < 1000000; i++ {
		sb.Reset()
		sb.WriteString("pod://")
		for j := 0; j < 14; j++ {
			sb.WriteByte(randomChar[rand.Int()%len(randomChar)])
		}
		RandomStrings = append(RandomStrings, sb.String())
	}
}

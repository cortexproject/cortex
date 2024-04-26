package util

import (
	"math/rand"
	"strings"
)

func GenerateRandomStrings() []string {
	randomChar := "0123456789abcdef"
	randomStrings := make([]string, 0, 1000000)
	sb := strings.Builder{}
	for i := 0; i < 1000000; i++ {
		sb.Reset()
		sb.WriteString("pod://")
		for j := 0; j < 14; j++ {
			sb.WriteByte(randomChar[rand.Int()%len(randomChar)])
		}
		randomStrings = append(randomStrings, sb.String())
	}
	return randomStrings
}

package logutil

import (
	"bufio"
	"io"

	"go.uber.org/zap"
)

func LogOutput(r io.Reader, l *zap.Logger) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		l.Info(s.Text())
	}
	return s.Err()
}

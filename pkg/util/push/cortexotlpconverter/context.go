package cortexotlpconverter

import "context"

// everyNTimes supports checking for context error every n times.
type everyNTimes struct {
	n   int
	i   int
	err error
}

// checkContext calls ctx.Err() every e.n times and returns an eventual error.
func (e *everyNTimes) checkContext(ctx context.Context) error {
	if e.err != nil {
		return e.err
	}

	e.i++
	if e.i >= e.n {
		e.i = 0
		e.err = ctx.Err()
	}

	return e.err
}

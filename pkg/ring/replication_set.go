package ring

import (
	"context"
)

// ReplicationSet describes the ingesters to talk to for a given key, and how
// many errors to tolerate.
type ReplicationSet struct {
	Ingesters []*IngesterDesc
	MaxErrors int
}

// Do function f in parallel for all replicas in the set, erroring is we exceed
// MaxErrors and returning early otherwise.
func (r ReplicationSet) Do(ctx context.Context, f func(*IngesterDesc) (interface{}, error)) ([]interface{}, error) {
	errs := make(chan error, len(r.Ingesters))
	resultsChan := make(chan interface{}, len(r.Ingesters))

	for _, ing := range r.Ingesters {
		go func(ing *IngesterDesc) {
			result, err := f(ing)
			if err != nil {
				errs <- err
			} else {
				resultsChan <- result
			}
		}(ing)
	}

	var (
		minSuccess = len(r.Ingesters) - r.MaxErrors
		numErrs    int
		numSuccess int
		results    = make([]interface{}, 0, len(r.Ingesters))
	)
	for numSuccess < minSuccess {
		select {
		case err := <-errs:
			numErrs++
			if numErrs > r.MaxErrors {
				return nil, err
			}

		case result := <-resultsChan:
			numSuccess++
			results = append(results, result)

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return results, nil
}

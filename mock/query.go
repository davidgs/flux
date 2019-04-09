package mock

import (
	"sync"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute/executetest"
)

// QueryFromTables returns a query that contains results as provided.
func QueryFromTables(results [][]*executetest.Table) flux.Query {
	q := new(query)
	mu := new(sync.Mutex)
	q.results = make(chan flux.Result)
	q.mu = mu

	go func() {
		defer close(q.results)
		for _, r := range results {
			mu.Lock()
			if !q.done {
				q.results <- executetest.NewResult(r)
			}
			mu.Unlock()
		}
		q.Done()
	}()
	return q
}

// QueryFromTables returns some results and fails with the provided error in the middle of execution.
// You shouldn't rely on the number or the content of results returned before the error.
func QueryFromTablesWithError(err error) flux.Query {
	q := new(query)
	q.results = make(chan flux.Result)
	q.mu = new(sync.Mutex)

	go func() {
		defer close(q.results)
		q.results <- executetest.NewResult([]*executetest.Table{})
		q.err = err
	}()
	return q
}

// query implements flux.Query
type query struct {
	results chan flux.Result
	done    bool
	mu      *sync.Mutex

	err error
}

func (q *query) Results() <-chan flux.Result {
	return q.results
}

func (q *query) Done() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.done = true
}

func (q *query) Cancel() {
	q.Done()
}

func (q *query) Err() error {
	return q.err
}

func (q *query) Statistics() flux.Statistics {
	return flux.Statistics{}
}

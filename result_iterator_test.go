package flux_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/mock"
	"github.com/pkg/errors"
)

func TestQueryResultIterator_Results(t *testing.T) {
	type row struct {
		Value int64
		Tag   string
	}

	sampleData := [][]*executetest.Table{
		{
			{
				ColMeta: []flux.ColMeta{
					{Label: "Value", Type: flux.TInt},
					{Label: "Tag", Type: flux.TString},
				},
				KeyCols: []string{"Tag"},
				Data: [][]interface{}{
					{int64(10), "a"},
				},
			},
			{
				ColMeta: []flux.ColMeta{
					{Label: "Value", Type: flux.TInt},
					{Label: "Tag", Type: flux.TString},
				},
				KeyCols: []string{"Tag"},
				Data: [][]interface{}{
					{int64(20), "b"},
				},
			},
			{
				ColMeta: []flux.ColMeta{
					{Label: "Value", Type: flux.TInt},
					{Label: "Tag", Type: flux.TString},
				},
				KeyCols: []string{"Tag"},
				Data: [][]interface{}{
					{int64(30), "c"},
				},
			},
		},
	}

	q := mock.QueryFromTables(sampleData)
	ri := flux.NewResultIteratorFromQuery(q)
	defer ri.Release()

	// Create a slice with elements for every row in tables.
	got := make([]row, 0)
	for ri.More() {
		if err := ri.Next().Tables().Do(func(table flux.Table) error {
			return table.Do(func(cr flux.ColReader) error {
				for i := 0; i < cr.Len(); i++ {
					r := row{
						Value: cr.Ints(0).Value(i),
						Tag:   cr.Strings(1).ValueString(i),
					}
					got = append(got, r)
				}
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
	}

	if ri.Err() != nil {
		t.Fatal(errors.Wrap(ri.Err(), "unexpected error in result iterator"))
	}

	want := []row{
		{Value: 10, Tag: "a"},
		{Value: 20, Tag: "b"},
		{Value: 30, Tag: "c"},
	}

	if !cmp.Equal(want, got) {
		t.Fatalf("got unexpected results -want/got:\n%s\n", cmp.Diff(want, got))
	}
}

func TestQueryResultIterator_Error(t *testing.T) {
	expectedErr := errors.New("hello, I am an error")
	q := mock.QueryFromTablesWithError(expectedErr)
	ri := flux.NewResultIteratorFromQuery(q)
	defer ri.Release()

	for ri.More() {
		if err := ri.Next().Tables().Do(func(table flux.Table) error {
			return table.Do(func(cr flux.ColReader) error {
				// does nothing
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
	}

	if ri.Err() != expectedErr {
		t.Fatalf("didnt' get the expected error: -want/got:\n%s\n", cmp.Diff(expectedErr, ri.Err()))
	}
}

package core

import (
	"context"
	"testing"

	"github.com/MontFerret/api"
)

func BenchmarkCancelExecution(b *testing.B) {
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return &spySession{run: func(context.Context) (api.Output, error) {
			return api.Output{}, nil
		}}, nil
	}}

	host, err := newTestHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, testLimits())
	if err != nil {
		b.Fatal(err)
	}

	connection, err := host.OpenConnection()
	if err != nil {
		b.Fatal(err)
	}

	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		b.Fatal(err)
	}

	execution, err := connection.Execute(context.Background(), executeRequest{PlanID: compiled.ID})
	if err != nil {
		b.Fatal(err)
	}

	retained, err := connection.resources.Execution(context.Background(), execution.ID)
	if err != nil {
		b.Fatal(err)
	}

	<-retained.done
	b.Cleanup(func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			b.Errorf("close connection: %v", closeErr)
		}
	})

	b.ReportAllocs()
	operation, cancel := connection.operation(context.Background())
	defer cancel()
	b.ResetTimer()
	for b.Loop() {
		retained, err := connection.resources.Execution(operation, execution.ID)
		if err != nil {
			b.Fatal(err)
		}

		retained.Cancel()
	}
}

func BenchmarkRunDurableSession(b *testing.B) {
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return &spySession{run: func(context.Context) (api.Output, error) {
			return api.Output{ContentType: "text/plain", Content: []byte("ok")}, nil
		}}, nil
	}}

	host, err := newTestHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, testLimits())
	if err != nil {
		b.Fatal(err)
	}

	connection, err := host.OpenConnection()
	if err != nil {
		b.Fatal(err)
	}

	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		b.Fatal(err)
	}

	session, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID})
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		if closeErr := connection.Close(context.Background()); closeErr != nil {
			b.Errorf("close connection: %v", closeErr)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		run, err := connection.RunSession(context.Background(), session)
		if err != nil {
			b.Fatal(err)
		}

		retained, err := connection.resources.Execution(context.Background(), run.ID)
		if err != nil {
			b.Fatal(err)
		}

		<-retained.done

		if err := connection.ReleaseExecution(context.Background(), run.ID); err != nil {
			b.Fatal(err)
		}
	}
}

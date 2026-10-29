package core

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/testoutput"
)

func TestWireOutputCallbackPanicPropagatesAfterSettlement(t *testing.T) {
	host, err := newTestHost(&spyRuntime{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}

	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := connection.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	sentinel := errors.New("Wire callback defect")
	execution := newExecution(connection.resources, nil, nil, func(ctx context.Context) (api.Output, error) {
		return testoutput.New(ctx, &api.Content{}, nil), nil
	}, nil)
	defer execution.cancel(context.Canceled)
	go func() {
		execution.delivery <- outputDelivery{ctx: testContext(t), header: func(api.Metadata) error { return nil }, consume: func(context.Context, []byte) error { panic(sentinel) }}
	}()
	func() {
		defer func() {
			if got := recover(); got != sentinel { //nolint:errorlint // The original Wire panic identity must propagate.
				t.Errorf("callback panic changed: %v", got)
			}
		}()
		execution.run()
	}()
	select {
	case <-execution.done:
	default:
		t.Fatal("panic escaped before output settlement")
	}
}

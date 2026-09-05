package harness

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
)

type (
	// Resource identifies a hosted object, never a Wire protocol handle.
	Resource struct {
		ID, Parent int
		Kind       string
	}

	// Call is an immutable observation of a hosted API invocation.
	Call struct {
		Resource int
		Method   string
		Source   api.Source
		Compile  CompileOptions
		Options  SessionOptions
		Argument any
		Index    int
	}

	Snapshot struct {
		Resources []Resource
		Calls     []Call
	}

	// Recorder broadcasts changes so lifecycle assertions need no polling.
	Recorder struct {
		mu        sync.Mutex
		changed   chan struct{}
		resources []Resource
		calls     []Call
	}
)

func newRecorder() *Recorder {
	return &Recorder{changed: make(chan struct{})}
}

func (r *Recorder) create(kind string, parent int) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := len(r.resources) + 1
	r.resources = append(r.resources, Resource{ID: id, Parent: parent, Kind: kind})
	r.notify()

	return id
}

func (r *Recorder) record(call Call) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	call.Options = call.Options.clone()
	r.calls = append(r.calls, call)
	r.notify()
	count := 0

	for _, previous := range r.calls {
		if previous.Resource == call.Resource && previous.Method == call.Method {
			count++
		}
	}

	return count
}

// notify requires mu. Subscribers inspect state and subscribe under the same lock.
func (r *Recorder) notify() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *Recorder) snapshot() Snapshot {
	result := Snapshot{Resources: append([]Resource(nil), r.resources...), Calls: append([]Call(nil), r.calls...)}

	for i := range result.Calls {
		result.Calls[i].Options = result.Calls[i].Options.clone()
	}

	return result
}

func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.snapshot()
}

func (s Snapshot) Count(id int, method string) int {
	count := 0

	for _, call := range s.Calls {
		if call.Resource == id && call.Method == method {
			count++
		}
	}

	return count
}

func (s Snapshot) OfKind(kind string) []Resource {
	var result []Resource

	for _, resource := range s.Resources {
		if resource.Kind == kind {
			result = append(result, resource)
		}
	}

	return result
}

func (r *Recorder) Wait(t testing.TB, description string, predicate func(Snapshot) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		r.mu.Lock()
		snapshot, changed := r.snapshot(), r.changed
		r.mu.Unlock()

		if predicate(snapshot) {
			return
		}

		select {
		case <-changed:
		case <-ctx.Done():
			t.Errorf("%s: timed out; resources=%+v calls=%+v", description, snapshot.Resources, snapshot.Calls)

			return
		}
	}
}

func (r *Recorder) AssertClosed(t testing.TB) {
	t.Helper()
	r.Wait(t, "hosted resource reclamation", func(s Snapshot) bool {
		for _, resource := range s.Resources {
			if resource.Kind != "runtime" && s.Count(resource.ID, "CloseFinished") == 0 {
				return false
			}

			for _, method := range []string{"Run", "Compile", "CompileDebug", "Start", "Continue", "StepOver", "StepIn", "StepOut", "EvaluateFrame"} {
				if s.Count(resource.ID, method) != s.Count(resource.ID, method+"Finished") {
					return false
				}
			}
		}

		return true
	})

	snapshot := r.Snapshot()

	for _, resource := range snapshot.Resources {
		want := 1

		if resource.Kind == "runtime" {
			want = 0
		}

		if got := snapshot.Count(resource.ID, "Close"); got != want {
			t.Errorf("hosted %s %d Close calls = %d, want %d", resource.Kind, resource.ID, got, want)
		}
	}
}

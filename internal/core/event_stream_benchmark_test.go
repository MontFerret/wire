package core

import "testing"

func BenchmarkExecutionEventPublication(b *testing.B) {
	b.Run("no watchers", func(b *testing.B) {
		execution := newPublicationBenchmarkExecution()

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			execution.mu.Lock()
			execution.publishLocked(ExecutionEventStarted, false)
			execution.mu.Unlock()
		}
	})

	b.Run("one watcher", func(b *testing.B) {
		execution := newPublicationBenchmarkExecution()
		subscription, err := execution.Watch()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(subscription.Cancel)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			execution.mu.Lock()
			execution.publishLocked(ExecutionEventStarted, false)
			execution.mu.Unlock()
			<-subscription.Events
		}
	})
}

func newPublicationBenchmarkExecution() *Execution {
	return &Execution{
		id:     ExecutionID("execution"),
		planID: PlanID("plan"),
		state:  ExecutionRunning,
		events: newEventStream[ExecutionEvent](1),
		done:   make(chan struct{}),
	}
}

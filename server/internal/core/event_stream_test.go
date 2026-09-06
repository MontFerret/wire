package core

import (
	"errors"
	"sync"
	"testing"
)

type streamTestEvent struct {
	sequence uint64
	values   []int
}

func (e streamTestEvent) clone() streamTestEvent {
	e.values = append([]int(nil), e.values...)

	return e
}

func (e streamTestEvent) withSequence(sequence uint64) streamTestEvent {
	e.sequence = sequence

	return e
}

func TestEventStreamPublishesMonotonicEventsAndReplaysLatest(t *testing.T) {
	stream := newStreamTestEventStream(2)

	first, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer first.cancel()

	if first.current.sequence != 0 || first.current.values != nil {
		t.Fatalf("unexpected initial event: %#v", first.current)
	}

	published := streamTestEvent{values: []int{1}}
	stream.publish(published, false)

	received := <-first.events
	if received.sequence != 1 || received.values[0] != 1 {
		t.Fatalf("unexpected first event: %#v", received)
	}

	received.values[0] = 8

	stream.publish(streamTestEvent{values: []int{2}}, false)

	received = <-first.events
	if received.sequence != 2 || received.values[0] != 2 {
		t.Fatalf("unexpected second event: %#v", received)
	}

	received.values[0] = 9

	latest, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer latest.cancel()

	if latest.current.sequence != 2 || latest.current.values[0] != 2 {
		t.Fatalf("latest event was not replayed defensively: %#v", latest.current)
	}
}

func TestEventStreamUnsubscribeIsIdempotentAndReleasesCapacity(t *testing.T) {
	stream := newStreamTestEventStream(1)

	subscription, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stream.subscribe(); !errors.Is(err, errEventStreamLimit) {
		t.Fatalf("unexpected subscription-limit error: %v", err)
	}

	subscription.cancel()
	subscription.cancel()

	if _, open := <-subscription.events; open {
		t.Fatal("events channel remained open after unsubscribe")
	}

	if _, open := <-subscription.errors; open {
		t.Fatal("errors channel remained open after unsubscribe")
	}

	next, err := stream.subscribe()
	if err != nil {
		t.Fatalf("unsubscribe did not release capacity: %v", err)
	}

	next.cancel()
}

func TestEventStreamEvictsLaggingWatcherAndRetainsSlotUntilCancel(t *testing.T) {
	stream := newStreamTestEventStream(1)

	subscription, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	for value := range watcherBufferSize + 1 {
		stream.publish(streamTestEvent{values: []int{value}}, false)
	}

	if err := <-subscription.errors; !errors.Is(err, ErrWatcherLagged) {
		t.Fatalf("unexpected lag error: %v", err)
	}

	count := 0
	for range subscription.events {
		count++
	}

	if count != watcherBufferSize {
		t.Fatalf("lagging watcher retained %d buffered events, want %d", count, watcherBufferSize)
	}

	if _, err := stream.subscribe(); !errors.Is(err, errEventStreamLimit) {
		t.Fatalf("lag eviction released capacity before cancel: %v", err)
	}

	subscription.cancel()

	next, err := stream.subscribe()
	if err != nil {
		t.Fatalf("cancel did not release lagged subscription capacity: %v", err)
	}

	if next.current.sequence != watcherBufferSize+1 {
		t.Fatalf("unexpected latest sequence after lag: %d", next.current.sequence)
	}

	next.cancel()
}

func TestEventStreamTerminalPublishAndClose(t *testing.T) {
	stream := newStreamTestEventStream(2)

	subscription, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	stream.publish(streamTestEvent{values: []int{1}}, true)

	event := <-subscription.events
	if event.sequence != 1 || event.values[0] != 1 {
		t.Fatalf("unexpected terminal event: %#v", event)
	}

	if _, open := <-subscription.events; open {
		t.Fatal("terminal event did not close the events channel")
	}

	if _, open := <-subscription.errors; open {
		t.Fatal("terminal event did not close the errors channel")
	}

	late, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	if late.current.sequence != 1 || late.current.values[0] != 1 {
		t.Fatalf("late subscription did not receive the terminal event: %#v", late.current)
	}

	if _, open := <-late.events; open {
		t.Fatal("late terminal subscription received an open events channel")
	}

	if _, open := <-late.errors; open {
		t.Fatal("late terminal subscription received an open errors channel")
	}

	if _, err := stream.subscribe(); !errors.Is(err, errEventStreamLimit) {
		t.Fatalf("terminal subscriptions released capacity before cancel: %v", err)
	}

	stream.publish(streamTestEvent{values: []int{2}}, false)
	late.cancel()
	subscription.cancel()

	closed := newStreamTestEventStream(1)

	active, err := closed.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	closed.close()
	closed.close()

	if _, open := <-active.events; open {
		t.Fatal("explicit close left an active events channel open")
	}

	if _, err := closed.subscribe(); !errors.Is(err, errEventStreamLimit) {
		t.Fatalf("explicit close released capacity before cancel: %v", err)
	}

	active.cancel()

	lateClosed, err := closed.subscribe()
	if err != nil {
		t.Fatalf("cancel did not release explicitly closed capacity: %v", err)
	}

	if _, open := <-lateClosed.events; open {
		t.Fatal("late explicitly closed subscription received an open events channel")
	}

	lateClosed.cancel()
}

func TestEventStreamSupportsConcurrentPublishSubscribeAndCancel(t *testing.T) {
	const operations = 64

	stream := newStreamTestEventStream(operations)
	var wait sync.WaitGroup
	subscriptionErrors := make(chan error, operations)
	wait.Add(operations * 2)

	for value := range operations {
		go func() {
			defer wait.Done()
			stream.publish(streamTestEvent{values: []int{value}}, false)
		}()

		go func() {
			defer wait.Done()

			subscription, err := stream.subscribe()
			if err != nil {
				subscriptionErrors <- err

				return
			}

			subscription.cancel()
		}()
	}

	wait.Wait()
	close(subscriptionErrors)
	for err := range subscriptionErrors {
		t.Errorf("concurrent subscription failed: %v", err)
	}

	latest, err := stream.subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer latest.cancel()

	if latest.current.sequence != operations {
		t.Fatalf("concurrent publications ended at sequence %d, want %d", latest.current.sequence, operations)
	}
}

func TestResourceWatchersRetainTheirLimitErrors(t *testing.T) {
	execution := &Execution{events: newEventStream(1, cloneExecutionEvent, sequenceExecutionEvent)}

	executionSubscription, err := execution.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer executionSubscription.Cancel()

	if _, err := execution.Watch(); !hasCategory(err, ErrorKindResourceExhausted) || err.Error() != "execution watcher limit reached" {
		t.Fatalf("unexpected execution watcher limit error: %v", err)
	}

	session := &DebugSession{events: newEventStream(1, cloneDebugEvent, sequenceDebugEvent)}

	debugSubscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer debugSubscription.Cancel()

	if _, err := session.Watch(); !hasCategory(err, ErrorKindResourceExhausted) || err.Error() != "debug watcher limit reached" {
		t.Fatalf("unexpected debug watcher limit error: %v", err)
	}
}

func newStreamTestEventStream(maxWatchers int) *eventStream[streamTestEvent] {
	return newEventStream(
		maxWatchers,
		func(event streamTestEvent) streamTestEvent { return event.clone() },
		func(event streamTestEvent, sequence uint64) streamTestEvent { return event.withSequence(sequence) },
	)
}

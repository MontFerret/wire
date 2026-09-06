package core

import (
	"errors"
	"sync"
)

type (
	eventStream[T any] struct {
		mu               sync.Mutex
		maxSubscriptions int
		clone            func(T) T
		withSequence     func(T, uint64) T
		sequence         uint64
		latest           T
		hasLatest        bool
		closed           bool
		nextWatcher      uint64
		subscriptions    int
		watchers         map[uint64]*eventWatcher[T]
	}

	eventWatcher[T any] struct {
		events chan T
		errors chan error
	}

	eventSubscription[T any] struct {
		current T
		events  <-chan T
		errors  <-chan error
		cancel  func()
	}
)

const watcherBufferSize = 8

var errEventStreamLimit = errors.New("event stream subscription limit reached")

func newEventStream[T any](maxSubscriptions int, clone func(T) T, withSequence func(T, uint64) T) *eventStream[T] {
	return &eventStream[T]{
		maxSubscriptions: maxSubscriptions,
		clone:            clone,
		withSequence:     withSequence,
		watchers:         make(map[uint64]*eventWatcher[T]),
	}
}

func (s *eventStream[T]) subscribe() (eventSubscription[T], error) {
	s.mu.Lock()
	if s.subscriptions >= s.maxSubscriptions {
		s.mu.Unlock()

		return eventSubscription[T]{}, errEventStreamLimit
	}

	s.subscriptions++
	s.nextWatcher++
	id := s.nextWatcher

	var current T

	if s.hasLatest {
		current = s.clone(s.latest)
	}

	if s.closed {
		events := make(chan T)
		errorsChannel := make(chan error)
		close(events)
		close(errorsChannel)
		s.mu.Unlock()

		var once sync.Once

		return eventSubscription[T]{
			current: current,
			events:  events,
			errors:  errorsChannel,
			cancel: func() {
				once.Do(func() { s.unsubscribe(id) })
			},
		}, nil
	}

	watcher := &eventWatcher[T]{events: make(chan T, watcherBufferSize), errors: make(chan error, 1)}
	s.watchers[id] = watcher
	s.mu.Unlock()

	var once sync.Once

	return eventSubscription[T]{
		current: current,
		events:  watcher.events,
		errors:  watcher.errors,
		cancel: func() {
			once.Do(func() { s.unsubscribe(id) })
		},
	}, nil
}

// publish takes ownership of event. Semantic owners must provide an event whose
// mutable snapshot data is already detached from their live state.
func (s *eventStream[T]) publish(event T, terminal bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.sequence++
	s.latest = s.withSequence(event, s.sequence)
	s.hasLatest = true

	if terminal {
		s.closed = true
	}

	for id, watcher := range s.watchers {
		select {
		case watcher.events <- s.clone(s.latest):
			if terminal {
				s.closeWatcherLocked(id, watcher, nil)
			}
		default:
			s.closeWatcherLocked(id, watcher, ErrWatcherLagged)
		}
	}
}

func (s *eventStream[T]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	for id, watcher := range s.watchers {
		s.closeWatcherLocked(id, watcher, nil)
	}
}

func (s *eventStream[T]) unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if watcher := s.watchers[id]; watcher != nil {
		s.closeWatcherLocked(id, watcher, nil)
	}

	if s.subscriptions > 0 {
		s.subscriptions--
	}
}

func (s *eventStream[T]) closeWatcherLocked(id uint64, watcher *eventWatcher[T], err error) {
	if err != nil {
		watcher.errors <- err
	}

	close(watcher.events)
	close(watcher.errors)
	delete(s.watchers, id)
}

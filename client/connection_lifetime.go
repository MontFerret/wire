package client

// ConnectionLifetime observes the logical Wire connection of a runtime returned
// by New or From. Runtime.Close can leave this connection alive for retained plans.
type ConnectionLifetime interface {
	// ConnectionDone closes when the Connect reader ends, before final cleanup.
	ConnectionDone() <-chan struct{}
	// ConnectionError returns the retained cause of unexpected connection loss.
	// An unexpected EOF yields ErrClosed. Before loss and for locally initiated
	// teardown it returns nil. Read it after ConnectionDone for the final cause.
	ConnectionError() error
}

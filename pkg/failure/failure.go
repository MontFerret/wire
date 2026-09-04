package failure

import "github.com/MontFerret/api/diagnostics"

type (
	// Category identifies a failure category carried explicitly by the Wire
	// protocol. Transport-native conditions such as cancellation are not
	// represented here.
	Category uint8

	// Failure is a sanitized terminal execution or debug failure.
	Failure struct {
		Category    Category
		Message     string
		Diagnostics diagnostics.Diagnostics
	}
)

const (
	CategoryCompilation Category = iota + 1
	CategoryExecution
	CategoryPlanNotFound
	CategoryExecutionNotFound
	CategoryDebugSessionNotFound
	CategoryConnectionNotFound
	CategoryInvalidState
	CategoryInternalRuntime
	CategoryWatcherLagged
	CategoryBreakpointNotFound
)

// Error returns the sanitized terminal failure message.
func (f *Failure) Error() string {
	if f == nil {
		return ""
	}

	return f.Message
}

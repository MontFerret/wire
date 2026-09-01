package core

func failureFromError(category ErrorCategory) *Failure {
	if category == ErrorExecution {
		return &Failure{Category: ErrorExecution, Message: "runtime execution failed"}
	}

	return &Failure{Category: ErrorInternal, Message: "internal runtime failure"}
}

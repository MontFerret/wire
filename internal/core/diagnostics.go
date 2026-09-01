package core

func failureFromError(category ErrorCategory) *Failure {
	if category == ErrorExecution {
		return &Failure{Category: ErrorExecution, Message: "runtime execution failed"}
	}

	return &Failure{Category: ErrorInternal, Message: "internal runtime failure"}
}

func cloneDiagnostics(values []Diagnostic) []Diagnostic {
	result := make([]Diagnostic, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Spans = append([]DiagnosticSpan(nil), value.Spans...)
	}

	return result
}

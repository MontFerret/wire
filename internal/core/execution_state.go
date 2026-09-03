package core

type (
	ExecutionState uint8

	Output struct {
		ContentType string
		Content     []byte
	}

	Failure struct {
		Category ErrorCategory
		Message  string
	}

	ExecutionSnapshot struct {
		ID      ExecutionID
		PlanID  PlanID
		State   ExecutionState
		Output  *Output
		Failure *Failure
	}
)

const (
	ExecutionRunning ExecutionState = iota + 1
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
)

func (s ExecutionSnapshot) clone() ExecutionSnapshot {
	result := s

	if s.Output != nil {
		result.Output = &Output{ContentType: s.Output.ContentType, Content: append([]byte(nil), s.Output.Content...)}
	}

	if s.Failure != nil {
		result.Failure = &Failure{Category: s.Failure.Category, Message: s.Failure.Message}
	}

	return result
}

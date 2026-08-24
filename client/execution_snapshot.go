package client

// ExecutionSnapshot is the state published for one remote execution event.
type ExecutionSnapshot struct {
	State   ExecutionState
	Output  *Output
	Failure *Failure
}

func (snapshot ExecutionSnapshot) output() Output {
	if snapshot.Output == nil {
		return Output{}
	}

	return Output{
		ContentType: snapshot.Output.ContentType,
		Content:     append([]byte(nil), snapshot.Output.Content...),
	}
}

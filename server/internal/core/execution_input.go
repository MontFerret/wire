package core

import (
	"github.com/MontFerret/api"
)

type ExecuteInput struct {
	PlanID            PlanID
	Parameters        map[string]any
	OutputContentType string
}

type RunInput struct {
	Source            api.Source
	Parameters        map[string]any
	OutputContentType string
}

package core

type OpenDebugInput struct {
	PlanID            PlanID
	Parameters        map[string]any
	OutputContentType string
}

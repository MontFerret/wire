package core

type CreateSessionInput struct {
	PlanID            PlanID
	Parameters        map[string]any
	OutputContentType string
}

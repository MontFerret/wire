package execution

// Identity describes optional host-supplied identity for a hosted runtime.
type Identity struct {
	Name       string
	Version    string
	InstanceID string
}

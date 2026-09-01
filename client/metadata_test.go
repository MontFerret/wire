package client

import "testing"

func TestRuntimeInfoReturnsDefensiveIdentityCopy(t *testing.T) {
	client := &Client{info: RuntimeInfo{
		APIIdentity:     "ferret.wire",
		WireVersion:     "v1",
		RuntimeIdentity: &RuntimeIdentity{Name: "host", Version: "1.0.0", InstanceID: "instance"},
	}}

	first := client.RuntimeInfo()
	first.RuntimeIdentity.Name = "changed"
	second := client.RuntimeInfo()

	if second.RuntimeIdentity == nil || second.RuntimeIdentity.Name != "host" || second.APIIdentity != "ferret.wire" ||
		second.WireVersion != "v1" || second.FerretVersion != "" || second.Capabilities != (Capabilities{}) {
		t.Fatalf("RuntimeInfo returned mutable client metadata: %#v", second)
	}
}

package client

import (
	"context"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type planRPCStub struct {
	compileRequest  *wirev1.CompileRequest
	compileResponse *wirev1.CompileResponse
	compileErr      error
	releaseRequest  *wirev1.ReleasePlanRequest
}

func (c *planRPCStub) Compile(
	_ context.Context,
	request *wirev1.CompileRequest,
	_ ...grpc.CallOption,
) (*wirev1.CompileResponse, error) {
	c.compileRequest = request

	return c.compileResponse, c.compileErr
}

func (c *planRPCStub) ReleasePlan(
	_ context.Context,
	request *wirev1.ReleasePlanRequest,
	_ ...grpc.CallOption,
) (*wirev1.ReleasePlanResponse, error) {
	c.releaseRequest = request

	return &wirev1.ReleasePlanResponse{}, nil
}

func TestPlanTransportBuildsRequestsAndReturnsDomainMetadata(t *testing.T) {
	implementation := &planRPCStub{compileResponse: &wirev1.CompileResponse{Plan: &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: "plan"},
		Parameters: []string{"input"},
		Debuggable: true,
	}}}
	transport := &planTransport{session: &session{id: "connection"}, rpc: implementation}

	compiled, err := transport.compile(
		context.Background(),
		Source{Content: "RETURN @input", Identity: "query.fql"},
		CompileOptions{Debuggable: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	request := implementation.compileRequest
	if request.GetConnectionId().GetValue() != "connection" || request.GetSource().GetContent() != "RETURN @input" ||
		request.GetSource().GetIdentity() != "query.fql" || !request.GetOptions().GetDebuggable() {
		t.Fatalf("unexpected compile request: %#v", request)
	}

	if compiled.id != "plan" || len(compiled.parameters) != 1 || compiled.parameters[0] != "input" || !compiled.debuggable {
		t.Fatalf("unexpected compiled plan: %#v", compiled)
	}

	implementation.compileResponse.Plan.Parameters[0] = "changed"
	if compiled.parameters[0] != "input" {
		t.Fatalf("compiled metadata retained protobuf storage: %v", compiled.parameters)
	}

	if err := transport.release(context.Background(), "plan"); err != nil {
		t.Fatal(err)
	}

	if implementation.releaseRequest.GetConnectionId().GetValue() != "connection" ||
		implementation.releaseRequest.GetPlanId().GetValue() != "plan" {
		t.Fatalf("unexpected release request: %#v", implementation.releaseRequest)
	}
}

func TestPlanTransportRejectsInvalidAndRemoteResponses(t *testing.T) {
	tests := []struct {
		name     string
		response *wirev1.CompileResponse
		err      error
		message  string
	}{
		{name: "missing plan", response: &wirev1.CompileResponse{}, message: "Wire server returned an invalid compiled plan"},
		{name: "missing plan ID", response: &wirev1.CompileResponse{Plan: &wirev1.Plan{}}, message: "Wire server returned an invalid compiled plan"},
		{name: "remote failure", err: status.Error(codes.Unavailable, "compile unavailable"), message: "compile unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			implementation := &planRPCStub{compileResponse: test.response, compileErr: test.err}
			transport := &planTransport{session: &session{id: "connection"}, rpc: implementation}

			_, err := transport.compile(context.Background(), Source{}, CompileOptions{})
			if err == nil || err.Error() != test.message {
				t.Fatalf("compile error = %v", err)
			}
		})
	}
}

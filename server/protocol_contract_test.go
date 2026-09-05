package server_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestProtocolDescriptorsReserveRemovedV1Surface(t *testing.T) {
	tests := []struct {
		message  protoreflect.MessageDescriptor
		fields   []protoreflect.Name
		numbers  []protoreflect.FieldNumber
		reserved []protoreflect.Name
	}{
		{
			message:  wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("ConnectResponse"),
			fields:   []protoreflect.Name{"connection_id", "protocol", "runtime_identity"},
			numbers:  []protoreflect.FieldNumber{1, 2},
			reserved: []protoreflect.Name{"opened", "closing"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_source_proto.Messages().ByName("Source"),
			fields:   []protoreflect.Name{"content", "name"},
			numbers:  []protoreflect.FieldNumber{2},
			reserved: []protoreflect.Name{"identity"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_source_proto.Messages().ByName("Location"),
			fields:   []protoreflect.Name{"source_name", "position"},
			reserved: []protoreflect.Name{"file"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("Diagnostic"),
			fields:   []protoreflect.Name{"kind", "message", "hint", "note", "source", "annotations"},
			numbers:  []protoreflect.FieldNumber{5, 6},
			reserved: []protoreflect.Name{"source_identity", "spans"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("Failure"),
			fields:   []protoreflect.Name{"category", "message", "diagnostic_set"},
			numbers:  []protoreflect.FieldNumber{3},
			reserved: []protoreflect.Name{"diagnostics"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_plan_proto.Messages().ByName("CompileOptions"),
			fields:   []protoreflect.Name{"optimization_level"},
			numbers:  []protoreflect.FieldNumber{1},
			reserved: []protoreflect.Name{"debuggable"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_plan_proto.Messages().ByName("Plan"),
			fields:   []protoreflect.Name{"id", "parameters"},
			numbers:  []protoreflect.FieldNumber{3},
			reserved: []protoreflect.Name{"debuggable"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_execution_proto.Messages().ByName("Execution"),
			fields:   []protoreflect.Name{"id", "state", "output", "failure"},
			numbers:  []protoreflect.FieldNumber{2},
			reserved: []protoreflect.Name{"plan_id"},
		},
		{
			message: wirev1.File_ferret_wire_v1_session_proto.Messages().ByName("CreateSessionRequest"),
			fields:  []protoreflect.Name{"connection_id", "plan_id", "parameters", "output_content_type"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_execution_proto.Messages().ByName("WatchExecutionResponse"),
			fields:   []protoreflect.Name{"sequence", "execution"},
			numbers:  []protoreflect.FieldNumber{1, 3, 4, 5, 6, 7},
			reserved: []protoreflect.Name{"execution_id", "output", "started", "completed", "failed", "cancelled"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_debug_proto.Messages().ByName("DebugSession"),
			fields:   []protoreflect.Name{"id", "state", "stop_reason", "location", "hit_breakpoint_ids", "output", "failure", "depth"},
			numbers:  []protoreflect.FieldNumber{2, 5},
			reserved: []protoreflect.Name{"plan_id"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_debug_proto.Messages().ByName("Breakpoint"),
			fields:   []protoreflect.Name{"id", "requested_location", "location", "point_id", "function_id", "binding_mode", "bound"},
			numbers:  []protoreflect.FieldNumber{2, 3, 4, 5, 6, 7},
			reserved: []protoreflect.Name{"file", "requested_line", "requested_column", "line", "column", "verified"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_debug_proto.Messages().ByName("Frame"),
			fields:   []protoreflect.Name{"name", "location", "function_id"},
			numbers:  []protoreflect.FieldNumber{1, 3},
			reserved: []protoreflect.Name{"index"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_debug_proto.Messages().ByName("WatchDebugResponse"),
			fields:   []protoreflect.Name{"sequence", "kind", "session"},
			numbers:  []protoreflect.FieldNumber{1, 3, 4, 5, 6, 7, 8, 9},
			reserved: []protoreflect.Name{"debug_session_id", "output", "started", "continued", "stopped", "completed", "failed", "terminated"},
		},
		{
			message:  wirev1.File_ferret_wire_v1_value_proto.Messages().ByName("Value"),
			fields:   []protoreflect.Name{"null_value", "boolean_value", "integer_value", "float_value", "string_value", "binary_value", "array_value", "object_value"},
			numbers:  []protoreflect.FieldNumber{7, 8, 9},
			reserved: []protoreflect.Name{"none_value", "duration_nanos", "datetime_value", "regexp_value"},
		},
	}

	for _, test := range tests {
		if test.message == nil {
			t.Fatal("protocol message descriptor is missing")
		}

		for _, name := range test.fields {
			if test.message.Fields().ByName(name) == nil {
				t.Errorf("%s.%s is missing", test.message.FullName(), name)
			}
		}
		for _, number := range test.numbers {
			if !test.message.ReservedRanges().Has(number) {
				t.Errorf("%s does not reserve field %d", test.message.FullName(), number)
			}
		}
		for _, name := range test.reserved {
			if !test.message.ReservedNames().Has(name) {
				t.Errorf("%s does not reserve field name %s", test.message.FullName(), name)
			}
			if test.message.Fields().ByName(name) != nil {
				t.Errorf("%s still declares removed field %s", test.message.FullName(), name)
			}
		}
	}

	if wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("RuntimeInfo") != nil ||
		wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("ConnectionOpened") != nil ||
		wirev1.File_ferret_wire_v1_runtime_proto.Enums().ByName("Capability") != nil ||
		wirev1.File_ferret_wire_v1_runtime_proto.Enums().ByName("ResourceKind") != nil {
		t.Fatal("removed native runtime metadata remains in the descriptor")
	}

	errorCategory := wirev1.File_ferret_wire_v1_runtime_proto.Enums().ByName("ErrorCategory")
	for _, number := range []protoreflect.EnumNumber{1, 9, 12, 13, 14} {
		if !errorCategory.ReservedRanges().Has(number) {
			t.Errorf("ErrorCategory does not reserve value %d", number)
		}
	}
	for _, name := range []protoreflect.Name{
		"ERROR_CATEGORY_INVALID_REQUEST",
		"ERROR_CATEGORY_UNSUPPORTED_CAPABILITY",
		"ERROR_CATEGORY_CANCELLED",
		"ERROR_CATEGORY_VALUE_REFERENCE_NOT_FOUND",
		"ERROR_CATEGORY_RESOURCE_EXHAUSTED",
	} {
		if !errorCategory.ReservedNames().Has(name) {
			t.Errorf("ErrorCategory does not reserve value name %s", name)
		}
	}

	if errorCategory.Values().ByName("ERROR_CATEGORY_VALUE_REFERENCE_NOT_FOUND") != nil {
		t.Error("ErrorCategory still declares the removed value-reference category")
	}

	if sessionNotFound := errorCategory.Values().ByName("ERROR_CATEGORY_SESSION_NOT_FOUND"); sessionNotFound == nil || sessionNotFound.Number() != 16 {
		t.Error("ErrorCategory does not expose SESSION_NOT_FOUND at value 16")
	}

	location := wirev1.File_ferret_wire_v1_source_proto.Messages().ByName("Location")
	if location.Fields().ByName("source_name").Number() != 1 || location.Fields().ByName("position").Number() != 2 {
		t.Error("Location field numbers changed")
	}

	diagnostic := wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("Diagnostic")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"kind": 1, "message": 2, "hint": 3, "note": 4, "source": 7, "annotations": 8,
	} {
		if field := diagnostic.Fields().ByName(name); field == nil || field.Number() != number {
			t.Errorf("Diagnostic.%s does not use field %d", name, number)
		}
	}
	annotation := wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("DiagnosticAnnotation")
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"range": 1, "message": 2, "primary": 3,
	} {
		if field := annotation.Fields().ByName(name); field == nil || field.Number() != number {
			t.Errorf("DiagnosticAnnotation.%s does not use field %d", name, number)
		}
	}
	diagnosticSet := wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("DiagnosticSet")
	if field := diagnosticSet.Fields().ByName("diagnostics"); field == nil || field.Number() != 1 {
		t.Error("DiagnosticSet.diagnostics does not use field 1")
	}
	failure := wirev1.File_ferret_wire_v1_runtime_proto.Messages().ByName("Failure")
	if field := failure.Fields().ByName("diagnostic_set"); field == nil || field.Number() != 4 {
		t.Error("Failure.diagnostic_set does not use field 4")
	}

	debugEventKind := wirev1.File_ferret_wire_v1_debug_proto.Enums().ByName("DebugEventKind")
	if created := debugEventKind.Values().ByName("DEBUG_EVENT_KIND_CREATED"); created == nil || created.Number() != 7 {
		t.Error("DebugEventKind does not expose CREATED at value 7")
	}
	breakpointMode := wirev1.File_ferret_wire_v1_debug_proto.Enums().ByName("BreakpointBindingMode")
	if next := breakpointMode.Values().ByName("BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE"); next == nil || next.Number() != 1 {
		t.Error("BreakpointBindingMode does not expose source-neutral default at value 1")
	}

	if !breakpointMode.ReservedNames().Has("BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FILE") ||
		breakpointMode.Values().ByName("BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FILE") != nil {
		t.Error("BreakpointBindingMode does not reserve the removed file-specific name")
	}

	debugMessages := wirev1.File_ferret_wire_v1_debug_proto.Messages()
	for _, operation := range []string{"Continue", "Pause", "StepOver", "StepIn", "StepOut"} {
		request := debugMessages.ByName(protoreflect.Name(operation + "Request"))
		response := debugMessages.ByName(protoreflect.Name(operation + "Response"))
		if request == nil || !request.ReservedRanges().Has(1) || !request.ReservedNames().Has("command") {
			t.Errorf("%sRequest does not reserve the removed command envelope", operation)
		}
		if response == nil || !response.ReservedRanges().Has(1) || !response.ReservedNames().Has("session") {
			t.Errorf("%sResponse does not reserve the removed session snapshot", operation)
		}
	}
	for _, removed := range []protoreflect.Name{"NextRequest", "NextResponse", "StepRequest", "StepResponse", "OutRequest", "OutResponse"} {
		if debugMessages.ByName(removed) != nil {
			t.Errorf("DebugService still declares removed envelope %s", removed)
		}
	}
	setBreakpoint := debugMessages.ByName("SetBreakpointRequest")
	if setBreakpoint == nil || !setBreakpoint.ReservedRanges().Has(3) {
		t.Error("SetBreakpointRequest does not reserve the old SourceLocation field tag")
	}

	runtimeMethods := wirev1.File_ferret_wire_v1_runtime_service_proto.Services().ByName("RuntimeService").Methods()
	if runtimeMethods.ByName("Run") == nil {
		t.Error("RuntimeService is missing the hosted Runtime.Run operation")
	}

	if wirev1.RuntimeService_Run_FullMethodName != "/ferret.wire.v1.RuntimeService/Run" ||
		wirev1.RuntimeService_Connect_FullMethodName != "/ferret.wire.v1.RuntimeService/Connect" ||
		wirev1.RuntimeService_CloseConnection_FullMethodName != "/ferret.wire.v1.RuntimeService/CloseConnection" {
		t.Error("RuntimeService RPC paths changed")
	}

	if runtimeMethods.Len() != 3 || !runtimeMethods.ByName("Connect").IsStreamingServer() ||
		runtimeMethods.ByName("Run").IsStreamingServer() || runtimeMethods.ByName("Run").IsStreamingClient() {
		t.Error("RuntimeService RPC or streaming contract changed")
	}

	runMessages := wirev1.File_ferret_wire_v1_runtime_service_proto.Messages()
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"connection_id": 1, "source": 2, "parameters": 3, "output_content_type": 4,
	} {
		field := runMessages.ByName("RunRequest").Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Errorf("RunRequest.%s does not use field %d", name, number)
		}
	}

	runExecution := runMessages.ByName("RunResponse").Fields().ByName("execution")
	if runExecution == nil || runExecution.Number() != 1 || runExecution.Message().FullName() != "ferret.wire.v1.Execution" {
		t.Error("RunResponse does not preserve the Execution response")
	}
	planMethods := wirev1.File_ferret_wire_v1_plan_proto.Services().ByName("PlanService").Methods()
	if planMethods.ByName("Compile") == nil || planMethods.ByName("CompileDebug") == nil {
		t.Error("PlanService does not expose distinct normal and debug compilation")
	}
	sessionMethods := wirev1.File_ferret_wire_v1_session_proto.Services().ByName("SessionService").Methods()
	for _, required := range []protoreflect.Name{"CreateSession", "ReleaseSession"} {
		if sessionMethods.ByName(required) == nil {
			t.Errorf("SessionService is missing RPC %s", required)
		}
	}
	executionMethods := wirev1.File_ferret_wire_v1_execution_proto.Services().ByName("ExecutionService").Methods()
	if executionMethods.Len() != 5 {
		t.Error("ExecutionService retained a direct Runtime invocation RPC")
	}
	for _, required := range []protoreflect.Name{"Execute", "RunSession"} {
		if executionMethods.ByName(required) == nil {
			t.Errorf("ExecutionService is missing RPC %s", required)
		}
	}
	debugMethods := wirev1.File_ferret_wire_v1_debug_proto.Services().ByName("DebugService").Methods()
	for _, required := range []protoreflect.Name{"StepOver", "StepIn", "StepOut"} {
		if debugMethods.ByName(required) == nil {
			t.Errorf("DebugService is missing RPC %s", required)
		}
	}
	for _, removed := range []protoreflect.Name{"OpenDebugSession", "StartDebug", "StopDebug", "Next", "Step", "Out"} {
		if debugMethods.ByName(removed) != nil {
			t.Errorf("DebugService still exposes removed RPC %s", removed)
		}
	}
}

func TestProtocolSourcesContainNoNativeMetadataOrFakeCapabilities(t *testing.T) {
	banned := []string{
		"ferret_version",
		"module_build",
		"message DiagnosticSpan",
		"message ConnectionOpened",
		"enum Capability",
		"enum ResourceKind",
	}
	files, err := filepath.Glob(filepath.Join("..", "proto", "ferret", "wire", "v1", "*.proto"))
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Fatal("protocol sources are missing")
	}

	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		for _, token := range banned {
			if strings.Contains(string(content), token) {
				t.Errorf("%s still contains removed native protocol token %q", file, token)
			}
		}
	}
}

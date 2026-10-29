package client

import (
	"errors"
	"io"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

func TestOutputMalformedFramesFinalizeAndKeepPrefix(t *testing.T) {
	for _, test := range []struct {
		name string
		end  *wirev1.OutputFrame
	}{
		{"missing terminal", nil},
		{"duplicate header", outputstream.Header(api.Metadata{})},
		{"bad accounting", outputstream.End(&wirev1.OutputTerminal{TransferredBytes: 100})},
	} {
		t.Run(test.name, func(t *testing.T) {
			frames := []*wirev1.OutputFrame{outputstream.Header(api.Metadata{Length: 1 << 62, LengthKnown: true}), {Frame: &wirev1.OutputFrame_Chunk{Chunk: []byte("prefix")}}}

			if test.end != nil {
				frames = append(frames, test.end)
			}

			server := &clientTestServer{outputScripts: []executionOutputScript{{frames: frames}}}
			remote := openTestRuntime(t, startClientTestServer(t, server))

			output, err := remote.Run(testClientContext(t), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			content, err := output.Collect(testClientContext(t))
			if err == nil || content == nil || string(content.Data) != "prefix" {
				t.Fatalf("content=%v error=%v", content, err)
			}

			if test.end == nil && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("missing terminal=%v", err)
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}

			_, _, releases, _ := server.callSnapshot()
			if releases != 1 {
				t.Fatalf("releases=%d", releases)
			}
		})
	}
}

func TestDebuggerContentSingleTransferAndDetachedProjections(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("result")} {
		metadata := api.Metadata{Length: int64(len(data)), LengthKnown: true}
		event := wiredebugger.Event{Snapshot: wiredebugger.Snapshot{
			Output:        &api.Content{Metadata: metadata},
			CommandResult: &wiredebugger.CommandResult{Event: &debugger.Event{Output: &api.Content{Metadata: metadata}}},
		}}
		frames := []*wirev1.OutputFrame{outputstream.Header(metadata), {Frame: &wirev1.OutputFrame_Chunk{Chunk: data}}, outputstream.End(&wirev1.OutputTerminal{TransferredBytes: uint64(len(data))})}
		index := 0

		err := receiveDebugEventContent(&event, func() (*wirev1.OutputFrame, error) {
			if index == len(frames) {
				return nil, io.EOF
			}

			frame := frames[index]
			index++

			return frame, nil
		})
		if err != nil || index != 3 || event.Snapshot.Output == nil || event.Snapshot.CommandResult.Event.Output == nil {
			t.Fatalf("event=%v frames=%d error=%v", event, index, err)
		}

		if len(data) > 0 {
			event.Snapshot.Output.Data[0] = 'X'
			if string(event.Snapshot.CommandResult.Event.Output.Data) != "result" || string(data) != "result" {
				t.Fatal("content projections alias each other or transport storage")
			}
		}
	}
}

func TestDebuggerContentRejectsInterruptedReconstruction(t *testing.T) {
	metadata := api.Metadata{Length: 10, LengthKnown: true}
	semantic := errors.New("already observed semantic failure")
	event := wiredebugger.Event{Snapshot: wiredebugger.Snapshot{Output: &api.Content{Metadata: metadata}, CommandResult: &wiredebugger.CommandResult{Error: semantic}}}
	index := 0
	frames := []*wirev1.OutputFrame{outputstream.Header(metadata), {Frame: &wirev1.OutputFrame_Chunk{Chunk: []byte("part")}}}

	err := receiveDebugEventContent(&event, func() (*wirev1.OutputFrame, error) {
		if index == len(frames) {
			return nil, io.EOF
		}

		frame := frames[index]
		index++

		return frame, nil
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, semantic) || len(event.Snapshot.Output.Data) != 0 {
		t.Fatalf("incomplete reconstruction=%v error=%v", event, err)
	}
}

func TestOutputMalformedTerminalFailureReachesClient(t *testing.T) {
	frames := completedOutputFrames("text/plain", []byte("prefix"))
	frames[2].GetTerminal().Failure = &wirev1.Failure{Category: wirev1.ErrorCategory(999), Message: "invalid failure category"}
	server := &clientTestServer{outputScripts: []executionOutputScript{{frames: frames}}}
	remote := openTestRuntime(t, startClientTestServer(t, server))

	output, err := remote.Run(testClientContext(t), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	content, err := output.Collect(testClientContext(t))

	var rpc *Error
	if err == nil || errors.As(err, &rpc) || content == nil || string(content.Data) != "prefix" {
		t.Fatalf("client validation content=%v error=%v", content, err)
	}

	_, _, releases, _ := server.callSnapshot()
	if releases != 1 {
		t.Fatalf("releases=%d", releases)
	}
}

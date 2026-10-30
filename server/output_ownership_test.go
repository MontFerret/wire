package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	"github.com/MontFerret/wire/server"
)

type (
	retainedOutputMessage struct {
		message  proto.Message
		expected proto.Message
		frame    *wirev1.OutputFrame
	}

	retainedOutputStream struct {
		grpc.ServerStream
		messages *[]retainedOutputMessage
	}
)

// Keep the original pointers after SendMsg returns, as a transport observer may.
func (s *retainedOutputStream) SendMsg(message any) error {
	var frame *wirev1.OutputFrame
	switch value := message.(type) {
	case *wirev1.ReadExecutionOutputResponse:
		frame = value.GetFrame()
	case *wirev1.RunCommandResponse:
		frame = value.GetOutputFrame()
	case *wirev1.WatchDebugResponse:
		frame = value.GetOutputFrame()
	}

	if frame != nil {
		value := message.(proto.Message)
		*s.messages = append(*s.messages, retainedOutputMessage{message: value, expected: proto.Clone(value), frame: frame})
	}

	return s.ServerStream.SendMsg(message)
}

func TestOutputMessagesRemainStableAfterDelivery(t *testing.T) {
	for _, method := range []string{"execution", "command", "watch"} {
		for _, kind := range []string{"multiple", "empty", "absent", "partial"} {
			for _, limit := range []int{256, 1024} {
				t.Run(fmt.Sprintf("%s/%s/%d", method, kind, limit), func(t *testing.T) {
					payload := make([]byte, limit*3+17)
					for i := range payload {
						payload[i] = byte(i % 251)
					}

					if kind == "empty" || kind == "absent" {
						payload = nil
					}

					metadata := api.Metadata{ContentType: "application/octet-stream", Length: int64(len(payload)), LengthKnown: true}
					var failure error

					if kind == "partial" {
						metadata.Length += 33
						failure = errors.New("private output failure")
					}

					output := &transportOutput{metadata: metadata, data: payload, failure: failure}
					hosted := &transportDebugger{content: &api.Content{Metadata: metadata, Data: payload}, failure: failure}

					if kind == "absent" {
						output.consume = func(context.Context, api.Consumer) error { return nil }
						hosted.content = nil
					}

					fullMethod := map[string]string{
						"execution": wirev1.ExecutionService_ReadExecutionOutput_FullMethodName,
						"command":   wirev1.DebugService_RunCommand_FullMethodName,
						"watch":     wirev1.DebugService_WatchDebug_FullMethodName,
					}[method]
					var retained []retainedOutputMessage
					finished := make(chan struct{})
					finish := sync.OnceFunc(func() { close(finished) })
					f := newOutputTransportFixture(t, output, hosted, limit, server.WithStreamInterceptors(func(host any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
						if info.FullMethod != fullMethod {
							return next(host, stream)
						}

						defer finish()

						return next(host, &retainedOutputStream{ServerStream: stream, messages: &retained})
					}))
					var receive func() (*wirev1.OutputFrame, error)
					var release func()

					if method == "execution" {
						receive, release = f.execution(t)
					} else {
						receive, release = f.debugger(t, method == "watch")
					}

					defer release()

					var decoder outputstream.Decoder
					var actual []byte
					var terminal *wirev1.OutputTerminal
					var chunkSizes []int
					for {
						frame, err := receive()
						if errors.Is(err, io.EOF) {
							break
						}

						if err != nil {
							t.Fatal(err)
						}

						data, present, end, err := decoder.Accept(frame)
						if err != nil {
							t.Fatal(err)
						}

						if present {
							chunkSizes = append(chunkSizes, len(data))
							actual = append(actual, data...)
						}

						if end != nil {
							terminal = end
						}
					}

					select {
					case <-finished:
					case <-f.ctx.Done():
						t.Fatal("output handler did not finish")
					}

					if !bytes.Equal(actual, payload) || kind == "absent" && len(chunkSizes) != 0 || kind == "empty" && len(chunkSizes) != 1 || len(payload) > 0 && len(chunkSizes) < 3 {
						t.Fatalf("chunks=%v bytes=%d want=%d", chunkSizes, len(actual), len(payload))
					}

					if len(payload) > 0 && chunkSizes[len(chunkSizes)-1] >= chunkSizes[0] {
						t.Fatalf("missing short final chunk: %v", chunkSizes)
					}

					if kind == "absent" && method != "execution" && len(retained) != 0 {
						t.Fatal("absent debugger content emitted output frames")
					}

					if kind != "absent" || method == "execution" {
						if decoder.Metadata() != metadata || terminal == nil || terminal.GetTransferredBytes() != uint64(len(payload)) || (terminal.GetFailure() != nil) != (kind == "partial") {
							t.Fatalf("metadata=%v terminal=%v", decoder.Metadata(), terminal)
						}
					}

					// Producer storage and every earlier message must be independent.
					clear(payload)
					messages := make(map[proto.Message]bool)
					frames := make(map[*wirev1.OutputFrame]bool)
					wrappers := make(map[*wirev1.OutputFrame_Chunk]bool)
					buffers := make(map[*byte]bool)
					for _, value := range retained {
						if messages[value.message] || frames[value.frame] || !proto.Equal(value.message, value.expected) || proto.Size(value.message) > limit {
							t.Fatal("reused, mutated, or oversized output message")
						}

						messages[value.message], frames[value.frame] = true, true
						if chunk, ok := value.frame.GetFrame().(*wirev1.OutputFrame_Chunk); ok {
							if wrappers[chunk] {
								t.Fatal("reused chunk wrapper")
							}

							wrappers[chunk] = true
							if len(chunk.Chunk) > 0 {
								if buffers[&chunk.Chunk[0]] {
									t.Fatal("reused payload storage")
								}

								buffers[&chunk.Chunk[0]] = true
							}
						}
					}

					if method == "execution" && output.closes.Load() != 1 {
						t.Fatal("output was not closed exactly once")
					}
				})
			}
		}
	}
}

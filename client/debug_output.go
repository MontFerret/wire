package client

import (
	"errors"
	"io"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

// receiveDebugContent exposes content only after a complete framed transfer.
// Hosted partial results remain valid content; an interrupted transport never
// publishes a truncated debugger completion as though reconstruction succeeded.
func receiveDebugContent(metadata api.Metadata, receive func() (*wirev1.OutputFrame, error)) (*api.Content, error) {
	var decoder outputstream.Decoder
	content := outputstream.Accumulator{Metadata: metadata}
	first := true
	for {
		frame, err := receive()
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		if err != nil {
			return nil, decodeError(err)
		}

		data, chunk, terminal, err := decoder.Accept(frame)
		if err != nil {
			return nil, err
		}

		if first {
			if frame.GetHeader() == nil || decoder.Metadata() != metadata {
				return nil, invalidDebuggerResponse("output metadata changed during transfer")
			}

			first = false
		}

		if chunk {
			content.Add(data)
		}

		if terminal != nil {
			if _, err := convertFailure(terminal.GetFailure()); err != nil {
				return nil, err
			}

			if content.Content() == nil {
				return nil, invalidDebuggerResponse("present debugger output has no content chunk")
			}

			return content.Content(), nil
		}
	}
}

func commandResultError(result *wiredebugger.CommandResult) error {
	if result == nil {
		return nil
	}

	if result.Event == nil {
		return result.Error
	}

	return errors.Join(result.Error, result.Event.Error)
}

func detachContent(content *api.Content) *api.Content {
	if content == nil {
		return nil
	}

	return &api.Content{Metadata: content.Metadata, Data: append(content.Data[:0:0], content.Data...)}
}

func receiveDebugEventContent(event *wiredebugger.Event, receive func() (*wirev1.OutputFrame, error)) error {
	snapshot := &event.Snapshot
	descriptor := snapshot.Output
	var commandContent *api.Content

	if snapshot.CommandResult != nil && snapshot.CommandResult.Event != nil {
		commandContent = snapshot.CommandResult.Event.Output
	}

	if descriptor == nil {
		descriptor = commandContent
	}

	if descriptor == nil {
		return nil
	}

	if commandContent != nil && descriptor.Metadata != commandContent.Metadata {
		return invalidDebuggerResponse("conflicting output descriptors")
	}

	content, err := receiveDebugContent(descriptor.Metadata, receive)
	if err != nil {
		var failureErr error

		if snapshot.Failure != nil {
			failureErr = snapshot.Failure
		}

		return errors.Join(err, failureErr, commandResultError(snapshot.CommandResult))
	}

	if snapshot.Output != nil {
		snapshot.Output = content
	}

	if commandContent != nil {
		snapshot.CommandResult.Event.Output = detachContent(content)
	}

	return nil
}

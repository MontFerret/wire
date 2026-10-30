// Package outputstream implements Wire's encoded-output framing. It owns no
// runtime resources and does not interpret encoded payloads.
package outputstream

import (
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// Version is required in both directions of the Connect handshake.
const Version uint32 = 1

// ChunkSize is the preferred maximum encoded chunk, before envelope overhead.
const ChunkSize = 64 << 10

var sizingBytes [ChunkSize]byte

// Metadata validates a descriptor without interpreting unknown lengths.
func Metadata(value *wirev1.OutputMetadata) (api.Metadata, error) {
	if value == nil || value.GetLengthKnown() && value.GetLength() < 0 {
		return api.Metadata{}, errors.New("invalid output metadata")
	}

	return api.Metadata{ContentType: value.GetContentType(), Length: value.GetLength(), LengthKnown: value.GetLengthKnown()}, nil
}

// Descriptor preserves the original complete-payload metadata.
func Descriptor(value api.Metadata) *wirev1.OutputMetadata {
	return &wirev1.OutputMetadata{ContentType: value.ContentType, Length: value.Length, LengthKnown: value.LengthKnown}
}

// Header starts a transfer without establishing whether content is present.
func Header(value api.Metadata) *wirev1.OutputFrame {
	return &wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Header{Header: Descriptor(value)}}
}

// End commits the number of bytes actually transferred, independently of metadata.
func End(value *wirev1.OutputTerminal) *wirev1.OutputFrame {
	return &wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Terminal{Terminal: value}}
}

// MaxChunk includes the concrete RPC envelope in the message-size calculation.
func MaxChunk(limit int, envelope func(*wirev1.OutputFrame) proto.Message) (int, error) {
	data := sizingBytes[:]
	if proto.Size(envelope(&wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Chunk{Chunk: data}})) <= limit {
		return ChunkSize, nil
	}

	low, high := 0, ChunkSize
	for low < high {
		mid := (low + high + 1) / 2

		message := envelope(&wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Chunk{Chunk: data[:mid]}})
		if proto.Size(message) <= limit {
			low = mid
		} else {
			high = mid - 1
		}
	}

	if low == 0 {
		return 0, errors.New("outbound message limit cannot fit an output chunk")
	}

	return low, nil
}

// Chunks sends borrowed bytes synchronously. Even empty input emits one chunk.
func Chunks(data []byte, size int, send func(*wirev1.OutputFrame) error) error {
	if size <= 0 {
		return errors.New("output chunk size must be positive")
	}

	for {
		n := min(len(data), size)
		owner := &chunkFrame{}
		owner.chunk.Chunk = data[:n]
		owner.frame.Frame = &owner.chunk

		if err := send(&owner.frame); err != nil {
			return err
		}

		data = data[n:]
		if len(data) == 0 {
			return nil
		}
	}
}

type (
	// Each frame and its wrapper share one fresh allocation. Initialize them in
	// place: protobuf state must not be copied, and send may retain the frame.
	chunkFrame struct {
		frame wirev1.OutputFrame
		chunk wirev1.OutputFrame_Chunk
	}

	// Decoder validates one ordered transfer. It retains only metadata and counters;
	// callers decide whether to deliver borrowed chunks or accumulate detached data.
	Decoder struct {
		metadata api.Metadata
		header   bool
		ended    bool
		bytes    uint64
	}
)

// Metadata returns the immutable descriptor received in the header.
func (d *Decoder) Metadata() api.Metadata { return d.metadata }

// Accept returns either a chunk or the terminal result. A header returns neither.
func (d *Decoder) Accept(frame *wirev1.OutputFrame) ([]byte, bool, *wirev1.OutputTerminal, error) {
	if frame == nil || d.ended {
		return nil, false, nil, errors.New("invalid output frame ordering")
	}

	switch value := frame.GetFrame().(type) {
	case *wirev1.OutputFrame_Header:
		if d.header {
			return nil, false, nil, errors.New("duplicate output header")
		}

		metadata, err := Metadata(value.Header)
		if err != nil {
			return nil, false, nil, err
		}

		d.metadata, d.header = metadata, true

		return nil, false, nil, nil
	case *wirev1.OutputFrame_Chunk:
		if !d.header || uint64(len(value.Chunk)) > math.MaxUint64-d.bytes {
			return nil, false, nil, errors.New("invalid output chunk")
		}

		d.bytes += uint64(len(value.Chunk))

		return value.Chunk, true, nil, nil
	case *wirev1.OutputFrame_Terminal:
		terminal := value.Terminal
		if !d.header || terminal == nil || terminal.GetTransferredBytes() != d.bytes {
			return nil, false, terminal, errors.New("incomplete output transfer")
		}

		d.ended = true
		if terminal.GetFailure() == nil && !terminal.GetCancelled() && !terminal.GetDeadlineExceeded() &&
			d.metadata.LengthKnown && uint64(d.metadata.Length) != d.bytes {
			return nil, false, terminal, fmt.Errorf("output length mismatch: received %d, expected %d", d.bytes, d.metadata.Length)
		}

		return nil, false, terminal, nil
	default:
		return nil, false, nil, errors.New("empty output frame")
	}
}

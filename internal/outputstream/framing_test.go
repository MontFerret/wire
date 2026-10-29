package outputstream

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestChunkBudgetIncludesEachEnvelope(t *testing.T) {
	envelopes := []func(*wirev1.OutputFrame) proto.Message{
		func(f *wirev1.OutputFrame) proto.Message { return &wirev1.ReadExecutionOutputResponse{Frame: f} },
		func(f *wirev1.OutputFrame) proto.Message { return &wirev1.RunCommandResponse{OutputFrame: f} },
		func(f *wirev1.OutputFrame) proto.Message { return &wirev1.WatchDebugResponse{OutputFrame: f} },
	}
	for _, envelope := range envelopes {
		for _, limit := range []int{16, 128, 1024, 4 << 20} {
			size, err := MaxChunk(limit, envelope)
			if err != nil {
				t.Fatal(err)
			}

			payload := bytes.Repeat([]byte{123}, size*3+1)
			var actual []byte

			if err := Chunks(payload, size, func(f *wirev1.OutputFrame) error {
				if proto.Size(envelope(f)) > limit {
					t.Fatal("chunk exceeds configured message size")
				}

				actual = append(actual, f.GetChunk()...)

				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(actual, payload) {
				t.Fatal("chunking changed content")
			}
		}
	}
}

func TestDecoderPresenceAndTerminalAccounting(t *testing.T) {
	for _, present := range []bool{false, true} {
		var decoder Decoder
		if _, _, _, err := decoder.Accept(Header(api.Metadata{LengthKnown: true})); err != nil {
			t.Fatal(err)
		}

		if present {
			_, chunk, _, err := decoder.Accept(&wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Chunk{}})
			if err != nil || !chunk {
				t.Fatalf("empty chunk: %v", err)
			}
		}

		if _, _, _, err := decoder.Accept(End(&wirev1.OutputTerminal{})); err != nil {
			t.Fatal(err)
		}

		if decoder.Present() != present {
			t.Fatal("zero length confused content presence")
		}

		if _, _, _, err := decoder.Accept(End(&wirev1.OutputTerminal{})); err == nil {
			t.Fatal("accepted a second terminal")
		}
	}
}

func TestDecoderRejectsMalformedTransfers(t *testing.T) {
	tests := map[string][]*wirev1.OutputFrame{
		"missing header":        {{Frame: &wirev1.OutputFrame_Chunk{Chunk: []byte("x")}}},
		"negative length":       {Header(api.Metadata{LengthKnown: true, Length: -1})},
		"duplicate header":      {Header(api.Metadata{}), Header(api.Metadata{})},
		"missing bytes":         {Header(api.Metadata{}), End(&wirev1.OutputTerminal{TransferredBytes: 1})},
		"known length mismatch": {Header(api.Metadata{LengthKnown: true, Length: 2}), {Frame: &wirev1.OutputFrame_Chunk{Chunk: []byte("x")}}, End(&wirev1.OutputTerminal{TransferredBytes: 1})},
		"empty frame":           {{}},
	}
	for name, frames := range tests {
		t.Run(name, func(t *testing.T) {
			var decoder Decoder
			var failure error
			for _, frame := range frames {
				_, _, _, failure = decoder.Accept(frame)
				if failure != nil {
					break
				}
			}

			if failure == nil {
				t.Fatal("accepted malformed transfer")
			}
		})
	}
}

func TestDecoderPreservesDescriptorOfPartialContent(t *testing.T) {
	var decoder Decoder
	metadata := api.Metadata{ContentType: "application/json", Length: 100, LengthKnown: true}
	frames := []*wirev1.OutputFrame{Header(metadata), {Frame: &wirev1.OutputFrame_Chunk{Chunk: []byte("[")}}, End(&wirev1.OutputTerminal{TransferredBytes: 1, Failure: &wirev1.Failure{Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE}})}
	for _, frame := range frames {
		if _, _, _, err := decoder.Accept(frame); err != nil {
			t.Fatal(err)
		}
	}

	if decoder.Metadata() != metadata {
		t.Fatal("partial transfer changed the complete-payload descriptor")
	}
}

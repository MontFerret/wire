package grpcserver

import (
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
)

func TestExecutionOutputChunkBudgetConcurrentReaders(t *testing.T) {
	for _, limit := range []int{1, 16, 1024, 4 << 20} {
		service := &ExecutionService{maxOutbound: limit}
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				for range 10 {
					size, err := service.outputChunkSize()

					if limit == 1 {
						if err == nil || size != 0 {
							t.Error("accepted a limit too small for a chunk")
						}

						continue
					}

					if err != nil || size <= 0 || size > outputstream.ChunkSize {
						t.Errorf("invalid budget: size=%d error=%v", size, err)

						return
					}

					message := &wirev1.ReadExecutionOutputResponse{Frame: &wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Chunk{Chunk: make([]byte, size)}}}
					if proto.Size(message) > limit {
						t.Error("cached budget exceeds the message limit")
					}
				}
			})
		}

		workers.Wait()
	}
}

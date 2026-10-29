package grpcserver

import (
	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
)

func output(value *api.Content) *wirev1.Output {
	if value == nil {
		return nil
	}

	return &wirev1.Output{Metadata: outputstream.Descriptor(value.Metadata)}
}

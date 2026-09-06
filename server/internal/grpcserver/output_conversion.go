package grpcserver

import (
	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func output(value *api.Output) *wirev1.Output {
	if value == nil {
		return nil
	}

	return &wirev1.Output{ContentType: value.ContentType, Content: append([]byte(nil), value.Content...)}
}

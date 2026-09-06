package grpcserver

import (
	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func decodeSessionOptions(parameters *wirev1.Parameters, contentType string) ([]api.SessionOption, error) {
	values, err := decodeParameters(parameters)
	if err != nil {
		return nil, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: err.Error()}
	}

	options := []api.SessionOption{api.WithParams(values)}
	if contentType != "" {
		options = append(options, api.WithOutputContentType(contentType))
	}

	return options, nil
}

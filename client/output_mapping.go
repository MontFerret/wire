package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

func convertOutput(value *wirev1.Output) *Output {
	if value == nil {
		return nil
	}

	return &Output{ContentType: value.GetContentType(), Content: append([]byte(nil), value.GetContent()...)}
}

package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

type (
	// DiagnosticSpan is a labeled half-open UTF-8 byte span in source.
	DiagnosticSpan struct {
		Start   uint64
		End     uint64
		Label   string
		Primary bool
	}

	// Diagnostic is a structured Ferret compiler or runtime diagnostic.
	Diagnostic struct {
		Kind           string
		Message        string
		Hint           string
		Note           string
		SourceIdentity string
		Spans          []DiagnosticSpan
	}
)

func convertDiagnostics(values []*wirev1.Diagnostic) []Diagnostic {
	result := make([]Diagnostic, len(values))

	for i, value := range values {
		if value == nil {
			continue
		}

		spans := make([]DiagnosticSpan, len(value.GetSpans()))
		for j, span := range value.GetSpans() {
			if span == nil {
				continue
			}

			spans[j] = DiagnosticSpan{
				Start:   span.GetStartByte(),
				End:     span.GetEndByte(),
				Label:   span.GetLabel(),
				Primary: span.GetPrimary(),
			}
		}

		result[i] = Diagnostic{
			Kind:           value.GetKind(),
			Message:        value.GetMessage(),
			Hint:           value.GetHint(),
			Note:           value.GetNote(),
			SourceIdentity: value.GetSourceIdentity(),
			Spans:          spans,
		}
	}

	return result
}

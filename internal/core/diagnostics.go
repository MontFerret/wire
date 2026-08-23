package core

import (
	"errors"
	"iter"

	ferretdiagnostics "github.com/MontFerret/ferret/v2/pkg/diagnostics"
	"github.com/MontFerret/ferret/v2/pkg/vm"
)

type runtimeErrorSet interface {
	Errors() iter.Seq2[int, *vm.RuntimeError]
}

func diagnosticsFromError(err error, identity string) []Diagnostic {
	values := extractDiagnostics(err)
	result := make([]Diagnostic, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}

		diagnostic := Diagnostic{
			Kind:           value.Kind.String(),
			Message:        value.Message,
			Hint:           value.Hint,
			Note:           value.Note,
			SourceIdentity: identity,
			Spans:          make([]DiagnosticSpan, 0, len(value.Spans)),
		}
		for _, span := range value.Spans {
			if span.Span.Start < 0 || span.Span.End < span.Span.Start {
				continue
			}

			diagnostic.Spans = append(diagnostic.Spans, DiagnosticSpan{
				Start:   uint64(span.Span.Start),
				End:     uint64(span.Span.End),
				Label:   span.Label,
				Primary: span.Main,
			})
		}

		result = append(result, diagnostic)
	}

	return result
}

func extractDiagnostics(err error) []*ferretdiagnostics.Diagnostic {
	var runtimeSet runtimeErrorSet
	if errors.As(err, &runtimeSet) {
		var result []*ferretdiagnostics.Diagnostic
		for _, item := range runtimeSet.Errors() {
			if item != nil && item.Diagnostic != nil {
				result = append(result, item.Diagnostic)
			}
		}

		return result
	}

	var runtimeError *vm.RuntimeError
	if errors.As(err, &runtimeError) && runtimeError != nil && runtimeError.Diagnostic != nil {
		return []*ferretdiagnostics.Diagnostic{runtimeError.Diagnostic}
	}

	var set *ferretdiagnostics.DiagnosticSet
	if errors.As(err, &set) && set != nil {
		result := make([]*ferretdiagnostics.Diagnostic, 0, set.Size())
		for _, item := range set.Errors() {
			result = append(result, item)
		}

		return result
	}

	var single *ferretdiagnostics.Diagnostic
	if errors.As(err, &single) && single != nil {
		return []*ferretdiagnostics.Diagnostic{single}
	}

	return nil
}

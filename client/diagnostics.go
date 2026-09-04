package client

import (
	"fmt"

	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func convertDiagnosticSet(value *wirev1.DiagnosticSet) (diagnostics.Diagnostics, error) {
	if value == nil {
		return nil, nil
	}

	result := make(diagnostics.Diagnostics, len(value.GetDiagnostics()))
	for i, item := range value.GetDiagnostics() {
		if item == nil || item.GetSource() == nil {
			return nil, invalidDiagnosticResponse("diagnostic %d is incomplete", i)
		}

		annotations := make([]diagnostics.Annotation, len(item.GetAnnotations()))
		for j, annotation := range item.GetAnnotations() {
			if annotation == nil {
				return nil, invalidDiagnosticResponse("diagnostic %d annotation %d is missing", i, j)
			}

			convertedRange, err := convertSourceRange(annotation.GetRange())
			if err != nil {
				return nil, invalidDiagnosticResponse("diagnostic %d annotation %d: %v", i, j, err)
			}

			if convertedRange == nil {
				return nil, invalidDiagnosticResponse("diagnostic %d annotation %d range is missing", i, j)
			}

			annotations[j] = diagnostics.Annotation{
				Range:   *convertedRange,
				Message: annotation.GetMessage(),
				Primary: annotation.GetPrimary(),
			}
		}

		result[i] = diagnostics.Diagnostic{
			Kind:        diagnostics.Kind(item.GetKind()),
			Message:     item.GetMessage(),
			Source:      source.Source{Name: item.GetSource().GetName(), Content: item.GetSource().GetContent()},
			Annotations: annotations,
			Hint:        item.GetHint(),
			Note:        item.GetNote(),
		}
	}

	return result, nil
}

func invalidDiagnosticResponse(format string, args ...any) error {
	return fmt.Errorf("Wire server returned invalid diagnostics: %s", fmt.Sprintf(format, args...))
}

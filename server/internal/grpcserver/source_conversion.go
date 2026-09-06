package grpcserver

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func diagnosticsToProto(values diagnostics.Diagnostics) (*wirev1.DiagnosticSet, error) {
	if values == nil {
		return nil, nil
	}

	result := &wirev1.DiagnosticSet{Diagnostics: make([]*wirev1.Diagnostic, len(values))}
	for i, value := range values {
		annotations := make([]*wirev1.DiagnosticAnnotation, len(value.Annotations))
		for j, annotation := range value.Annotations {
			convertedRange, err := sourceRange(annotation.Range)
			if err != nil {
				return nil, err
			}

			if convertedRange == nil {
				return nil, runtimeConversionError("runtime returned a diagnostic annotation with no range")
			}

			annotations[j] = &wirev1.DiagnosticAnnotation{
				Range:   convertedRange,
				Message: annotation.Message,
				Primary: annotation.Primary,
			}
		}

		result.Diagnostics[i] = &wirev1.Diagnostic{
			Kind:        value.Kind.String(),
			Message:     value.Message,
			Hint:        value.Hint,
			Note:        value.Note,
			Source:      &wirev1.Source{Name: value.Source.Name, Content: value.Source.Content},
			Annotations: annotations,
		}
	}

	return result, nil
}

func sourceLocation(value source.Location) (*wirev1.Location, error) {
	if value == (source.Location{}) {
		return nil, nil
	}

	if value.SourceName == "" {
		return nil, runtimeConversionError("runtime returned a source location with no source name")
	}

	if value.Line <= 0 || value.Column < 0 {
		return nil, runtimeConversionError("runtime returned an invalid source location")
	}

	return &wirev1.Location{
		SourceName: value.SourceName,
		Position: &wirev1.Position{
			Line:   int64(value.Line),
			Column: int64(value.Column),
		},
	}, nil
}

func sourceRange(value source.Range) (*wirev1.Range, error) {
	if value == (source.Range{}) {
		return nil, nil
	}

	location, err := sourceLocation(value.Location)
	if err != nil {
		return nil, err
	}

	if location == nil {
		return nil, runtimeConversionError("runtime returned a source range with no location")
	}

	if value.Span.Start < 0 || value.Span.End < value.Span.Start {
		return nil, runtimeConversionError("runtime returned an invalid source span")
	}

	return &wirev1.Range{
		Location: location,
		Span: &wirev1.Span{
			Start: int64(value.Span.Start),
			End:   int64(value.Span.End),
		},
	}, nil
}

func sourceLocationFromProto(value *wirev1.Location, name string) (source.Location, error) {
	if value == nil {
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " is required"}
	}

	if value.GetSourceName() == "" {
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " source name is required"}
	}

	position := value.GetPosition()
	if position == nil || position.GetLine() <= 0 || position.GetColumn() < 0 {
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " position is invalid"}
	}

	line, err := intFromProto(position.GetLine(), name+" line")
	if err != nil {
		return source.Location{}, err
	}

	column, err := intFromProto(position.GetColumn(), name+" column")
	if err != nil {
		return source.Location{}, err
	}

	return source.Location{
		SourceName: value.GetSourceName(),
		Position: source.Position{
			Line:   line,
			Column: column,
		},
	}, nil
}

func intFromProto(value int64, name string) (int, error) {
	if value < 0 || uint64(value) > uint64(^uint(0)>>1) {
		return 0, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " is out of range"}
	}

	return int(value), nil
}

func decodeSource(value *wirev1.Source) api.Source {
	return api.Source{Name: value.GetName(), Content: value.GetContent()}
}

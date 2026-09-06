package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// compileConfigured shares allocation transport without reapplying option callbacks.
func (c *connectionHandle) compileConfigured(ctx context.Context, src api.Source, debuggable bool, configured runtimePlanOptions) (*planHandle, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	convertedOptions, err := encodeCompileOptions(configured.optimizationLevel, configured.hasOptimizationLevel)
	if err != nil {
		return nil, err
	}

	var value *wirev1.Plan
	if debuggable {
		response, err := c.planClient.CompileDebug(ctx, &wirev1.CompileDebugRequest{
			ConnectionId: c.connectionProto(),
			Source:       &wirev1.Source{Content: src.Content, Name: src.Name},
			Options:      convertedOptions,
		})
		if err != nil {
			return nil, allocationRPCError(err)
		}

		value = response.GetPlan()
	} else {
		response, err := c.planClient.Compile(ctx, &wirev1.CompileRequest{
			ConnectionId: c.connectionProto(),
			Source:       &wirev1.Source{Content: src.Content, Name: src.Name},
			Options:      convertedOptions,
		})
		if err != nil {
			return nil, allocationRPCError(err)
		}

		value = response.GetPlan()
	}

	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid compiled plan")}
	}

	return &planHandle{
		client:     c,
		id:         value.GetId().GetValue(),
		parameters: append([]string(nil), value.GetParameters()...),
		close:      &closeState{},
	}, nil
}

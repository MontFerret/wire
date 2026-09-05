package client

import (
	"context"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func (c *Client) run(
	ctx context.Context,
	src api.Source,
	parameters Parameters,
	options ExecuteOptions,
) (*Execution, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := c.runtimeClient.Run(ctx, &wirev1.RunRequest{
		ConnectionId:      c.connectionProto(),
		Source:            &wirev1.Source{Name: src.Name, Content: src.Content},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, allocationRPCError(err)
	}

	return newExecutionHandle(c, nil, nil, response.GetExecution())
}

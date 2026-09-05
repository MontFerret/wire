package grpcserver

func subscriptionError(errors <-chan error) error {
	select {
	case err, ok := <-errors:
		if ok && err != nil {
			return rpcError(err)
		}
	default:
	}

	return nil
}

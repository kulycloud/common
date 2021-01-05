package http

func receive(stream grpcStream, object chunkable) (error, errorChannel) {
	chunk, err := stream.Recv()
	if err != nil {
		logger.Errorw("could not receive first chunk", "error", err)
		return ErrStreamError, nil
	}
	err = object.fromChunk(chunk)
	if err != nil {
		return err, nil
	}
	done := object.getBody().connectStream(stream)
	return nil, done
}

func send(stream grpcStream, object chunkable) (error, errorChannel) {
	err := stream.Send(object.toChunk())
	if err != nil {
		logger.Errorw("could not send first chunk", "error", err)
		return ErrStreamError, nil
	}
	done := object.getBody().toStream(stream)
	return nil, done
}

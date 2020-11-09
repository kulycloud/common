package http

import (
	protoHttp "github.com/kulycloud/protocol/http"
)

func receive(stream grpcStream, object chunkable) error {
	chunk, err := stream.Recv()
	if err != nil {
		return err
	}
	err = object.fromChunk(chunk)
	if err != nil {
		return err
	}
	object.getBody().setStream(stream)
	return nil
}

func send(stream grpcStream, object chunkable) error {
	err := stream.Send(object.toChunk())
	if err != nil {
		return err
	}
	object.getBody().toStream()
	for {
		chunk, ok := <-object.getBody().sendChannel
		if !ok {
			break
		}
		err = stream.Send(chunk)
		if err != nil {
			return err
		}
	}
	// properly close the send stream if performed by a client
	clientStream, isClientStream := stream.(protoHttp.Http_ProcessRequestClient)
	if isClientStream {
		err = clientStream.CloseSend()
	}
	return err
}

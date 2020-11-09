package http

import (
	"errors"
	protoHttp "github.com/kulycloud/protocol/http"
)

var ErrConversionError = errors.New("error during conversion")

// request

func (request *HttpRequest) fromChunk(chunk *protoHttp.Chunk) error {
	request.requestHeader = chunk.GetHeader().GetRequestHeader()
	if request.requestHeader == nil {
		return ErrConversionError
	}
	return nil
}

func (request *HttpRequest) toChunk() *protoHttp.Chunk {
	return &protoHttp.Chunk{
		Content: &protoHttp.Chunk_Header{
			Header: &protoHttp.Header{
				Content: &protoHttp.Header_RequestHeader{
					RequestHeader: request.requestHeader,
				},
			},
		},
	}
}

// response

func (response *HttpResponse) fromChunk(chunk *protoHttp.Chunk) error {
	response.responseHeader = chunk.GetHeader().GetResponseHeader()
	if response.responseHeader == nil {
		return ErrConversionError
	}
	return nil
}

func (response *HttpResponse) toChunk() *protoHttp.Chunk {
	return &protoHttp.Chunk{
		Content: &protoHttp.Chunk_Header{
			Header: &protoHttp.Header{
				Content: &protoHttp.Header_ResponseHeader{
					ResponseHeader: response.responseHeader,
				},
			},
		},
	}
}

// body

func (bs ByteSlice) toChunk() *protoHttp.Chunk {
	return &protoHttp.Chunk{
		Content: &protoHttp.Chunk_BodyChunk{
			BodyChunk: bs,
		},
	}
}

func (bw *bodyWorker) setStream(stream grpcStream) {
	bw.connectedToStream = true
	go func() {
		for {
			chunk, err := stream.Recv()
			if err != nil {
				close(bw.recvChannel)
				break
			}
			bw.recvChannel <- chunk
		}
	}()
}

func (bw *bodyWorker) toStream() {
	go func() {
		// parse buffer
		i := 0
		length := len(bw.buffer)
		for {
			j := i + MAX_CHUNK_SIZE
			if j >= length {
				break
			}
			bw.sendChannel <- ByteSlice(bw.buffer[i:j]).toChunk()
			i = j
		}
		bw.sendChannel <- ByteSlice(bw.buffer[i:]).toChunk()

		// propagate remaining packages
		if bw.connectedToStream {
			for {
				chunk, ok := <-bw.recvChannel
				if !ok {
					break
				}
				bw.sendChannel <- chunk
			}
		}
		close(bw.sendChannel)
	}()
}

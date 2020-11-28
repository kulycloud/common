package http

import (
	"errors"
	protoHttp "github.com/kulycloud/protocol/http"
)

var ErrConversionError = errors.New("error during conversion")

// request

func (request *Request) fromChunk(chunk *protoHttp.Chunk) error {
	request.requestHeader = chunk.GetHeader().GetRequestHeader()
	if request.requestHeader == nil {
		return ErrConversionError
	}
	return nil
}

func (request *Request) toChunk() *protoHttp.Chunk {
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

func (response *Response) fromChunk(chunk *protoHttp.Chunk) error {
	response.responseHeader = chunk.GetHeader().GetResponseHeader()
	if response.responseHeader == nil {
		return ErrConversionError
	}
	return nil
}

func (response *Response) toChunk() *protoHttp.Chunk {
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

func (bw *bodyWorker) connectStream(stream grpcStream) {
	bw.connectedToStream = true
	go func() {
		for {
			chunk, err := stream.Recv()
			if err != nil {
				close(bw.backlog)
				break
			}
			bw.backlog <- chunk
		}
	}()
}

func (bw *bodyWorker) toStream() <-chan *protoHttp.Chunk {
	sendChannel := make(chan *protoHttp.Chunk, 1)
	go func() {
		// parse buffer
		i := 0
		length := len(bw.buffer)
		for {
			j := i + MaxChunkSize
			if j >= length {
				break
			}
			sendChannel <- ByteSlice(bw.buffer[i:j]).toChunk()
			i = j
		}
		sendChannel <- ByteSlice(bw.buffer[i:]).toChunk()

		// propagate remaining packages
		if bw.connectedToStream {
			for {
				chunk, ok := <-bw.backlog
				if !ok {
					break
				}
				sendChannel <- chunk
			}
		}
		close(sendChannel)
	}()
	return sendChannel
}

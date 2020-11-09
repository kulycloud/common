package http

import (
	"errors"

	protoHttp "github.com/kulycloud/protocol/http"
)

const MAX_CHUNK_SIZE = 4096

var ErrBodyTooSmall = errors.New("body does not contain number of bytes requested")

// handler interface
type HttpHandler interface {
	HandleRequest(*HttpRequest) *HttpResponse
}

// type aliasing to hide in structs
type requestHeader = protoHttp.RequestHeader
type responseHeader = protoHttp.ResponseHeader

// request/response
type HttpRequest struct {
	*requestHeader
	Body *bodyWorker
}

type HttpResponse struct {
	*responseHeader
	Body *bodyWorker
}

func newEmptyHttpRequest() *HttpRequest {
	return &HttpRequest{
		Body: newBodyWorker(),
	}
}

func newEmptyHttpResponse() *HttpResponse {
	return &HttpResponse{
		Body: newBodyWorker(),
	}

}

func NewHttpResponse() *HttpResponse {
	response := newEmptyHttpResponse()
	response.responseHeader = &protoHttp.ResponseHeader{
		Status:     200,
		Headers:    make(map[string]string),
		RequestUid: "not set",
	}
	return response
}

func (response *HttpResponse) WithStatus(status int32) *HttpResponse {
	response.Status = status
	return response
}

func (response *HttpResponse) WithHeader(key string, value string) *HttpResponse {
	response.Headers[key] = value
	return response
}

func (response *HttpResponse) withRequestUid(requestUid string) *HttpResponse {
	response.RequestUid = requestUid
	return response
}

// grpc

type grpcStream interface {
	Send(*protoHttp.Chunk) error
	Recv() (*protoHttp.Chunk, error)
}

type chunkable interface {
	fromChunk(*protoHttp.Chunk) error
	toChunk() *protoHttp.Chunk
	// this is not pleasent but simplifies parsing
	getBody() *bodyWorker
}

func (request *HttpRequest) getBody() *bodyWorker {
	return request.Body
}

func (response *HttpResponse) getBody() *bodyWorker {
	return response.Body
}

// body
type ByteSlice []byte

/*
The concept of the bodyWorker is to lazy load the
body contents from a stream.
If the body is streamed only the loaded parts of
the body have to be converted back into chunks.
The remaining packets in the receiving stream
are forewarded to the send stream.

- connectedToStream is a necessary flag because
  a body can exist without a receiving stream
  i.e. a newly created response
- recvChannel is filled with packets from the
  connected grpc stream after the
  setStream method is called
- sendStream is filled with the buffered body
  and the remaining packets in the recvChannel
  after the toStream method is called
- buffer contains the part of the body that has
  been loaded
*/
type bodyWorker struct {
	connectedToStream bool
	recvChannel       chan *protoHttp.Chunk
	sendChannel       chan *protoHttp.Chunk
	buffer            ByteSlice
}

func newBodyWorker() *bodyWorker {
	return &bodyWorker{
		connectedToStream: false,
		recvChannel:       make(chan *protoHttp.Chunk, 1),
		sendChannel:       make(chan *protoHttp.Chunk, 1),
		buffer:            make(ByteSlice, 0, MAX_CHUNK_SIZE),
	}
}

// lazy load from recvChannel until buffer contains numberOfBytes
func (bw *bodyWorker) Read(numberOfBytes int) (ByteSlice, error) {
	for len(bw.buffer) < numberOfBytes || numberOfBytes == -1 {
		if bw.connectedToStream {
			chunk, ok := <-bw.recvChannel
			if !ok {
				return bw.buffer, ErrBodyTooSmall
			}
			bw.buffer = append(bw.buffer, chunk.GetBodyChunk()...)
		} else {
			return bw.buffer, ErrBodyTooSmall
		}
	}
	return bw.buffer[:numberOfBytes], nil
}

// lazy load offset+numberOfBytes bytes from stream and return from offset
func (bw *bodyWorker) ReadAtOffset(numberOfBytes int, offset int) (ByteSlice, error) {
	bs, err := bw.Read(offset + numberOfBytes)
	if err != nil {
		if len(bs) > offset {
			return bs[offset:], err
		}
		return nil, err
	}
	return bs[offset:], err
}

func (bw *bodyWorker) ReadAll() ByteSlice {
	bs, _ := bw.Read(-1)
	return bs
}

func (bw *bodyWorker) ReadAllAtOffset(offset int) (ByteSlice, error) {
	bs := bw.ReadAll()
	if len(bs) > offset {
		return bs[offset:], nil
	}
	return nil, ErrBodyTooSmall
}

// clear out receiving stream
func (bw *bodyWorker) clearBacklog() {
	if bw.connectedToStream {
		for {
			_, ok := <-bw.recvChannel
			if !ok {
				break
			}
		}
	}
}

func (bw *bodyWorker) Write(content ByteSlice) {
	bw.clearBacklog()
	bw.buffer = content
}

func (bw *bodyWorker) Clear() {
	bw.Write(ByteSlice{})
}

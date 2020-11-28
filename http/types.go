package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strings"

	protoHttp "github.com/kulycloud/protocol/http"
)

const MaxChunkSize = 4 << 10

var ErrBodyTooSmall = errors.New("body does not contain number of bytes requested")

// handler function
type HandlerFunc func(*Request) *Response

// type aliasing to hide in structs
type requestHeader = protoHttp.RequestHeader
type responseHeader = protoHttp.ResponseHeader

// Request
type Request struct {
	*requestHeader
	Body *bodyWorker
}

func NewRequest() *Request {
	return &Request{
		requestHeader: &requestHeader{
			HttpData: &protoHttp.RequestHeader_HttpData{
				Headers: make(Headers),
			},
			KulyData:    &protoHttp.RequestHeader_KulyData{},
			ServiceData: make(ServiceData),
		},
		Body: NewBody(),
	}
}

// Response
type Response struct {
	*responseHeader
	Body *bodyWorker
}

func NewResponse() *Response {
	return &Response{
		responseHeader: &responseHeader{
			Status:     200,
			Headers:    make(Headers),
			RequestUid: "not set",
		},
		Body: NewBody(),
	}
}

func (response *Response) SetStatus(status int) {
	response.Status = int32(status)
}

func (response *Response) setRequestUid(requestUid string) {
	response.RequestUid = requestUid
}

// grpc

type grpcStream interface {
	Send(*protoHttp.Chunk) error
	Recv() (*protoHttp.Chunk, error)
}

type chunkable interface {
	fromChunk(*protoHttp.Chunk) error
	toChunk() *protoHttp.Chunk
	// this is not pleasant but simplifies parsing
	getBody() *bodyWorker
}

func (request *Request) getBody() *bodyWorker {
	return request.Body
}

func (response *Response) getBody() *bodyWorker {
	return response.Body
}

// Headers
// headers are stored in a map[string]string
// in contrast to the way the net/http package stores them (map[string][]string)
// the values of the header are stored in a string separated by semicolons
type Headers map[string]string

func (h Headers) Set(key string, value string) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	h[key] = value
}

func (h Headers) Add(key string, value string) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	if len(h[key]) > 0 {
		h[key] = value
	} else {
		h[key] = fmt.Sprintf("%s;%s", h[key], value)
	}
}

// 	get returns the whole value of the header, not just the first part
// 	if you only want the first value, use GetValues()[0]
func (h Headers) Get(key string) string {
	key = textproto.CanonicalMIMEHeaderKey(key)
	return h[key]
}

func (h Headers) GetValues(key string) []string {
	key = textproto.CanonicalMIMEHeaderKey(key)
	return strings.Split(h[key], ";")
}

func (h Headers) SetValues(key string, values []string) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	h[key] = strings.Join(values, ";")
}

// Service Data

type ServiceData map[string]string

// ByteSlice
type ByteSlice []byte

func (bs ByteSlice) String() string {
	return string(bs)
}

func (bs ByteSlice) Unmarshal(result interface{}) error {
	return json.Unmarshal(bs, result)
}

/* bodyWorker
The concept of the bodyWorker is to lazy load the
body contents from a stream.
If the body is streamed only the loaded parts of
the body have to be converted back into chunks.
The remaining packets in the receiving stream
are forwarded to the send stream.

- connectedToStream is a necessary flag because
  a body can exist without a receiving stream
  i.e. a newly created response
- backlog is filled with packets from the
  connected grpc stream after the
  connectStream method is called
- sendStream is filled with the buffered body
  and the remaining packets in the backlog
  after the toStream method is called
- buffer contains the part of the body that has
  been loaded
*/
type bodyWorker struct {
	connectedToStream bool
	backlog           chan *protoHttp.Chunk
	buffer            ByteSlice
}

func NewBody() *bodyWorker {
	return &bodyWorker{
		connectedToStream: false,
		backlog:           make(chan *protoHttp.Chunk, 1),
		buffer:            make(ByteSlice, 0, MaxChunkSize),
	}
}

// lazy load from backlog until buffer contains numberOfBytes
func (bw *bodyWorker) Read(numberOfBytes int) (ByteSlice, error) {
	for len(bw.buffer) < numberOfBytes || numberOfBytes == -1 {
		if bw.connectedToStream {
			chunk, ok := <-bw.backlog
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
			_, ok := <-bw.backlog
			if !ok {
				break
			}
		}
	}
}

func (bw *bodyWorker) Write(content []byte) {
	bw.clearBacklog()
	bw.buffer = content
}

func (bw *bodyWorker) Append(content []byte) {
	_ = bw.ReadAll()
	bw.buffer = append(bw.buffer, content...)
}

func (bw *bodyWorker) Clear() {
	bw.Write(ByteSlice{})
}

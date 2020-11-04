package http

import (
	protoHttp "github.com/kulycloud/protocol/http"
)

type HttpHandler interface {
	HandleRequest(*HttpRequest) *HttpResponse
}

type HttpRequest struct {
	*protoHttp.RequestHeader
	*bodyWrapper
}

type HttpResponse struct {
	*protoHttp.ResponseHeader
	*bodyWrapper
}

const MAX_CHUNK_SIZE = 4096
const MAX_CHUNK_COUNT = 4096

type bodyWrapper struct {
	body chan []byte
}

func (bw *bodyWrapper) NewBody() {
	bw.body = make(chan []byte, MAX_CHUNK_COUNT)
}

func (bw *bodyWrapper) GetBody() chan []byte {
	return bw.body
}

/*
Parses content into packets of size MAX_CHUNK_SIZE
If len(content) % MAX_CHUNK_SIZE != 0 the remaining
part is parsed in its own packet.

Example with MAX_CHUNK_SIZE = 4:
Length:  7
[0 1 2 3]
[4 5 6]
Length:  8
[0 1 2 3]
[4 5 6 7]
Length:  9
[0 1 2 3]
[4 5 6 7]
[8]
*/
func (bw *bodyWrapper) Append(content []byte) {
	i := 0
	length := len(content)
	for {
		j := i + MAX_CHUNK_SIZE
		if j >= length {
			break
		}
		bw.body <- content[i:j]
		i = j
	}
	bw.body <- content[i:]
}

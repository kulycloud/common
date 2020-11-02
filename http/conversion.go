package http

import (
	protoHttp "github.com/kulycloud/protocol/http"
)

// http-intermediate <-> stream

func newRequestFromStream(reqStream <-chan *protoHttp.RequestChunk) *HttpRequest {
	request := newRequestFromChunk(<-reqStream)
	for {
		bodyChunk, ok := <-reqStream
		if ok {
			request.Append(bodyChunk.GetBodyChunk())
		} else {
			break
		}
	}
	return request
}

func newStreamFromRequest(request *HttpRequest) <-chan *protoHttp.RequestChunk {
	var reqStream = make(chan *protoHttp.RequestChunk)
	go func() {
		reqStream <- newGrpcRequestHeaderChunk(request)
		for {
			bodyChunk, ok := <-request.GetBody()
			if ok {
				reqStream <- newGrpcRequestBodyChunk(bodyChunk)
			} else {
				break
			}
		}
		close(reqStream)
	}()
	return reqStream
}

func newResponseFromStream(resStream <-chan *protoHttp.ResponseChunk) *HttpResponse {
	response := newResponseFromChunk(<-resStream)
	for {
		bodyChunk, ok := <-resStream
		if ok {
			response.Append(bodyChunk.GetBodyChunk())
		} else {
			break
		}
	}
	return response
}

func newStreamFromResponse(response *HttpResponse) <-chan *protoHttp.ResponseChunk {
	var resStream = make(chan *protoHttp.ResponseChunk)
	go func() {
		resStream <- newGrpcResponseHeaderChunk(response)
		for {
			bodyChunk, ok := <-response.GetBody()
			if ok {
				resStream <- newGrpcResponseBodyChunk(bodyChunk)
			} else {
				break
			}
		}
		close(resStream)
	}()
	return resStream
}

// http-intermediate -> grpc chunks

func newGrpcRequestHeaderChunk(request *HttpRequest) *protoHttp.RequestChunk {
	return &protoHttp.RequestChunk{
		Content: &protoHttp.RequestChunk_Header{
			Header: request.RequestHeader,
		},
	}
}

func newGrpcRequestBodyChunk(bodyChunk []byte) *protoHttp.RequestChunk {
	return &protoHttp.RequestChunk{
		Content: &protoHttp.RequestChunk_BodyChunk{
			BodyChunk: bodyChunk,
		},
	}
}

func newGrpcResponseHeaderChunk(response *HttpResponse) *protoHttp.ResponseChunk {
	return &protoHttp.ResponseChunk{
		Content: &protoHttp.ResponseChunk_Header{
			Header: response.ResponseHeader,
		},
	}
}

func newGrpcResponseBodyChunk(bodyChunk []byte) *protoHttp.ResponseChunk {
	return &protoHttp.ResponseChunk{
		Content: &protoHttp.ResponseChunk_BodyChunk{
			BodyChunk: bodyChunk,
		},
	}
}

// grpc chunk -> http-intermediate

func newRequestFromChunk(requestChunk *protoHttp.RequestChunk) *HttpRequest {
	request := &HttpRequest{}
	request.NewBody()
	if requestChunk != nil {
		request.RequestHeader = requestChunk.GetHeader()
	}
	return request
}

func newRequestBodyChunk(requestChunk *protoHttp.RequestChunk) []byte {
	return requestChunk.GetBodyChunk()
}

func newResponseFromChunk(responseChunk *protoHttp.ResponseChunk) *HttpResponse {
	response := &HttpResponse{}
	response.NewBody()
	if responseChunk != nil {
		response.ResponseHeader = responseChunk.GetHeader()
	}
	return response
}

func newResponseBodyChunk(responseChunk *protoHttp.ResponseChunk) []byte {
	return responseChunk.GetBodyChunk()
}

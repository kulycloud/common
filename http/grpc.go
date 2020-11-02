package http

import (
	"errors"
	protoHttp "github.com/kulycloud/protocol/http"
	"io"
)

// client

func sendRequest(grpcStream protoHttp.Http_ProcessRequestClient, request *HttpRequest) error {
	var err error
	reqStream := newStreamFromRequest(request)
	for {
		reqChunk, ok := <-reqStream
		if ok {
			err = grpcStream.Send(reqChunk)
			if err != nil {
				break
			}
		} else {
			err = grpcStream.CloseSend()
			break
		}
	}
	return err
}

func receiveResponse(grpcStream protoHttp.Http_ProcessRequestClient) (*HttpResponse, error) {
	var err error
	var resStream = make(chan *protoHttp.ResponseChunk)
	go func() {
		var resChunk *protoHttp.ResponseChunk
		for {
			resChunk, err = grpcStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
					break
				}
				break
			}
			resStream <- resChunk
		}
		close(resStream)
	}()
	if err != nil {
		return nil, err
	}
	response := newResponseFromStream(resStream)
	return response, err
}

// server

func sendResponse(grpcStream protoHttp.Http_ProcessRequestServer, response *HttpResponse) error {
	var err error
	resStream := newStreamFromResponse(response)
	for {
		resChunk, ok := <-resStream
		if ok {
			err = grpcStream.Send(resChunk)
			if err != nil {
				break
			}
		} else {
			break
		}
	}
	return err
}

func receiveRequest(grpcStream protoHttp.Http_ProcessRequestServer) (*HttpRequest, error) {
	var err error
	var reqStream = make(chan *protoHttp.RequestChunk)
	go func() {
		var reqChunk *protoHttp.RequestChunk
		for {
			reqChunk, err = grpcStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
					break
				}
				break
			}
			reqStream <- reqChunk
		}
		close(reqStream)
	}()
	if err != nil {
		return nil, err
	}
	request := newRequestFromStream(reqStream)
	return request, err
}

package http

import (
	"errors"
	protoHttp "github.com/kulycloud/protocol/http"
)

var ErrConversionError = errors.New("error during conversion")

// request

func (request *Request) fromChunk(chunk *protoHttp.Chunk) error {
	// check if all necessary parts are set
	header := chunk.GetHeader().GetRequestHeader()
	if header == nil {
		logger.Warn("request header in chunk is nil")
		return ErrConversionError
	}
	httpData := header.HttpData
	kulyData := header.KulyData

	if httpData == nil || kulyData == nil {
		logger.Warnw("at least one data object is nil", "httpData", httpData, "kulyData", kulyData)
		return ErrConversionError
	}

	serviceData := header.ServiceData
	if serviceData == nil {
		serviceData = make(ServiceData)
	}

	request.Method = httpData.Method
	request.Host = httpData.Host
	request.Path = httpData.Path
	request.Headers = httpData.Headers
	request.Source = httpData.Source

	request.KulyData = kulyData

	request.ServiceData = serviceData

	return nil
}

func (request *Request) toChunk() *protoHttp.Chunk {
	return &protoHttp.Chunk{
		Content: &protoHttp.Chunk_Header{
			Header: &protoHttp.Header{
				Content: &protoHttp.Header_RequestHeader{
					RequestHeader: &protoHttp.RequestHeader{
						HttpData: &protoHttp.RequestHeader_HttpData{
							Method:  request.Method,
							Host:    request.Host,
							Path:    request.Path,
							Headers: request.Headers,
							Source:  request.Source,
						},
						KulyData:    request.KulyData,
						ServiceData: request.ServiceData,
					},
				},
			},
		},
	}
}

// response

func (response *Response) fromChunk(chunk *protoHttp.Chunk) error {
	header := chunk.GetHeader().GetResponseHeader()
	if header == nil {
		logger.Warn("response header in chunk is nil")
		return ErrConversionError
	}

	response.Status = (int)(header.Status)
	response.Headers = header.Headers
	response.RequestUid = header.RequestUid

	return nil
}

func (response *Response) toChunk() *protoHttp.Chunk {
	return &protoHttp.Chunk{
		Content: &protoHttp.Chunk_Header{
			Header: &protoHttp.Header{
				Content: &protoHttp.Header_ResponseHeader{
					ResponseHeader: &protoHttp.ResponseHeader{
						Status:     (int32)(response.Status),
						Headers:    response.Headers,
						RequestUid: response.RequestUid,
					},
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

func (bw *body) connectStream(stream grpcStream) {
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

func (bw *body) toStream() <-chan *protoHttp.Chunk {
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

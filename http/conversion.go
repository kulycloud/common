package http

import (
	"errors"
	protoHttp "github.com/kulycloud/protocol/http"
	"io"
)

var ErrConversionError = errors.New("error during conversion")
var ErrRequestClosed = errors.New("request context was closed")
var ErrStreamError = errors.New("error during http stream")

type errorChannel chan error

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

func (bw *body) connectStream(stream grpcStream) errorChannel {
	bw.connectedToStream = true
	errCh := make(errorChannel, 1)
	go func() {
		defer func() {
			close(bw.backlog)
			close(errCh)
		}()
		for {
			chunk, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					logger.Warnw("error receiving chunk", "error", err)
					errCh <- err
				}
				break
			}
			select {
			case bw.backlog <- chunk:
			case <-stream.Context().Done():
				logger.Warnw("stream context closed", "error", stream.Context().Err())
				break
			}
		}
	}()
	return errCh
}

func (bw *body) toStream(stream grpcStream) errorChannel {
	errCh := make(errorChannel)
	var err error
	go func() {
		defer func() {
			close(errCh)
		}()
		// parse buffer
		i := 0
		length := len(bw.buffer)
		for {
			j := i + MaxChunkSize
			if j >= length {
				if length > 0 {
					err = sendChunk(stream, bw.buffer[i:].toChunk())
					if err != nil {
						errCh <- err
						return
					}
				}
				break
			}
			err = sendChunk(stream, bw.buffer[i:j].toChunk())
			if err != nil {
				errCh <- err
				return
			}
			i = j
		}

		// propagate remaining packages
		if bw.connectedToStream {
			for {
				chunk, ok := <-bw.backlog
				if !ok {
					break
				}
				err = sendChunk(stream, chunk)
				if err != nil {
					errCh <- err
					return
				}
			}
		}
		// properly close the send stream if performed by a client
		clientStream, isClientStream := stream.(protoHttp.Http_ProcessRequestClient)
		if isClientStream {
			err = clientStream.CloseSend()
			if err != nil && !errors.Is(err, io.EOF) {
				logger.Warnw("could not close stream", "error", err)
			}
		}
	}()
	return errCh
}

func sendChunk(stream grpcStream, chunk *protoHttp.Chunk) error {
	select {
	case <-stream.Context().Done():
		logger.Warnw("stream context closed", "error", stream.Context().Err())
		return ErrRequestClosed
	default:
		err := stream.Send(chunk)
		if err != nil && !errors.Is(err, io.EOF) {
			logger.Warnw("could not send chunk", "error", err)
			return ErrStreamError
		}
		return nil
	}
}

func logErrors(ch errorChannel) {
	for {
		err, ok := <-ch
		if !ok {
			break
		}
		logger.Warnw(ErrStreamError.Error(), "error", err)
	}
}

func waitUntilDone(ch errorChannel) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		logErrors(ch)
	}()
	<-done
}

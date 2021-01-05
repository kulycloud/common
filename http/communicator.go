package http

import (
	"context"
	"errors"
	"github.com/kulycloud/common/communication"
	protoCommon "github.com/kulycloud/protocol/common"
	protoHttp "github.com/kulycloud/protocol/http"
)

var _ communication.RemoteComponent = &Communicator{}

type Communicator struct {
	communication.ComponentCommunicator
	httpClient protoHttp.HttpClient
}

var ErrNoSuitableEndpoint = errors.New("no suitable endpoint found")

func NewCommunicatorFromEndpoint(ctx context.Context, endpoint *protoCommon.Endpoint) (*Communicator, error) {
	componentCommunicator, err := communication.NewComponentCommunicatorFromEndpoint(endpoint)
	if err == nil {
		communicator := &Communicator{
			ComponentCommunicator: *componentCommunicator,
		}
		if err = communicator.Ping(ctx); err == nil {
			communicator.httpClient = protoHttp.NewHttpClient(componentCommunicator.GrpcClient)
			return communicator, nil
		}
	}
	return nil, err
}

func NewCommunicator(ctx context.Context, endpoints []*protoCommon.Endpoint) (*Communicator, error) {
	// choose the first reachable endpoint from the list
	for _, endpoint := range endpoints {
		communicator, err := NewCommunicatorFromEndpoint(ctx, endpoint)
		if err == nil {
			return communicator, nil
		}
		logger.Warnw("could not create communicator", "endpoint", endpoint, "error", err)
	}
	return nil, ErrNoSuitableEndpoint
}

func (communicator *Communicator) ProcessRequest(ctx context.Context, request *Request) (*Response, error) {
	grpcStream, err := communicator.Stream(ctx)
	if err != nil {
		return nil, err
	}
	err, sendErrs := send(grpcStream, request)
	if err != nil {
		return nil, err
	}
	go logErrors(sendErrs)
	response := NewResponse()
	err, recvErrs := receive(grpcStream, response)
	if err != nil {
		return nil, err
	}
	go logErrors(recvErrs)
	return response, nil
}

func (communicator *Communicator) Stream(ctx context.Context) (protoHttp.Http_ProcessRequestClient, error) {
	return communicator.httpClient.ProcessRequest(ctx)
}

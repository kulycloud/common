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

func NewCommunicatorFromEndpoint(endpoint *protoCommon.Endpoint) (*Communicator, error) {
	componentCommunicator, err := communication.NewComponentCommunicatorFromEndpoint(endpoint)
	if err == nil {
		communicator := &Communicator{
			ComponentCommunicator: *componentCommunicator,
		}
		if err = communicator.Ping(context.Background()); err == nil {
			communicator.httpClient = protoHttp.NewHttpClient(componentCommunicator.GrpcClient)
			return communicator, nil
		}
	}
	return nil, err
}

func NewCommunicator(endpoints []*protoCommon.Endpoint) (*Communicator, error) {
	// choose the first reachable endpoint from the list
	for _, endpoint := range endpoints {
		communicator, err := NewCommunicatorFromEndpoint(endpoint)
		if err == nil {
			return communicator, nil
		}
	}
	return nil, ErrNoSuitableEndpoint
}

func (communicator *Communicator) ProcessRequest(ctx context.Context, request *Request) (*Response, error) {
	grpcStream, err := communicator.Stream(ctx)
	if err != nil {
		return nil, err
	}
	err = send(grpcStream, request)
	if err != nil {
		return nil, err
	}
	response := NewResponse()
	err = receive(grpcStream, response)
	return response, err
}

func (communicator *Communicator) Stream(ctx context.Context) (protoHttp.Http_ProcessRequestClient, error) {
	return communicator.httpClient.ProcessRequest(ctx)
}

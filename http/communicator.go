package http

import (
	"context"
	"github.com/kulycloud/common/communication"
	protoCommon "github.com/kulycloud/protocol/common"
	protoHttp "github.com/kulycloud/protocol/http"
)

var _ communication.RemoteComponent = &HttpCommunicator{}

type HttpCommunicator struct {
	communication.ComponentCommunicator
	httpClient protoHttp.HttpClient
}

func NewHttpCommunicator(endpoints []*protoCommon.Endpoint) *HttpCommunicator {
	communicator := &HttpCommunicator{}
	// choose the first reachable endpoint from the list
	for _, endpoint := range endpoints {
		componentCommunicator, err := communication.NewComponentCommunicatorFromEndpoint(endpoint)
		if err == nil {
			if err = componentCommunicator.Ping(context.Background()); err == nil {
				communicator.httpClient = protoHttp.NewHttpClient(componentCommunicator.GrpcClient)
				return communicator
			}
		}
	}
	return nil
}

func (communicator *HttpCommunicator) ProcessRequest(ctx context.Context, request *HttpRequest) (*HttpResponse, error) {
	grpcStream, err := communicator.Stream(ctx)
	if err != nil {
		return nil, err
	}
	err = send(grpcStream, request)
	if err != nil {
		return nil, err
	}
	response := &HttpResponse{Body: NewBody()}
	err = receive(grpcStream, response)
	return response, err
}

func (communicator *HttpCommunicator) Stream(ctx context.Context) (protoHttp.Http_ProcessRequestClient, error) {
	return communicator.httpClient.ProcessRequest(ctx)
}

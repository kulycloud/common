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
	grpcStream, err := communicator.httpClient.ProcessRequest(ctx)
	if err != nil {
		return nil, err
	}
	err = sendRequest(grpcStream, request)
	if err != nil {
		return nil, err
	}
	return receiveResponse(grpcStream)
}

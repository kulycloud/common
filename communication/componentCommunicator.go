package communication

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	"google.golang.org/grpc"
)

type ComponentCommunicator struct {
	Client     protoCommon.ComponentClient
	GrpcClient grpc.ClientConnInterface
}

func NewComponentCommunicatorFromEndpoint(endpoint *protoCommon.Endpoint) (*ComponentCommunicator, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%v", endpoint.Host, endpoint.Port), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("could not create connection to component: %w", err)
	}

	return NewComponentCommunicator(conn), nil
}

func NewComponentCommunicator(grpcClient grpc.ClientConnInterface) *ComponentCommunicator {
	return &ComponentCommunicator{GrpcClient: grpcClient, Client: protoCommon.NewComponentClient(grpcClient)}
}

func (communicator *ComponentCommunicator) Ping(ctx context.Context) error {
	_, err := communicator.Client.Ping(ctx, &protoCommon.Empty{})
	return err
}

type RemoteComponent interface {
	Ping(ctx context.Context) error
}

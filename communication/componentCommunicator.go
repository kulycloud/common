package communication

import (
	"context"
	protoCommon "github.com/kulycloud/protocol/common"
	"google.golang.org/grpc"
)

type ComponentCommunicator struct {
	Client protoCommon.ComponentClient
	GrpcClient grpc.ClientConnInterface
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
package communication

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	protoControlPlane "github.com/kulycloud/protocol/control-plane"
	"google.golang.org/grpc"
)

type Communicator struct {
	connection *grpc.ClientConn
	controlPlaneClient protoControlPlane.ControlPlaneClient
}

func NewCommunicator() *Communicator {
	return &Communicator{}
}

func (communicator *Communicator) Connect(host string, port uint32) error {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%v", host, port), grpc.WithInsecure())
	if err != nil {
		return err
	}
	communicator.controlPlaneClient = protoControlPlane.NewControlPlaneClient(conn)
	return nil
}

func (communicator *Communicator) RegisterThisService(ctx context.Context, typeName string, ownHost string, ownPort uint32 ) error {
	_, err := communicator.controlPlaneClient.RegisterComponent(ctx, &protoControlPlane.RegisterComponentRequest {
		Type:     typeName,
		Endpoint: &protoCommon.Endpoint{
			Host: ownHost,
			Port: ownPort,
		},
	})

	if err != nil {
		return fmt.Errorf("error from control-plane during connection: %w", err)
	}
	return nil
}

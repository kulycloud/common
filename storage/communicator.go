package storage

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
	"google.golang.org/grpc"
)

type Communicator struct {
	client protoStorage.StorageClient
}

func NewCommunicator(grpcClient grpc.ClientConnInterface) *Communicator {
	return &Communicator{client: protoStorage.NewStorageClient(grpcClient)}
}

func (communicator *Communicator) Check(ctx context.Context) error {
	_, err := communicator.client.Ping(ctx, &protoCommon.Empty{})
	return err
}

func (communicator *Communicator) GetRouteByNamespacedName(ctx context.Context, namespace string, name string) (*protoStorage.RouteWithId, error) {
	resp, err := communicator.client.GetRoute(ctx, &protoStorage.GetRouteRequest{Id: &protoStorage.GetRouteRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Route, nil
}

func (communicator *Communicator) GetRouteByUID(ctx context.Context, UID string) (*protoStorage.RouteWithId, error) {
	resp, err := communicator.client.GetRoute(ctx, &protoStorage.GetRouteRequest{Id: &protoStorage.GetRouteRequest_Uid{Uid: UID}})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Route, nil
}

func (communicator *Communicator) SetRouteByNamespacedName(ctx context.Context, namespace string, name string, route *protoStorage.Route) (string, error) {
	resp, err := communicator.client.SetRoute(ctx, &protoStorage.SetRouteRequest{Id: &protoStorage.SetRouteRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}, Data: route})

	if err != nil {
		return "", fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Uid, nil
}

func (communicator *Communicator) SetRouteByUID(ctx context.Context, UID string, route *protoStorage.Route) (string, error) {
	resp, err := communicator.client.SetRoute(ctx, &protoStorage.SetRouteRequest{Id: &protoStorage.SetRouteRequest_Uid{Uid: UID}, Data: route})

	if err != nil {
		return "", fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Uid, nil
}

func (communicator *Communicator) GetRouteStepByNamespacedName(ctx context.Context, namespace string, name string, stepId uint32) (*protoStorage.RouteStep, error) {
	resp, err := communicator.client.GetRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}


func (communicator *Communicator) GetRouteStepByUID(ctx context.Context, UID string, stepId uint32) (*protoStorage.RouteStep, error) {
	resp, err := communicator.client.GetRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_Uid{Uid: UID}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}

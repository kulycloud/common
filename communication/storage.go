package communication

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	protoStorage "github.com/kulycloud/protocol/storage"
)

var _ RemoteComponent = &StorageCommunicator{}

type StorageCommunicator struct {
	*ComponentCommunicator
	storageClient protoStorage.StorageClient
	Endpoints     []*protoCommon.Endpoint
}

func NewEmptyStorageCommunicator() *StorageCommunicator {
	return &StorageCommunicator{}
}

func NewStorageCommunicator(componentCommunicator *ComponentCommunicator) *StorageCommunicator {
	return &StorageCommunicator{ComponentCommunicator: componentCommunicator, storageClient: protoStorage.NewStorageClient(componentCommunicator.GrpcClient)}
}

func (communicator *StorageCommunicator) Ready() bool {
	return communicator.storageClient != nil
}

func (communicator *StorageCommunicator) UpdateComponentCommunicator(componentCommunicator *ComponentCommunicator) {
	communicator.ComponentCommunicator = componentCommunicator

	if componentCommunicator != nil {
		communicator.storageClient = protoStorage.NewStorageClient(componentCommunicator.GrpcClient)
	} else {
		communicator.storageClient = nil
	}
}

func (communicator *StorageCommunicator) GetRouteByNamespacedName(ctx context.Context, namespace string, name string) (*protoStorage.RouteWithId, error) {
	resp, err := communicator.storageClient.GetRoute(ctx, &protoStorage.GetRouteRequest{Id: &protoStorage.GetRouteRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Route, nil
}

func (communicator *StorageCommunicator) GetRouteByUID(ctx context.Context, UID string) (*protoStorage.RouteWithId, error) {
	resp, err := communicator.storageClient.GetRoute(ctx, &protoStorage.GetRouteRequest{Id: &protoStorage.GetRouteRequest_Uid{Uid: UID}})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Route, nil
}

func (communicator *StorageCommunicator) SetRouteByNamespacedName(ctx context.Context, namespace string, name string, route *protoStorage.Route) (string, error) {
	resp, err := communicator.storageClient.SetRoute(ctx, &protoStorage.SetRouteRequest{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, Data: route})

	if err != nil {
		return "", fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Uid, nil
}

func (communicator *StorageCommunicator) GetRouteStepByNamespacedName(ctx context.Context, namespace string, name string, stepId uint32) (*protoStorage.RouteStep, error) {
	resp, err := communicator.storageClient.GetRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}

func (communicator *StorageCommunicator) GetRouteStepByUID(ctx context.Context, UID string, stepId uint32) (*protoStorage.RouteStep, error) {
	resp, err := communicator.storageClient.GetRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_Uid{Uid: UID}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}

func (communicator *StorageCommunicator) GetPopulatedRouteStepByNamespacedName(ctx context.Context, namespace string, name string, stepId uint32) (*protoStorage.PopulatedRouteStep, error) {
	resp, err := communicator.storageClient.GetPopulatedRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_NamespacedName{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}

func (communicator *StorageCommunicator) GetPopulatedRouteStepByUID(ctx context.Context, UID string, stepId uint32) (*protoStorage.PopulatedRouteStep, error) {
	resp, err := communicator.storageClient.GetPopulatedRouteStep(ctx, &protoStorage.GetRouteStepRequest{Id: &protoStorage.GetRouteStepRequest_Uid{Uid: UID}, StepId: stepId})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Step, nil
}

func (communicator *StorageCommunicator) GetRoutesInNamespace(ctx context.Context, namespace string) ([]string, error) {
	resp, err := communicator.storageClient.GetRoutesInNamespace(ctx, &protoStorage.GetRoutesInNamespaceRequest{Namespace: namespace})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.RouteUids, nil
}

func (communicator *StorageCommunicator) GetRouteStart(ctx context.Context, host string) (*protoStorage.GetRouteStartResponse, error) {
	resp, err := communicator.storageClient.GetRouteStart(ctx, &protoStorage.GetRouteStartRequest{Host: host})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp, nil
}

func (communicator *StorageCommunicator) DeleteRoute(ctx context.Context, namespace string, name string) error {
	_, err := communicator.storageClient.DeleteRoute(ctx, &protoStorage.DeleteRouteRequest{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}})

	if err != nil {
		return fmt.Errorf("error from storage provider: %w", err)
	}

	return nil
}

func (communicator *StorageCommunicator) GetService(ctx context.Context, namespace string, name string) (*protoStorage.Service, error) {
	resp, err := communicator.storageClient.GetService(ctx, &protoStorage.GetServiceRequest{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Service, nil
}

func (communicator *StorageCommunicator) SetService(ctx context.Context, namespace string, name string, service *protoStorage.Service) error {
	_, err := communicator.storageClient.SetService(ctx, &protoStorage.SetServiceRequest{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, Service: service})

	if err != nil {
		return fmt.Errorf("error from storage provider: %w", err)
	}

	return nil
}

func (communicator *StorageCommunicator) GetServicesInNamespace(ctx context.Context, namespace string) ([]string, error) {
	resp, err := communicator.storageClient.GetServicesInNamespace(ctx, &protoStorage.GetServicesInNamespaceRequest{Namespace: namespace})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Names, nil
}

func (communicator *StorageCommunicator) GetServiceLBEndpoints(ctx context.Context, namespace string, name string) ([]*protoCommon.Endpoint, error) {
	resp, err := communicator.storageClient.GetServiceLBEndpoints(ctx, &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	})

	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return resp.Endpoints, nil
}

func (communicator *StorageCommunicator) SetServiceLBEndpoints(ctx context.Context, namespace string, name string, endpoints []*protoCommon.Endpoint) error {
	_, err := communicator.storageClient.SetServiceLBEndpoints(ctx, &protoStorage.SetServiceLBEndpointsRequest{ServiceName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, Endpoints: endpoints})

	if err != nil {
		return fmt.Errorf("error from storage provider: %w", err)
	}

	return nil
}

func (communicator *StorageCommunicator) DeleteService(ctx context.Context, namespace string, name string) error {
	_, err := communicator.storageClient.DeleteService(ctx, &protoStorage.DeleteServiceRequest{NamespacedName: &protoStorage.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}})

	if err != nil {
		return fmt.Errorf("error from storage provider: %w", err)
	}

	return nil
}

func (communicator *StorageCommunicator) GetNamespaces(ctx context.Context) ([]string, error) {
	ns, err := communicator.storageClient.GetNamespaces(ctx, &protoCommon.Empty{})
	if err != nil {
		return nil, fmt.Errorf("error from storage provider: %w", err)
	}

	return ns.Namespaces, nil
}

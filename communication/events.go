package communication

import (
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	protoControlPlane "github.com/kulycloud/protocol/control-plane"
)

type EventType string

const (
	StorageChangedEvent       EventType = "storageChanged"
	ConfigurationChangedEvent EventType = "configurationChanged"
	ClusterChangedEvent       EventType = "clusterChanged"
)

type Event interface {
	GetType() EventType
	ToGrpcEvent() *protoCommon.Event
}

type StorageChanged protoCommon.StorageChangedEvent

func NewStorageChanged(endpoints []*protoCommon.Endpoint) *StorageChanged {
	return &StorageChanged{
		Endpoints: endpoints,
	}
}

func (e *StorageChanged) GetType() EventType {
	return StorageChangedEvent
}

func (e *StorageChanged) ToGrpcEvent() *protoCommon.Event {
	return &protoCommon.Event{
		Type: string(StorageChangedEvent),
		Data: &protoCommon.Event_StorageChanged{
			StorageChanged: (*protoCommon.StorageChangedEvent)(e),
		},
	}
}

type ConfigurationChanged protoCommon.ConfigurationChangedEvent

func NewConfigurationChanged(resource *protoCommon.Resource) *ConfigurationChanged {
	return &ConfigurationChanged{
		Resource: resource,
	}
}

func (e *ConfigurationChanged) GetType() EventType {
	return ConfigurationChangedEvent
}

func (e *ConfigurationChanged) ToGrpcEvent() *protoCommon.Event {
	return &protoCommon.Event{
		Type: string(ConfigurationChangedEvent),
		Data: &protoCommon.Event_ConfigurationChanged{
			ConfigurationChanged: (*protoCommon.ConfigurationChangedEvent)(e),
		},
	}
}

type ClusterChanged protoCommon.ClusterChangedEvent

func NewClusterChanged(resource *protoCommon.Resource, serviceCount *protoCommon.InstanceCount, loadBalancerCount *protoCommon.InstanceCount) *ClusterChanged {
	return &ClusterChanged{
		Resource:          resource,
		ServiceCount:      serviceCount,
		LoadBalancerCount: loadBalancerCount,
	}
}

func (e *ClusterChanged) GetType() EventType {
	return ClusterChangedEvent
}

func (e *ClusterChanged) ToGrpcEvent() *protoCommon.Event {
	return &protoCommon.Event{
		Type: string(ClusterChangedEvent),
		Data: &protoCommon.Event_ClusterChanged{
			ClusterChanged: (*protoCommon.ClusterChangedEvent)(e),
		},
	}
}

type (
	StorageChangedHandler       func(*StorageChanged)
	ConfigurationChangedHandler func(*ConfigurationChanged)
	ClusterChangedHandler       func(*ClusterChanged)
)

func NewResource(resourceType string, namespace string, name string) *protoCommon.Resource {
	return &protoCommon.Resource{
		Type:      resourceType,
		Namespace: namespace,
		Name:      name,
	}
}

func NewInstanceCount(expected uint32, actual uint32) *protoCommon.InstanceCount {
	return &protoCommon.InstanceCount{
		Expected: expected,
		Actual:   actual,
	}
}

func newListenToEventRequest(identifier string, eventType EventType) *protoControlPlane.ListenToEventRequest {
	return &protoControlPlane.ListenToEventRequest{
		Type:        string(eventType),
		Destination: identifier,
	}
}

func NewIdentifierFromEndpoint(endpoint *protoCommon.Endpoint) string {
	return fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
}

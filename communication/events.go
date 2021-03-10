package communication

import (
    protoCommon "github.com/kulycloud/protocol/common"
    protoControlPlane "github.com/kulycloud/protocol/control-plane"
)

type EventType string

const (
    StorageChangedEvent EventType = "storageChanged"
    ConfigurationChangedEvent EventType = "configurationChanged"
    ClusterChangedEvent EventType = "clusterChanged"
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

func NewConfigurationChanged(namespace string) *ConfigurationChanged {
    return &ConfigurationChanged{
        Source: NewResource(namespace),
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

func NewClusterChanged(namespace string) *ClusterChanged {
    return &ClusterChanged{
        Source: NewResource(namespace),
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
    StorageChangedHandler func(*StorageChanged)
    ConfigurationChangedHandler func(*ConfigurationChanged)
    ClusterChangedHandler func(*ClusterChanged)
)

func NewResource(namespace string) *protoCommon.Resource {
    return &protoCommon.Resource{
        Namespace: namespace,
    }
}

func newListenToEventRequest(eventType EventType) *protoControlPlane.ListenToEventRequest {
    return &protoControlPlane.ListenToEventRequest{
        Type: string(eventType),
    }
}


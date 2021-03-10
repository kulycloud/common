package communication

import (
	"context"
	"fmt"
	"io"

	"github.com/kulycloud/common/logging"
	protoCommon "github.com/kulycloud/protocol/common"
	protoControlPlane "github.com/kulycloud/protocol/control-plane"
	"google.golang.org/grpc"
)

var logger = logging.GetForComponent("common-communication")

type ControlPlaneCommunicator struct {
	connection *grpc.ClientConn
	controlPlaneClient protoControlPlane.ControlPlaneClient

    storageChangedHandlers []StorageChangedHandler
    configurationChangedHandlers []ConfigurationChangedHandler
    clusterChangedHandlers []ClusterChangedHandler
}

func NewControlPlaneCommunicator() *ControlPlaneCommunicator {
	return &ControlPlaneCommunicator{
        storageChangedHandlers: make([]StorageChangedHandler, 0),
        configurationChangedHandlers: make([]ConfigurationChangedHandler, 0),
        clusterChangedHandlers: make([]ClusterChangedHandler, 0),
    }
}

func (communicator *ControlPlaneCommunicator) Connect(host string, port uint32) error {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%v", host, port), grpc.WithInsecure())
	if err != nil {
		return err
	}
	communicator.controlPlaneClient = protoControlPlane.NewControlPlaneClient(conn)
	return nil
}

func (communicator *ControlPlaneCommunicator) RegisterThisService(ctx context.Context, typeName string, ownHost string, ownPort uint32 ) error {
	stream, err := communicator.controlPlaneClient.RegisterComponent(ctx, &protoControlPlane.RegisterComponentRequest {
		Type:     typeName,
		Endpoint: &protoCommon.Endpoint{
			Host: ownHost,
			Port: ownPort,
		},
	})

	if err != nil {
		return fmt.Errorf("error from control-plane during connection: %w", err)
	}

    go func() {
        for {
            event, err := stream.Recv()
            if err != nil {
                if err != io.EOF {
                    // might want to log the error
                }
                return
            }
            communicator.processEvent(event)
        }
    }()

	return nil
}

func (communicator *ControlPlaneCommunicator) CreateEvent(event Event) error {
    _, err := communicator.controlPlaneClient.CreateEvent(context.Background(), event.ToGrpcEvent())
    return err
}

func (communicator *ControlPlaneCommunicator) RegisterStorageChangedHandler(handler StorageChangedHandler) error {
    request := newListenToEventRequest(StorageChangedEvent)
    _, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
    if err != nil {
        return err
    }
    communicator.storageChangedHandlers = append(communicator.storageChangedHandlers, handler)
    return nil
}

func (communicator *ControlPlaneCommunicator) RegisterConfigurationChangedHandler(handler ConfigurationChangedHandler) error {
    request := newListenToEventRequest(ConfigurationChangedEvent)
    _, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
    if err != nil {
        return err
    }
    communicator.configurationChangedHandlers = append(communicator.configurationChangedHandlers, handler)
    return nil
}

func (communicator *ControlPlaneCommunicator) RegisterClusterChangedHandler(handler ClusterChangedHandler) error {
    request := newListenToEventRequest(ClusterChangedEvent)
    _, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
    if err != nil {
        return err
    }
    communicator.clusterChangedHandlers = append(communicator.clusterChangedHandlers, handler)
    return nil
}


func (communicator *ControlPlaneCommunicator) processEvent(event *protoCommon.Event) {
    switch EventType(event.Type) {
        case StorageChangedEvent:
            sc := (*StorageChanged)(event.GetStorageChanged())
            if sc != nil {
                for _, handler := range communicator.storageChangedHandlers {
                    handler(sc)
                }
            }
        case ConfigurationChangedEvent:
            cc := (*ConfigurationChanged)(event.GetConfigurationChanged())
            if cc != nil {
                for _, handler := range communicator.configurationChangedHandlers {
                    handler(cc)
                }
            }
        case ClusterChangedEvent:
            cc := (*ClusterChanged)(event.GetClusterChanged())
            if cc != nil {
                for _, handler := range communicator.clusterChangedHandlers {
                    handler(cc)
                }
            }
    }
}


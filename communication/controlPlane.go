package communication

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/kulycloud/common/logging"
	protoCommon "github.com/kulycloud/protocol/common"
	protoControlPlane "github.com/kulycloud/protocol/control-plane"
	"google.golang.org/grpc"
)

var logger = logging.GetForComponent("common-communication")

type ControlPlaneCommunicator struct {
	connection         *grpc.ClientConn
	controlPlaneClient protoControlPlane.ControlPlaneClient

	storageChangedHandlers       []StorageChangedHandler
	configurationChangedHandlers []ConfigurationChangedHandler
	clusterChangedHandlers       []ClusterChangedHandler
	identifier                   string
}

func NewControlPlaneCommunicator() *ControlPlaneCommunicator {
	return &ControlPlaneCommunicator{
		storageChangedHandlers:       make([]StorageChangedHandler, 0),
		configurationChangedHandlers: make([]ConfigurationChangedHandler, 0),
		clusterChangedHandlers:       make([]ClusterChangedHandler, 0),
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

// Register service asynchronosly
// the returned channel will either receive nil or an error that was encountered during setup, then close the channel
func (communicator *ControlPlaneCommunicator) RegisterThisService(ctx context.Context, typeName string, ownHost string, ownPort uint32) <-chan error {
	done := make(chan error)
	go func() {
		endpoint := &protoCommon.Endpoint{
			Host: ownHost,
			Port: ownPort,
		}
		stream, err := communicator.controlPlaneClient.RegisterComponent(ctx, &protoControlPlane.RegisterComponentRequest{
			Type:     typeName,
			Endpoint: endpoint,
		})

		if err != nil {
			done <- err
			return
		}

		// validate the stream has been created
		_, err = stream.Recv()
		if err != nil {
			done <- err
			return
		}

		// necessary to identify itself to the control plane
		communicator.identifier = NewIdentifierFromEndpoint(endpoint)

		// start receiving events from stream in the background
		go func() {
			for {
				event, err := stream.Recv()
				if err != nil {
					if err != io.EOF {
						logger.Warnw("could not receive data from stream", "error", err)
					}
					return
				}
				communicator.processEvent(event)
			}
		}()
		done <- nil
		close(done)

		// keep this background task running until the stream is closed
		// otherwise the stream would be garbage collected, in turn closing the context
		<-stream.Context().Done()
		logger.Info("event stream closed")
	}()

	return done
}

func (communicator *ControlPlaneCommunicator) CreateEvent(event Event) error {
	_, err := communicator.controlPlaneClient.CreateEvent(context.Background(), event.ToGrpcEvent())
	return err
}

// add handler to the list of handlers for StorageChanged events and requests control plane to listen on StorageChanged
func (communicator *ControlPlaneCommunicator) RegisterStorageChangedHandler(handler StorageChangedHandler) error {
	request := newListenToEventRequest(communicator.identifier, StorageChangedEvent)
	_, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
	if err != nil {
		return err
	}
	communicator.storageChangedHandlers = append(communicator.storageChangedHandlers, handler)
	return nil
}

// add handler to the list of handlers for ConfigurationChanged events and requests control plane to listen on ConfigurationChanged
func (communicator *ControlPlaneCommunicator) RegisterConfigurationChangedHandler(handler ConfigurationChangedHandler) error {
	request := newListenToEventRequest(communicator.identifier, ConfigurationChangedEvent)
	_, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
	if err != nil {
		return err
	}
	communicator.configurationChangedHandlers = append(communicator.configurationChangedHandlers, handler)
	return nil
}

// add handler to the list of handlers for ClusterChanged events and requests control plane to listen on ClusterChanged
func (communicator *ControlPlaneCommunicator) RegisterClusterChangedHandler(handler ClusterChangedHandler) error {
	request := newListenToEventRequest(communicator.identifier, ClusterChangedEvent)
	_, err := communicator.controlPlaneClient.ListenToEvent(context.Background(), request)
	if err != nil {
		return err
	}
	communicator.clusterChangedHandlers = append(communicator.clusterChangedHandlers, handler)
	return nil
}

// Register to the control plane asynchronosly
// If it cannot register to the control plane within 5 tries it will panic as it does not make sense to continue running
// If it does not panic the returned channel will receive a valid *ControlPlaneCommunicator
func RegisterToControlPlane(typeName string, host string, port uint32, cpHost string, cpPort uint32) <-chan *ControlPlaneCommunicator {
	communicator := make(chan *ControlPlaneCommunicator)

	go func() {
		var err error
		var controlPlaneComm *ControlPlaneCommunicator
		numberOfTries := 0
		for {
			numberOfTries++
			controlPlaneComm, err = register(typeName, host, port, cpHost, cpPort)
			if err == nil || numberOfTries > 5 {
				break
			}

			logger.Info("Retrying in 5s...")
			time.Sleep(5 * time.Second)
		}
		if err != nil {
			logger.Panicw("could not register to control plane", "lastError", err)
		}

		communicator <- controlPlaneComm
		close(communicator)
	}()

	return communicator
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

func register(typeName string, host string, port uint32, cpHost string, cpPort uint32) (*ControlPlaneCommunicator, error) {
	comm := NewControlPlaneCommunicator()
	err := comm.Connect(cpHost, cpPort)
	if err != nil {
		logger.Errorw("Could not connect to control-plane", "error", err)
		return nil, err
	}
	err = <-comm.RegisterThisService(context.Background(), typeName, host, port)
	if err != nil {
		logger.Errorw("Could not register service", "error", err)
		return nil, err
	}
	logger.Info("Registered to control-plane")
	return comm, nil
}

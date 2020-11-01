package communication

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

type NewStorageHandler func(ctx context.Context, endpoint []*protoCommon.Endpoint)

type Listener struct {
	Server *grpc.Server
	Listener net.Listener
	logger *zap.SugaredLogger
	Storage *StorageCommunicator
	NewStorageHandlers []NewStorageHandler
}

func NewListener(logger *zap.SugaredLogger) *Listener {
	return &Listener{
		logger: logger,
		NewStorageHandlers: make([]NewStorageHandler, 0),
		Storage: NewEmptyStorageCommunicator(),
	}
}

func (listener *Listener) Setup(port uint32) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return err
	}
	listener.Listener = lis
	listener.Server = grpc.NewServer()
	listener.logger.Infow("created server", "port", port)
	protoCommon.RegisterComponentServer(listener.Server, &componentHandler{ listener: listener })
	return nil
}

func (listener *Listener) Serve() error {
	listener.logger.Infow("serving")
	return listener.Server.Serve(listener.Listener)
}

var _ protoCommon.ComponentServer = &componentHandler{}
type componentHandler struct {
	protoCommon.UnimplementedComponentServer
	listener *Listener
}

func (handler *componentHandler) Ping(ctx context.Context, empty *protoCommon.Empty) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, nil
}

func (handler *componentHandler) RegisterStorageEndpoints(ctx context.Context, endpoints *protoCommon.EndpointList) (*protoCommon.Empty, error) {
	// This logic will change when supporting multiple endpoints (tracked in KU-50)
	var comm *ComponentCommunicator = nil
	var err error = nil

	if endpoints.Endpoints != nil && len(endpoints.Endpoints) > 0 {
		comm, err = NewComponentCommunicatorFromEndpoint(endpoints.Endpoints[0])

		if err == nil {
			err = comm.Ping(ctx)
			if err != nil {
				// We cannot ping the new storage. We should explicitly set it to nil
				comm = nil
			}
		} else {
			comm = nil
		}
	}

	handler.listener.Storage.UpdateComponentCommunicator(comm)
	handler.listener.Storage.Endpoints = endpoints.Endpoints

	if err == nil {
		for _, handler := range handler.listener.NewStorageHandlers {
			handler(ctx, endpoints.Endpoints)
		}
	}

	if err != nil {
		handler.listener.logger.Warnw("Error registering new storage endpoints", "endpoints", endpoints.Endpoints)
	} else {
		handler.listener.logger.Infow("Registered new storage endpoints", "endpoints", endpoints.Endpoints)
	}

	return &protoCommon.Empty{}, err
}

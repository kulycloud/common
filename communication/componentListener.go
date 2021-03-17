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
	Server             *grpc.Server
	Listener           net.Listener
	logger             *zap.SugaredLogger
	NewStorageHandlers []NewStorageHandler
}

func NewListener(logger *zap.SugaredLogger) *Listener {
	return &Listener{
		logger:             logger,
		NewStorageHandlers: make([]NewStorageHandler, 0),
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
	protoCommon.RegisterComponentServer(listener.Server, &componentHandler{listener: listener})
	return nil
}

// Starts server asynchronously
// caller can decide when to wait for the result
func (listener *Listener) Serve() <-chan error {
	errChan := make(chan error)
	go func() {
		listener.logger.Infow("serving")
		err := listener.Server.Serve(listener.Listener)
		errChan <- err
		close(errChan)
	}()
	return errChan
}

var _ protoCommon.ComponentServer = &componentHandler{}

type componentHandler struct {
	protoCommon.UnimplementedComponentServer
	listener *Listener
}

func (handler *componentHandler) Ping(_ context.Context, _ *protoCommon.Empty) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, nil
}

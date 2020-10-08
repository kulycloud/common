package communication

import (
	"context"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

type Listener struct {
	Server *grpc.Server
	Listener net.Listener
	logger *zap.SugaredLogger
}

func NewListener(logger *zap.SugaredLogger) *Listener {
	return &Listener{
		logger: logger,
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
	protoCommon.RegisterComponentServer(listener.Server, &componentHandler{})
	return nil
}

func (listener *Listener) Serve() error {
	listener.logger.Infow("serving")
	return listener.Server.Serve(listener.Listener)
}

var _ protoCommon.ComponentServer = &componentHandler{}
type componentHandler struct {}


func (handler *componentHandler) Ping(ctx context.Context, empty *protoCommon.Empty) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, nil
}

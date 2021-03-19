package http

import (
	"context"
	"errors"
	"fmt"
	protoCommon "github.com/kulycloud/protocol/common"
	"google.golang.org/grpc"
	"net"

	"github.com/kulycloud/common/logging"
	protoHttp "github.com/kulycloud/protocol/http"
)

var ErrCouldNotCreateServer = errors.New("could not create server")

var _ protoHttp.HttpServer = &httpHandler{}

var logger = logging.GetForComponent("http")

type httpHandler struct {
	protoHttp.UnimplementedHttpServer
	handlerFunc HandlerFunc
}

type Server struct {
	handler  *httpHandler
	server   *grpc.Server
	listener net.Listener
}

func newHttpHandler(handlerFunc HandlerFunc) *httpHandler {
	return &httpHandler{
		handlerFunc: handlerFunc,
	}
}

func (server *httpHandler) Ping(_ context.Context, _ *protoCommon.Empty) (*protoCommon.Empty, error) {
	return &protoCommon.Empty{}, nil
}

func (server *httpHandler) ProcessRequest(grpcStream protoHttp.Http_ProcessRequestServer) error {
	request := NewRequest()
	err, recvErrs := receive(grpcStream, request)
	if err != nil {
		return err
	}
	go logErrors(recvErrs)
	response := server.handlerFunc(grpcStream.Context(), request)
	// set request uid for debug purposes
	response.RequestUid = request.KulyData.GetRequestUid()
	err, sendErrs := send(grpcStream, response)
	if err != nil {
		return err
	}
	waitUntilDone(sendErrs)
	return nil
}

func NewServer(httpPort uint32, handlerFunc HandlerFunc) (*Server, error) {
	handler := newHttpHandler(handlerFunc)
	server := &Server{
		handler: handler,
	}
	err := server.setup(httpPort, handler)
	if err != nil {
		logger.Errorw("error during http listener setup", "error", err)
		return nil, ErrCouldNotCreateServer
	}
	return server, nil
}

func (hs *Server) setup(port uint32, handler *httpHandler) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return err
	}
	hs.listener = lis
	hs.server = grpc.NewServer()
	logger.Infow("created server", "port", port)
	protoHttp.RegisterHttpServer(hs.server, handler)
	return nil
}

func (hs *Server) Serve() error {
	logger.Infow("serving")
	err := hs.server.Serve(hs.listener)
	return err
}

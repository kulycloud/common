package http

import (
	"errors"

	"github.com/kulycloud/common/communication"
	commonCommunication "github.com/kulycloud/common/communication"
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
	listener *commonCommunication.Listener
}

func newHttpHandler(handlerFunc HandlerFunc) *httpHandler {
	return &httpHandler{
		handlerFunc: handlerFunc,
	}
}

func (server *httpHandler) Register(listener *communication.Listener) {
	protoHttp.RegisterHttpServer(listener.Server, server)
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
	listener := commonCommunication.NewListener(logging.GetForComponent("listener"))
	err := listener.Setup(httpPort)
	if err != nil {
		logger.Errorw("error during http listener setup", "error", err)
		return nil, ErrCouldNotCreateServer
	}
	handler := newHttpHandler(handlerFunc)
	handler.Register(listener)
	server := &Server{
		handler:  handler,
		listener: listener,
	}
	return server, nil
}

func (hs *Server) Serve() error {
	return hs.listener.Serve()
}

package http

import (
	"fmt"
	"github.com/kulycloud/common/communication"
	commonCommunication "github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoHttp "github.com/kulycloud/protocol/http"
)

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
	request := &Request{Body: NewBody()}
	err := receive(grpcStream, request)
	if err != nil {
		return err
	}
	response := server.handlerFunc(request)
	// set request uid for debug purposes
	response.setRequestUid(request.GetKulyData().GetRequestUid())
	err = send(grpcStream, response)
	return err
}

func NewServer(httpPort uint32, handlerFunc HandlerFunc) *Server {
	listener := commonCommunication.NewListener(logging.GetForComponent("listener"))
	err := listener.Setup(httpPort)
	if err != nil {
		logger.Errorw("error during http listener setup", "error", err)
		return nil
	}
	handler := newHttpHandler(handlerFunc)
	handler.Register(listener)
	server := &Server{
		handler:  handler,
		listener: listener,
	}
	return server
}

func (hs *Server) Serve() error {
	if hs == nil {
		return fmt.Errorf("could not create server")
	}
	return hs.listener.Serve()
}

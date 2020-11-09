package http

import (
	"github.com/kulycloud/common/communication"
	"github.com/kulycloud/common/logging"
	protoHttp "github.com/kulycloud/protocol/http"
)

var _ protoHttp.HttpServer = &HttpServer{}

var logger = logging.GetForComponent("server")

type HttpServer struct {
	protoHttp.UnimplementedHttpServer
	handler HttpHandler
}

func NewHttpServer(handler HttpHandler) *HttpServer {
	return &HttpServer{
		handler: handler,
	}
}

func (server *HttpServer) Register(listener *communication.Listener) {
	protoHttp.RegisterHttpServer(listener.Server, server)
}

func (server *HttpServer) ProcessRequest(grpcStream protoHttp.Http_ProcessRequestServer) error {
	request := newEmptyHttpRequest()
	err := receive(grpcStream, request)
	if err != nil {
		return err
	}
	response := server.handler.HandleRequest(request)
	// set request uid for debug purposes
	response.withRequestUid(request.GetKulyData().GetRequestUid())
	err = send(grpcStream, response)
	return err
}

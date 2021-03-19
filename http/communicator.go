package http

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"sort"
	"sync"
	"time"

	"github.com/kulycloud/common/communication"
	protoCommon "github.com/kulycloud/protocol/common"
	protoHttp "github.com/kulycloud/protocol/http"
)

var _ communication.RemoteComponent = &Communicator{}

type Communicator struct {
	grpcClient  grpc.ClientConnInterface
	httpClient  protoHttp.HttpClient
	metrics     Metrics
	metricMutex sync.Mutex
}

type Metrics struct {
	LastUseTS    int64
	ResponseTime int64
}

var ErrNoSuitableEndpoint = errors.New("no suitable endpoint found")

func NewCommunicatorFromEndpoint(ctx context.Context, endpoint *protoCommon.Endpoint) (*Communicator, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%v", endpoint.Host, endpoint.Port), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("could not create connection to component: %w", err)
	}
	communicator := &Communicator{
		grpcClient: conn,
		httpClient: protoHttp.NewHttpClient(conn),
	}
	if err = communicator.Ping(ctx); err == nil {
		return communicator, nil
	}
	return communicator, err
}

// Return best communicator for endpoints based on response time
// If no valid communicator is found nil and an error is returned
func NewCommunicator(ctx context.Context, endpoints []*protoCommon.Endpoint) (*Communicator, error) {
	comms := make([]*Communicator, 0, len(endpoints))
	for _, endpoint := range endpoints {
		communicator, err := NewCommunicatorFromEndpoint(ctx, endpoint)
		if err != nil {
			logger.Warnw("could not create communicator", "endpoint", endpoint, "error", err)
		} else {
			comms = append(comms, communicator)
		}
	}

	if len(comms) < 1 {
		return nil, ErrNoSuitableEndpoint
	}

	sort.Slice(comms, func(i, j int) bool {
		// could use more than just response time to make decision
		return comms[i].metrics.ResponseTime < comms[j].metrics.ResponseTime
	})
	return comms[0], nil
}

func (communicator *Communicator) setLastUseTS(lastUseTS int64) {
	communicator.metricMutex.Lock()
	defer communicator.metricMutex.Unlock()
	communicator.metrics.LastUseTS = lastUseTS
}

func (communicator *Communicator) setResponseTime(responseTime int64) {
	communicator.metricMutex.Lock()
	defer communicator.metricMutex.Unlock()
	communicator.metrics.ResponseTime = responseTime
}

func (communicator *Communicator) GetMetrics() Metrics {
	communicator.metricMutex.Lock()
	defer communicator.metricMutex.Unlock()
	return communicator.metrics
}

func (communicator *Communicator) Ping(ctx context.Context) error {
	start := time.Now()
	_, err := communicator.httpClient.Ping(ctx, &protoCommon.Empty{})
	communicator.setResponseTime(time.Since(start).Milliseconds())
	return err
}

func (communicator *Communicator) ProcessRequest(ctx context.Context, request *Request) (*Response, error) {
	requestTS := time.Now()
	communicator.setLastUseTS(requestTS.Unix())

	grpcStream, err := communicator.Stream(ctx)
	if err != nil {
		return nil, err
	}
	err, sendErrs := send(grpcStream, request)
	if err != nil {
		return nil, err
	}
	go logErrors(sendErrs)
	response := NewResponse()
	err, recvErrs := receive(grpcStream, response)
	if err != nil {
		return nil, err
	}
	go logErrors(recvErrs)
	return response, nil
}

func (communicator *Communicator) Stream(ctx context.Context) (protoHttp.Http_ProcessRequestClient, error) {
	return communicator.httpClient.ProcessRequest(ctx)
}

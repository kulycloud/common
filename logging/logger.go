package logging

import (
	"fmt"
	"go.uber.org/zap"
)

var RootLogger *zap.SugaredLogger

func Init() {
	_logger, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("error initializing logger: %s", err.Error()))
	}
	RootLogger = _logger.Sugar()
}

func Sync() {
	_ = RootLogger.Sync()
}

func GetForComponent(component string) *zap.SugaredLogger {
	if RootLogger == nil {
		Init()
	}
	return RootLogger.With("component", component)
}

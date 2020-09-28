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
	err := RootLogger.Sync()
	if err != nil {
		panic(fmt.Sprintf("error syncing logger: %s", err.Error()))
	}
}

func GetForComponent(component string) *zap.SugaredLogger {
	return RootLogger.With("component", component)
}

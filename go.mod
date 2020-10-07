module github.com/kulycloud/common

go 1.15

require (
	github.com/iancoleman/strcase v0.1.2 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0 // indirect
	github.com/kulycloud/protocol v1.0.0
)

replace github.com/kulycloud/protocol v1.0.0 => ../protocol

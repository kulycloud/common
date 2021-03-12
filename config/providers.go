package config

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"os"
	"strings"
)

type Provider interface {
	Get(name string) (string, error)
}

var _ Provider = &EnvironmentVariableProvider{}

type EnvironmentVariableProvider struct{}

func (envVarProv *EnvironmentVariableProvider) Get(name string) (string, error) {
	value := os.Getenv(strcase.ToScreamingSnake(name))
	if value == "" {
		return value, ErrParamNotFound
	}
	return value, nil
}

func NewEnvironmentVariableProvider() *EnvironmentVariableProvider {
	return &EnvironmentVariableProvider{}
}

var _ Provider = &CliParamProvider{}

type CliParamProvider struct{}

func (cliProv CliParamProvider) Get(name string) (string, error) {
	args := os.Args[1:]
	for i, flag := range args {
		if strings.HasPrefix(flag, "--") {
			if flag[2:] == name {
				if len(args) > i+1 {
					return args[i+1], nil
				} else {
					return "", fmt.Errorf("missing value after flag %s", flag)
				}
			}

			if strings.HasPrefix(flag[2:], name+"=") {
				return flag[len(name)+3:], nil
			}
		}
	}

	return "", ErrParamNotFound
}

func NewCliParamProvider() *CliParamProvider {
	return &CliParamProvider{}
}

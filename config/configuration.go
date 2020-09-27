package config

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrParamNotFound = errors.New("param not found")

type Parser struct {
	providers []Provider
}

func NewParser() *Parser {
	return &Parser{
		providers: make([]Provider, 0),
	}
}

func (parser *Parser) AddProvider(provider Provider) {
	parser.providers = append(parser.providers, provider)
}

func (parser *Parser) GetParam(name string) (string, error) {
	var err error
	var val string

	for _, provider := range parser.providers {
		val, err = provider.Get(name)

		if err == nil {
			break
		}
		if !errors.Is(err, ErrParamNotFound) {
			return "", err
		}
	}

	return val, err
}

func (parser *Parser) Populate(config interface{}) error {
	v := reflect.ValueOf(config).Elem()

	for i := 0; i < v.NumField(); i++ {
		field :=  v.Type().Field(i)
		fieldName, ok := field.Tag.Lookup("configName")
		if !ok {
			continue
		}

		val, err := parser.GetParam(fieldName)

		if err != nil {
			if errors.Is(err, ErrParamNotFound) {
				val, ok = field.Tag.Lookup("defaultValue")
				if !ok {
					return fmt.Errorf("param %s not specified and no default value provided: %w", fieldName, err)
				}
			} else {
				return err
			}
		}

		v.Field(i).Set(reflect.ValueOf(val).Convert(field.Type))
	}

	return nil
}

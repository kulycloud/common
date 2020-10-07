package config

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var ErrParamNotFound = errors.New("param not found")
var ErrInvalidType = errors.New("invalid type")

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

func (parser *Parser) convertTo(value string, targetType string) (interface{}, error) {
	switch targetType {
	case "string":
		return value, nil
	case "int":
		return strconv.Atoi(value)
	case "int8":
		return strconv.ParseInt(value, 10, 8)
	case "int16":
		return strconv.ParseInt(value, 10, 16)
	case "int32":
		return strconv.ParseInt(value, 10, 32)
	case "int64":
		return strconv.ParseInt(value, 10, 64)
	case "uint8":
		return strconv.ParseUint(value, 10, 8)
	case "uint16":
		return strconv.ParseUint(value, 10, 16)
	case "uint32":
		return strconv.ParseUint(value, 10, 32)
	case "uint64":
		return strconv.ParseUint(value, 10, 64)
	case "float32":
		return strconv.ParseFloat(value, 32)
	case "float64":
		return strconv.ParseFloat(value, 64)
	case "bool":
		return strconv.ParseBool(value)
	case "[]string":
		return strings.Split(value, ","), nil
	default:
		return nil, fmt.Errorf("unknown type %s: %w", targetType, ErrInvalidType)
	}
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

		convertedVal, err := parser.convertTo(val, v.Field(i).Type().String())

		if err != nil {
			return fmt.Errorf("error while populating field %s: %w", fieldName, err)
		}

		v.Field(i).Set(reflect.ValueOf(convertedVal).Convert(field.Type))
	}

	return nil
}

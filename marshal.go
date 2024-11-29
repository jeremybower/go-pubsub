package pubsub

import (
	"encoding/json"
	"reflect"
	"sync"
)

var fqnMutex sync.RWMutex
var fqnCache map[reflect.Type]string

func fullyQualifiedNameFromType[T any]() string {
	typ := reflect.TypeFor[T]()

	fqnMutex.RLock()
	fqn, ok := fqnCache[typ]
	fqnMutex.RUnlock()

	if !ok {
		fqn = typ.PkgPath() + "." + typ.Name()
		fqnMutex.Lock()
		if fqnCache == nil {
			fqnCache = make(map[reflect.Type]string)
		}
		fqnCache[typ] = fqn
		fqnMutex.Unlock()
	}

	return fqn
}

type Marshaller func(v any) (string, error)

func defaultMarshaller[T any](v any) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

type Unmarshaller func(payload string) (any, error)

func UnmarshalString(payload string) (any, error) {
	return payload, nil
}

func UnmarshalJSON[T any](payload string) (any, error) {
	var v T
	if err := json.Unmarshal([]byte(payload), &v); err != nil {
		return v, err
	}
	return v, nil
}

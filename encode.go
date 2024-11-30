package pubsub

import (
	"context"
	"reflect"
)

type Encoder interface {
	Encode(ctx context.Context, v any) (*EncodedValue, error)
}

type Decoder interface {
	ContentType() string
	Types() map[string]reflect.Type
	Decode(ctx context.Context, encodedValue *EncodedValue) (any, error)
}

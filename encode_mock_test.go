package pubsub

import (
	"context"
	"reflect"

	"github.com/stretchr/testify/mock"
)

type MockEncoder struct {
	mock.Mock
}

var _ Encoder = &MockEncoder{}

func (me *MockEncoder) Encode(ctx context.Context, value any) (*EncodedValue, error) {
	mockArgs := me.Called(ctx, value)
	return mockArgs.Get(0).(*EncodedValue), mockArgs.Error(1)
}

type MockDecoder struct {
	mock.Mock
}

var _ Decoder = &MockDecoder{}

func (md *MockDecoder) ContentType() string {
	mockArgs := md.Called()
	return mockArgs.String(0)
}

func (md *MockDecoder) Decode(ctx context.Context, encodedValue *EncodedValue) (any, error) {
	mockArgs := md.Called(ctx, encodedValue)
	return mockArgs.Get(0), mockArgs.Error(1)
}

func (md *MockDecoder) Types() map[string]reflect.Type {
	mockArgs := md.Called()
	return mockArgs.Get(0).(map[string]reflect.Type)
}

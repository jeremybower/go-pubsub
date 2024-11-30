package pubsub

import "errors"

var ErrDecoderNotRegistered = errors.New("pubsub: decoder not registered")
var ErrEncoderRequired = errors.New("pubsub: encoder required")
var ErrTopicValidation = errors.New("pubsub: invalid topic")
var ErrTopicInvalidChannelPrefix = errors.New("pubsub: channel does not start with expected prefix")
var ErrTopicInvalidID = errors.New("pubsub: invalid topic id")
var ErrTopicUnknown = errors.New("pubsub: unknown topic")
var ErrTypeUnknown = errors.New("pubsub: unknown type")

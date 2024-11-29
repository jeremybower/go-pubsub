package pubsub

import "errors"

var ErrInvalidTopic = errors.New("pubsub: invalid topic")
var ErrNoUnmarshaller = errors.New("pubsub: no unmarshaller for type")

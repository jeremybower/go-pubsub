package pubsub

import (
	"time"

	"github.com/jeremybower/go-common/optional"
)

type MessageID int64

type PublishReceipt struct {
	MessageID    MessageID
	Topics       []Topic
	Value        any
	EncodedValue *EncodedValue
	PublishedAt  time.Time
}

type SubscriptionID string

type SubscribeReceipt struct {
	MaxMessageID MessageID
	Topics       []Topic
}

type TopicID int64

type Topic struct {
	ID   TopicID
	Name string
}

type EncodedValue struct {
	ContentType string
	Bytes       []byte
}

type ControlOperation string

const (
	StopSubscriptionsControlOperation ControlOperation = "stop-subscriptions"
)

type ControlNotification struct {
	Operation ControlOperation `json:"operation"`
}

type MessageNotification struct {
	MessageID   MessageID `json:"message_id"`
	HasValue    bool      `json:"has_value"`
	PublishedAt time.Time `json:"published_at"`
}

type EncodedMessage struct {
	MessageID    MessageID
	Topic        Topic
	EncodedValue *EncodedValue
	PublishedAt  time.Time
}

type Message struct {
	ID          MessageID
	Value       any
	PublishedAt time.Time
}

type ErrorEvent struct {
	SubscriptionID SubscriptionID
	Error          error
}

type MessageEvent struct {
	SubscriptionID SubscriptionID
	Topic          Topic
	Message        Message
}

type StatusEvent struct {
	SubscriptionID SubscriptionID
	Subscribed     optional.Value[bool]
	Closed         optional.Value[bool]
	Delay          optional.Value[time.Duration]
}

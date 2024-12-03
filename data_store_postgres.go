package pubsub

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jeremybower/go-common"
	"github.com/jeremybower/go-common/postgres"
)

var _ DataStore = NewPostgresDataStore()

type PostgresDataStore struct{}

func NewPostgresDataStore() *PostgresDataStore {
	return &PostgresDataStore{}
}

func (sd *PostgresDataStore) AcquireConnection(
	ctx context.Context,
	dbPool *pgxpool.Pool,
) (*pgxpool.Conn, error) {
	conn, err := dbPool.Acquire(ctx)
	return conn, postgres.NormalizeError(err)
}

func (sd *PostgresDataStore) Publish(
	ctx context.Context,
	querier postgres.Querier,
	topicNames []string,
	value any,
	encodedValue *EncodedValue,
) (*PublishReceipt, error) {
	var id MessageID
	var topicIDs []TopicID
	var publishedAt time.Time
	var contentType *string
	var bytes []byte

	if encodedValue != nil {
		contentType = &encodedValue.ContentType
		bytes = encodedValue.Bytes
	}

	sql := "SELECT message_id, topic_ids, published_at FROM pubsub_publish($1, $2, $3);"
	err := querier.QueryRow(ctx, sql, topicNames, contentType, bytes).Scan(&id, &topicIDs, &publishedAt)
	if err != nil {
		return nil, postgres.NormalizeError(err)
	}

	// Build the topics.
	topics := make([]Topic, len(topicIDs))
	for i, id := range topicIDs {
		name := topicNames[i]
		topics[i] = Topic{ID: id, Name: name}
	}

	// Return the receipt.
	return &PublishReceipt{
		MessageID:    id,
		Topics:       topics,
		Value:        value,
		EncodedValue: encodedValue,
		PublishedAt:  publishedAt,
	}, nil
}

func (sd *PostgresDataStore) ReadEncodedMessagesAfterID(
	ctx context.Context,
	querier postgres.Querier,
	messageID MessageID,
	topics []string,
) (chan common.Result[EncodedMessage], error) {
	const sql = `SELECT
			"pubsub_messages"."id",
			"pubsub_topics"."id" AS "topic_id",
			"pubsub_topics"."name" AS "topic_name",
			"pubsub_message_values"."content_type",
			"pubsub_message_values"."bytes",
			"pubsub_messages"."published_at"
		FROM
			"pubsub_messages"
			INNER JOIN "pubsub_message_topics" ON "pubsub_messages"."id" = "pubsub_message_topics"."message_id"
			INNER JOIN "pubsub_topics" ON "pubsub_message_topics"."topic_id" = "pubsub_topics"."id"
			LEFT OUTER JOIN "pubsub_message_values" ON "pubsub_messages"."id" = "pubsub_message_values"."message_id"
		WHERE
			"pubsub_messages"."id" > $1 AND
			"pubsub_topics"."name" = ANY($2)
		ORDER BY "pubsub_messages"."id" ASC;`

	rows, err := querier.Query(ctx, sql, messageID, topics)
	if err != nil {
		return nil, postgres.NormalizeError(err)
	}

	return handleEncodedMessagesRows(rows)
}

func (sd *PostgresDataStore) ReadEncodedMessagesWithID(
	ctx context.Context,
	querier postgres.Querier,
	messageID MessageID,
) (chan common.Result[EncodedMessage], error) {
	const sql = `SELECT
			"pubsub_messages"."id",
			"pubsub_topics"."id" AS "topic_id",
			"pubsub_topics"."name" AS "topic_name",
			"pubsub_message_values"."content_type",
			"pubsub_message_values"."bytes",
			"pubsub_messages"."published_at"
		FROM
			"pubsub_messages"
			INNER JOIN "pubsub_message_topics" ON "pubsub_messages"."id" = "pubsub_message_topics"."message_id"
			INNER JOIN "pubsub_topics" ON "pubsub_message_topics"."topic_id" = "pubsub_topics"."id"
			LEFT OUTER JOIN "pubsub_message_values" ON "pubsub_messages"."id" = "pubsub_message_values"."message_id"
		WHERE
			"pubsub_messages"."id" = $1`

	rows, err := querier.Query(ctx, sql, messageID)
	if err != nil {
		return nil, postgres.NormalizeError(err)
	}

	return handleEncodedMessagesRows(rows)
}

func (sd *PostgresDataStore) ReadEncodedValue(
	ctx context.Context,
	querier postgres.Querier,
	messageID MessageID,
) (*EncodedValue, error) {
	var contentType *string
	var bytes []byte
	const sql = `SELECT content_type, bytes FROM pubsub_message_values WHERE message_id = $1`
	err := querier.QueryRow(ctx, sql, messageID).Scan(&contentType, &bytes)
	if err != nil {
		return nil, postgres.NormalizeError(err)
	}

	return &EncodedValue{ContentType: *contentType, Bytes: bytes}, nil
}

func (sd *PostgresDataStore) Subscribe(
	ctx context.Context,
	querier postgres.Querier,
	topicNames []string,
) (*SubscribeReceipt, error) {
	var newMaxMessageID MessageID
	var topicIDs []TopicID
	const sql = `SELECT max_message_id, topic_ids FROM pubsub_subscribe($1);`
	err := querier.QueryRow(ctx, sql, topicNames).Scan(&newMaxMessageID, &topicIDs)
	if err != nil {
		return nil, postgres.NormalizeError(err)
	}

	topics := make([]Topic, len(topicIDs))
	for i, id := range topicIDs {
		name := topicNames[i]
		topics[i] = Topic{ID: id, Name: name}
	}

	return &SubscribeReceipt{
		MaxMessageID: newMaxMessageID,
		Topics:       topics,
	}, nil
}

func (sd *PostgresDataStore) UnmarshalMessageNotification(payload string) (*MessageNotification, error) {
	var notification MessageNotification
	err := json.Unmarshal([]byte(payload), &notification)
	return &notification, err
}

func (sd *PostgresDataStore) WaitForNotification(ctx context.Context, conn *pgxpool.Conn) (*pgconn.Notification, error) {
	return conn.Conn().WaitForNotification(ctx)
}

func handleEncodedMessagesRows(rows pgx.Rows) (chan common.Result[EncodedMessage], error) {
	// Create a channel.
	ch := make(chan common.Result[EncodedMessage])

	// Start a goroutine to read the rows.
	go func() {
		// Release resources when the goroutine completes.
		defer close(ch)
		defer rows.Close()

		// Read the rows.
		for rows.Next() {
			// Read the next message.
			var messageID MessageID
			var topicID TopicID
			var topicName string
			var contentType *string
			var bytes []byte
			var publishedAt time.Time
			if err := rows.Scan(
				&messageID,
				&topicID,
				&topicName,
				&contentType,
				&bytes,
				&publishedAt,
			); err != nil {
				// Send the error to the channel.
				ch <- common.Result[EncodedMessage]{
					Error: postgres.NormalizeError(err),
				}

				// Terminate the goroutine.
				return
			}

			// Create the topic.
			topic := Topic{ID: topicID, Name: topicName}

			// Create the encoded value.
			var encodedValue *EncodedValue
			if contentType != nil && bytes != nil {
				encodedValue = &EncodedValue{
					ContentType: *contentType,
					Bytes:       bytes,
				}
			}

			// Send the payload to the channel.
			ch <- common.Result[EncodedMessage]{
				Value: EncodedMessage{
					MessageID:    messageID,
					Topic:        topic,
					EncodedValue: encodedValue,
					PublishedAt:  publishedAt,
				},
			}
		}
	}()

	// Return the channel.
	return ch, nil
}

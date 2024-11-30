-- migrate:up
INSERT INTO pubsub_topics (topic)
  SELECT DISTINCT topic FROM pubsub_messages_v1;

INSERT INTO pubsub_messages (id, published_at)
	SELECT id, published_at FROM pubsub_messages_v1;

INSERT INTO pubsub_message_topics (message_id, topic_id)
	SELECT messages.id, topics.id
  FROM pubsub_messages_v1 AS messages
	JOIN pubsub_topics AS topics ON messages.topic = topics.topic;

INSERT INTO pubsub_message_values (message_id, content_type, bytes)
	SELECT messages.id, 'application/json', decode(messages.payload, 'escape') FROM pubsub_messages_v1 AS messages;

DROP TABLE pubsub_messages_v1;

-- migrate:down
CREATE TABLE pubsub_messages_v1(
  id bigserial PRIMARY KEY,
	topic text NOT NULL,
	payload text NOT NULL,
	published_at timestamp NOT NULL DEFAULT NOW(),

  CONSTRAINT topic_length CHECK (char_length(topic) > 0 AND char_length(topic) < 128)
);

INSERT INTO pubsub_messages_v1 (id, topic, payload, published_at)
	SELECT messages.id, message_topics.topic, encode(message_values.bytes, 'escape'), messages.published_at
  FROM pubsub_messages AS messages
	JOIN pubsub_message_values AS message_values ON messages.id = message_values.message_id
	JOIN pubsub_message_topics AS message_topics ON messages.id = message_topics.message_id
	WHERE message_values.content_type = 'application/json';

TRUNCATE pubsub_message_values;

TRUNCATE pubsub_message_topics;

TRUNCATE pubsub_messages;

TRUNCATE pubsub_topics;

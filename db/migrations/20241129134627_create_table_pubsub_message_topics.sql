-- migrate:up
CREATE TABLE pubsub_message_topics(
	"message_id" bigint NOT NULL REFERENCES pubsub_messages(id) ON DELETE CASCADE,
	"topic_id" bigint NOT NULL REFERENCES pubsub_topics(id) ON DELETE RESTRICT,
	PRIMARY KEY ("message_id", "topic_id")
);

-- migrate:down
DROP TABLE pubsub_message_topics;

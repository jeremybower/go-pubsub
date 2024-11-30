-- migrate:up
CREATE TABLE pubsub_message_values(
  "message_id" bigint PRIMARY KEY REFERENCES pubsub_messages(id) ON DELETE CASCADE,
	"content_type" text,
	"bytes" bytea
);

-- migrate:down
DROP TABLE pubsub_message_values;

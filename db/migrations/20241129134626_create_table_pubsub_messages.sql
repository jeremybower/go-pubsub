-- migrate:up
CREATE TABLE pubsub_messages(
  "id" bigserial PRIMARY KEY,
	"published_at" timestamp NOT NULL DEFAULT NOW()
);

CREATE INDEX pubsub_messages_published_at_idx ON pubsub_messages(published_at);

-- migrate:down
DROP INDEX pubsub_messages_published_at_idx;

DROP TABLE pubsub_messages;

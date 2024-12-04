-- migrate:up
CREATE TABLE pubsub_configurations(
	"missed_message_seconds" integer NOT NULL DEFAULT 120
);

INSERT INTO pubsub_configurations DEFAULT VALUES;

-- migrate:down
DROP TABLE pubsub_configurations;

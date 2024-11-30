-- migrate:up
CREATE TABLE pubsub_topics(
	"id" bigserial PRIMARY KEY,
	"topic" text UNIQUE NOT NULL
);

-- migrate:down
DROP TABLE pubsub_topics;

-- migrate:up
CREATE TABLE pubsub_topics(
	"id" bigserial PRIMARY KEY,
	"name" text UNIQUE NOT NULL
);

-- migrate:down
DROP TABLE pubsub_topics;

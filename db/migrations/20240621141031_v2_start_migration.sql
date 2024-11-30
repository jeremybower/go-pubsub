-- migrate:up
DROP FUNCTION pubsub_subscribe;

DROP TRIGGER pubsub_messages_notify_trigger ON pubsub_messages;

DROP FUNCTION pubsub_messages_notify_func;

DROP INDEX pubsub_message_topic_idx;

ALTER TABLE pubsub_messages RENAME TO pubsub_messages_v1;

-- migrate:down
ALTER TABLE pubsub_messages_v1 RENAME TO pubsub_messages;

CREATE INDEX pubsub_message_topic_idx ON pubsub_messages USING btree(topic);

CREATE FUNCTION pubsub_messages_notify_func()
RETURNS TRIGGER
AS $$
DECLARE
	channel text;
  envelope text;
BEGIN
	channel = concat('pubsub_topic:', NEW.topic);
	envelope = json_build_object(
		'id', NEW.id,
		'payload', NEW.payload,
		'published_at', NEW.published_at::timestamptz
	)::text;
	EXECUTE pg_notify(channel, envelope);
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER pubsub_messages_notify_trigger
  AFTER INSERT ON pubsub_messages
  FOR EACH ROW
  EXECUTE FUNCTION pubsub_messages_notify_func();

CREATE FUNCTION pubsub_subscribe(
  topic text
) RETURNS TABLE(
	max_message_id bigint
) AS $$
BEGIN
	-- Lock the table so that no messages are inserted between subscribing
	-- and getting the max message id.
	LOCK TABLE "pubsub_messages" IN EXCLUSIVE MODE;
	EXECUTE format('LISTEN %I', concat('pubsub_topic:', topic));
	RETURN QUERY SELECT coalesce(max(tbl.id), 0) FROM pubsub_messages AS tbl;
END;
$$
LANGUAGE plpgsql;

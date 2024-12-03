-- migrate:up
CREATE FUNCTION pubsub_subscribe(
  _topic_names text[]
) RETURNS TABLE(
	max_message_id bigint,
	topic_ids     bigint[]
) AS $$
DECLARE
	_channel text;
	_max_message_id bigint;
	_topic_name text;
	_topic_id bigint;
	_topic_ids bigint[];
BEGIN
	-- If there are no topics, raise an exception.
	IF _topic_names IS NULL OR array_length(_topic_names, 1) IS NULL THEN
		RAISE EXCEPTION 'pubsub: subscribed with no topics';
	END IF;

	-- Lock the table so that no messages are inserted between subscribing
	-- and getting the max message id.
	LOCK TABLE "pubsub_messages" IN EXCLUSIVE MODE;

	-- Listen for messages for each topic.
	-- This must be done in the same order as the topics array
	-- so that the client can associate the index of each topic name
	-- with the corresponding topic id.
	_topic_ids = array[]::bigint[];
	FOREACH _topic_name IN ARRAY _topic_names LOOP
		-- Insert the topic if it doesn't exist.
		INSERT INTO pubsub_topics ("name")
		VALUES (_topic_name)
		ON CONFLICT DO NOTHING
		RETURNING id INTO _topic_id;

		-- Lookup the topic id by name if it was not inserted.
		IF _topic_id IS NULL THEN
			SELECT id INTO _topic_id
			FROM pubsub_topics AS topics
			WHERE topics.name = _topic_name;

			IF NOT FOUND THEN
				RAISE EXCEPTION 'pubsub: topic "%" not found', _topic_name;
			END IF;
		END IF;

		-- Listen for messages on the channel with the topic id.
		_channel = concat('pubsub_topic:', _topic_id);
		EXECUTE format('LISTEN %I', _channel);

		-- Add the topic id to the array.
		_topic_ids = array_append(_topic_ids, _topic_id);
	END LOOP;

	-- Listen for control messages on the channel.
	EXECUTE 'LISTEN pubsub_control';

	-- Get the max message id for the given topics.
	SELECT coalesce(max(messages.id), 0) INTO _max_message_id
	FROM pubsub_messages AS messages
	JOIN pubsub_message_topics AS message_topics ON messages.id = message_topics.message_id
	WHERE message_topics.topic_id = ANY(_topic_ids);

	-- Return the max message id and topic ids.
	RETURN QUERY SELECT _max_message_id, _topic_ids;
END;
$$
LANGUAGE plpgsql;

-- migrate:down
DROP FUNCTION pubsub_subscribe;

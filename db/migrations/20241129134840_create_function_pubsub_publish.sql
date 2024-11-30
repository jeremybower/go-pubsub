-- migrate:up
CREATE FUNCTION pubsub_publish(
  _topics       text[],
	_content_type text,
  _bytes        bytea
) RETURNS TABLE(
	message_id    bigint,
	topic_ids     bigint[],
	published_at  timestamp
) AS $$
DECLARE
	_channel      text;
	_has_value    boolean;
	_notification text;
	_message_id   bigint;
	_published_at timestamp;
	_topic        text;
	_topic_id     bigint;
	_topic_ids    bigint[];
BEGIN
	-- There must be at least one topic.
	IF _topics IS NULL OR array_length(_topics, 1) IS NULL THEN
		RAISE EXCEPTION 'at least one topic is required';
	END IF;

	-- The content type and bytes must both be null or both non-null.
	IF (_content_type IS NULL AND _bytes IS NOT NULL) OR (_content_type IS NOT NULL AND _bytes IS NULL) THEN
		RAISE EXCEPTION 'value and content type must be provided together';
	END IF;

	-- Insert the message.
	INSERT INTO pubsub_messages DEFAULT VALUES
	RETURNING pubsub_messages.id, pubsub_messages.published_at
	INTO _message_id, _published_at;

	-- Insert the value.
	_has_value = (_content_type IS NOT NULL AND _bytes IS NOT NULL);
	IF _has_value THEN
		INSERT INTO pubsub_message_values ("message_id", "content_type", "bytes")
		VALUES (_message_id, _content_type, _bytes);
	END IF;

	-- Get the id for each topic.
	-- This must be done in the same order as the topics array
	-- so that the client can associate the index of each topic name
	-- with the corresponding topic id.
	_topic_ids = array[]::bigint[];
	FOREACH _topic IN ARRAY _topics LOOP
		-- Attempt to insert the topic.
		INSERT INTO pubsub_topics (topic)
		VALUES (_topic)
		ON CONFLICT DO NOTHING
		RETURNING id INTO _topic_id;

		-- Check if the topic was inserted.
		IF _topic_id IS NULL THEN
			-- Read the topic assuming it was already inserted.
			SELECT id INTO _topic_id
			FROM pubsub_topics AS topics
			WHERE topics.topic = _topic;

			-- If the topic was not found, raise an exception.
			IF NOT FOUND THEN
				RAISE EXCEPTION 'pubsub: topic "%" not found', _topic;
			END IF;
		END IF;

		-- Add the topic id to the array.
		_topic_ids = array_append(_topic_ids, _topic_id);
	END LOOP;

	-- Associate the message with each topic.
	FOREACH _topic_id IN ARRAY _topic_ids LOOP
		INSERT INTO pubsub_message_topics (message_id, topic_id)
		VALUES (_message_id, _topic_id);
	END LOOP;

	-- Create a JSON object for the payload.
	_notification = json_build_object(
		'message_id', _message_id,
		'has_value', _has_value,
		'published_at', _published_at::timestamptz
	)::text;

	-- Notify each topic's channel.
	FOREACH _topic_id IN ARRAY _topic_ids LOOP
		-- Create a channel name using the topic id
		_channel = concat('pubsub_topic:', _topic_id);

		-- Notify the topic's channel with the notification payload.
		EXECUTE pg_notify(_channel, _notification);
	END LOOP;

	-- Return the message id.
	RETURN QUERY SELECT _message_id, _topic_ids, _published_at;
END;
$$
LANGUAGE plpgsql;

-- migrate:down
DROP FUNCTION pubsub_publish;

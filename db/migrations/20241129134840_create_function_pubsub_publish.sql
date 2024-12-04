-- migrate:up
CREATE FUNCTION pubsub_publish(
  _topic_names            text[],
	_content_type           text,
  _bytes                  bytea,
	_published_at           timestamp
) RETURNS TABLE(
	message_id              bigint,
	topic_ids               bigint[],
	published_at            timestamp,
	deleted_message_count   bigint
) AS $$
DECLARE
	_channel                text;
	_deleted_message_count  bigint;
	_has_value              boolean;
	_missed_message_seconds integer;
	_notification           text;
	_message_id             bigint;
	_topic_name             text;
	_topic_id             	bigint;
	_topic_ids              bigint[];
BEGIN
	-- There must be at least one topic.
	IF _topic_names IS NULL OR array_length(_topic_names, 1) IS NULL THEN
		RAISE EXCEPTION 'at least one topic is required';
	END IF;

	-- The content type and bytes must both be null or both non-null.
	IF (_content_type IS NULL AND _bytes IS NOT NULL) OR (_content_type IS NOT NULL AND _bytes IS NULL) THEN
		RAISE EXCEPTION 'value and content type must be provided together';
	END IF;

	-- Read the configuration.
	SELECT missed_message_seconds
	INTO _missed_message_seconds
	FROM pubsub_configurations;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'pubsub: configuration not found';
	END IF;

	-- Insert the message.
	IF _published_at IS NULL THEN
		INSERT INTO pubsub_messages DEFAULT VALUES
		RETURNING pubsub_messages.id, pubsub_messages.published_at
		INTO _message_id, _published_at;
	ELSE
		INSERT INTO pubsub_messages ("published_at")
		VALUES (_published_at)
		RETURNING pubsub_messages.id
		INTO _message_id;
	END IF;

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
	FOREACH _topic_name IN ARRAY _topic_names LOOP
		-- Attempt to insert the topic.
		INSERT INTO pubsub_topics ("name")
		VALUES (_topic_name)
		ON CONFLICT DO NOTHING
		RETURNING id INTO _topic_id;

		-- Check if the topic was inserted.
		IF _topic_id IS NULL THEN
			-- Read the topic assuming it was already inserted.
			SELECT id INTO _topic_id
			FROM pubsub_topics AS topics
			WHERE topics.name = _topic_name;

			-- If the topic was not found, raise an exception.
			IF NOT FOUND THEN
				RAISE EXCEPTION 'pubsub: topic "%" not found', _topic_name;
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

	-- Delete messages older than the missed message threshold.
	DELETE FROM pubsub_messages
	WHERE pubsub_messages.published_at < _published_at - make_interval(secs => _missed_message_seconds);
	GET DIAGNOSTICS _deleted_message_count = ROW_COUNT;

	-- Return the message id.
	RETURN QUERY SELECT _message_id, _topic_ids, _published_at, _deleted_message_count;
END;
$$
LANGUAGE plpgsql;

-- migrate:down
DROP FUNCTION pubsub_publish;

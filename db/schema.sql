SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pubsub_messages_notify_func(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pubsub_messages_notify_func() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
	channel text;
  envelope text;
BEGIN
	channel = concat('pubsub_message:', NEW.topic);
	envelope = json_build_object(
		'message_id', NEW.id,
		'type', NEW.type,
		'payload', NEW.payload,
		'published_at', NEW.published_at::timestamptz
	)::text;
	EXECUTE pg_notify(channel, envelope);
  RETURN NULL;
END;
$$;


--
-- Name: pubsub_subscribe(text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.pubsub_subscribe(topics text[]) RETURNS TABLE(max_message_id bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
	topic text;
BEGIN
	-- Lock the table so that no messages are inserted between subscribing
	-- and getting the max message id.
	LOCK TABLE "pubsub_messages" IN EXCLUSIVE MODE;
	FOREACH topic IN ARRAY topics LOOP
		EXECUTE format('LISTEN %I', concat('pubsub_message:', topic));
	END LOOP;
	RETURN QUERY SELECT coalesce(max(tbl.id), 0) FROM pubsub_messages AS tbl WHERE tbl.topic = ANY(topics);
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: pubsub_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pubsub_messages (
    id bigint NOT NULL,
    topic text NOT NULL,
    type text NOT NULL,
    payload text NOT NULL,
    published_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT topic_length CHECK (((char_length(topic) > 0) AND (char_length(topic) < 128)))
);


--
-- Name: pubsub_messages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pubsub_messages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pubsub_messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pubsub_messages_id_seq OWNED BY public.pubsub_messages.id;


--
-- Name: pubsub_schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pubsub_schema_migrations (
    version character varying(128) NOT NULL
);


--
-- Name: pubsub_messages id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pubsub_messages ALTER COLUMN id SET DEFAULT nextval('public.pubsub_messages_id_seq'::regclass);


--
-- Name: pubsub_messages pubsub_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pubsub_messages
    ADD CONSTRAINT pubsub_messages_pkey PRIMARY KEY (id);


--
-- Name: pubsub_schema_migrations pubsub_schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pubsub_schema_migrations
    ADD CONSTRAINT pubsub_schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: pubsub_message_topic_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pubsub_message_topic_idx ON public.pubsub_messages USING btree (topic);


--
-- Name: pubsub_messages pubsub_messages_notify_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER pubsub_messages_notify_trigger AFTER INSERT ON public.pubsub_messages FOR EACH ROW EXECUTE FUNCTION public.pubsub_messages_notify_func();


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.pubsub_schema_migrations (version) VALUES
    ('20240621141030');

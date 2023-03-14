CREATE TABLE IF NOT EXISTS titles (
	show_id text NOT NULL,
	type text,
	title text,
	director text NULL,
	country text NULL,
	rating text NULL,
	listed_in text NULL,
	description text NULL,
	CONSTRAINT show_id_pkey PRIMARY KEY (show_id)
);
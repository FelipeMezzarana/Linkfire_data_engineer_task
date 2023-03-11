CREATE TABLE IF NOT EXISTS cast_members (
	show_id text NOT NULL REFERENCES titles,
	cast_member text,
	gender text
);
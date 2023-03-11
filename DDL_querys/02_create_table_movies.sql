CREATE TABLE IF NOT EXISTS movies (
	show_id text NOT NULL REFERENCES titles,
	date_added timestamp,
	release_year int,
	movie_length_min int
);
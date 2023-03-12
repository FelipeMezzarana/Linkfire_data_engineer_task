CREATE TABLE IF NOT EXISTS tv_shows (
	show_id text NOT NULL REFERENCES titles,
	date_added timestamp,
	release_year int,
	season_qty int
);
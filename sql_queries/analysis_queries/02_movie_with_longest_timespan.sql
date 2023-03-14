SELECT 
	mv.show_id,
	tl.title,
	mv.date_added,
	mv.release_year,
	date_part('year', mv.date_added) AS year_added,
	date_part('month', mv.date_added) AS month_added,
	date_part('day', mv.date_added) AS day_added,
	date_part('year', mv.date_added) -  mv.release_year AS  year_timespan
FROM
	movies mv
LEFT JOIN 
	titles tl
ON
	mv.show_id = tl.show_id
ORDER BY
	year_timespan DESC,
	month_added,
	day_added
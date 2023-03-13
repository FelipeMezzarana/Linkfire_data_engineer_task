SELECT 
	month_added,
	COUNT(month_added) as qty_titles_added
FROM
	(
	SELECT 
		(CASE 
			when ts.date_added is null
			then date_part('month', mv.date_added)
			else date_part('month', ts.date_added)
			end) as month_added
	FROM 
		titles tl
	LEFT JOIN 
		movies mv 
	ON 
		mv.show_id = tl.show_id
	LEFT JOIN 
		tv_shows ts 
	ON 
		ts.show_id = tl.show_id
	) AS full_titles_table
GROUP BY 
	month_added
HAVING
	month_added is not null
ORDER BY 
	qty_titles_added DESC
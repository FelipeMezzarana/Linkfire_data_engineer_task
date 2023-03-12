SELECT 
	count(*) as invalid_title_type_qty
FROM
	titles
WHERE
	type <> 'TV Show'
AND
	type <> 'Movie'
SELECT 
	COUNT(*)- COUNT(show_id) As show_id_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(show_id))/count(*) As show_id_null_percent,
	COUNT(*)- COUNT(type) As type_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(type))/count(*) As type_null_percent,
	COUNT(*)- COUNT(title) As title_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(title))/count(*) As title_null_percent,
	COUNT(*)- COUNT(director) As director_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(director))/count(*) As director_null_percent,
	COUNT(*)- COUNT(country) As country_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(country))/count(*) As country_null_percent,
	COUNT(*)- COUNT(rating) As rating_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(rating))/count(*) As rating_null_percent,
	COUNT(*)- COUNT(listed_in) As listed_in_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(listed_in))/count(*) As listed_in_null_percent,
	COUNT(*)- COUNT(description) As description_null_qtY,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(description))/count(*) As description_null_percent
FROM 
	titles
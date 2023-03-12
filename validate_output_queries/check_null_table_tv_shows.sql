SELECT 
	COUNT(*)- COUNT(show_id) As show_id_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(show_id))/count(*) As show_id_null_percent,
	COUNT(*)- COUNT(date_added) As date_added_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(date_added))/count(*) As date_added_null_percent,
	COUNT(*)- COUNT(release_year) As release_year_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(release_year))/count(*) As release_year_null_percent,
	COUNT(*)- COUNT(season_qty) As season_qty_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(season_qty))/count(*) As season_qty_null_percent
FROM 
	tv_shows
/*Subquery just to filter the first year  
which will not have an increase since it is the first*/
WITH increase_table AS (
	SELECT 
		date_part('year', date_added) AS year_added,
		count(date_part('year', date_added)) as qty_added_year,
		sum(count(date_part('year', date_added))) 
			OVER (ORDER BY date_part('year', date_added)) as cumulative_total,
		cast(100 AS FLOAT)*
			count(date_part('year', date_added))/
			sum(count(date_part('year', date_added))) OVER (ORDER BY date_part('year', date_added))
			as increase_percent
	FROM
		tv_shows
	WHERE 
		/*Between 2008 and 2012 there were no additions,
		we will only consider only years with consecutive additions*/
		date_added > '2009-1-01' 
	GROUP BY
		year_added 
	ORDER BY 
		increase_percent DESC
	) 
	
SELECT 
	*
FROM 
	increase_table
WHERE 
	year_added <> 2013
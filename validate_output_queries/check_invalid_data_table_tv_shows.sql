SELECT 
  	sum(case 
		when (season_qty <0 or season_qty >30) 
		then 1 else 0 
		end) as invalid_season_qty,
  	sum(case 
		when (release_year <1900 or release_year >date_part('year', CURRENT_DATE)) 
		then 1 else 0 
		end) as invalid_release_year,
  	sum(case 
		when (date_added <'1997-01-01' or date_added > now()) 
		then 1 else 0 
		end) as invalid_date_added,
	  sum(case 
		when (date_part('year', date_added) - release_year) <0
		then 1 else 0 
		end) as invalid_added_release_timespan
FROM
	tv_shows
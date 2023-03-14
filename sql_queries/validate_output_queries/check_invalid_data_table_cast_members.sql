SELECT 
	SUM(CASE 
		when gender = 'UNKNOWN' 
		then 1 else 0 end) as gender_unknown_qty,
	CAST(100 AS FLOAT)*
	SUM(CASE 
		when gender = 'UNKNOWN' 
		then 1 else 0 end)/count(*) as gender_unknown_percent,
	SUM(CASE 
		when gender = 'request_failed' 
		then 1 else 0 end) as gender_request_failed_qty,
	CAST(100 AS FLOAT)*
	SUM(CASE 
		when gender = 'request_failed' 
		then 1 else 0 end)/count(*) as gender_request_failed_percent		
FROM 
	cast_members
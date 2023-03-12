SELECT 
	COUNT(*)- COUNT(show_id) As show_id_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(show_id))/COUNT(DISTINCT show_id) As show_id_null_percent,
	COUNT(*)- COUNT(cast_member) As cast_member_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(cast_member))/COUNT(DISTINCT show_id) As cast_member_null_percent,
	COUNT(*)- COUNT(gender) As gender_null_qty,
	CAST(100 AS FLOAT)*(COUNT(*) - COUNT(gender))/COUNT(DISTINCT show_id) As gender_null_percent
FROM 
	cast_members
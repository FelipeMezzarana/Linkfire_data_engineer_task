SELECT 
	CASE
	WHEN SUBSTRING(cast_member,0,strpos(cast_member,' ')) = '' -- has only the first name
	THEN cast_member 
	ELSE SUBSTRING(cast_member,0,strpos(cast_member,' '))
	END AS first_name,
	COUNT(
		CASE
		WHEN SUBSTRING(cast_member,0,strpos(cast_member,' ')) = ''
		THEN cast_member 
		ELSE SUBSTRING(cast_member,0,strpos(cast_member,' '))
		END) AS first_name_qty
FROM
	cast_members
WHERE 
	cast_member is not null
GROUP BY 
	first_name
ORDER BY
	first_name_qty DESC
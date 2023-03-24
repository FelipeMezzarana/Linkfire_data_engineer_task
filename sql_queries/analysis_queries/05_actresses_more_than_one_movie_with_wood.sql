SELECT 
	cast_member,
	count(cast_member) as qty_movies_with_woody
FROM 
	cast_members 
WHERE 
	show_id IN (SELECT 
			show_id 
		     FROM 
			cast_members
		     WHERE
			cast_member = 'Woody Harrelson'
		    )
AND
	gender = 'female'
GROUP BY 
	cast_member
HAVING 
	count(cast_member) >1
ORDER BY
	qty_movies_with_woody DESC

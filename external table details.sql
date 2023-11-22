
select *
from sys.external_tables AS t
	JOIN sys.external_file_formats AS f ON t.file_format_id=f.file_format_id


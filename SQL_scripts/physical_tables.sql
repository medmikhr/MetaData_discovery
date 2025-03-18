SELECT
concat(t.table_catalog, '_', t.table_schema, '_', t.table_name) as id,
t.table_catalog as database, t.table_schema as schema_name, t.table_name as title,
t.table_type as type,
p.description,
p.size,
p.rows_count,
p.cols_count
from
(
	SELECT table_catalog, table_schema, table_name, table_type
	FROM information_schema.tables as t
	WHERE 1=1
	-- and table_type='BASE TABLE'
	AND table_schema not in ('pg_catalog', 'information_schema')
) as t
left join
(
	select obj_description(s.oid) as description,
	pg_size_pretty(pg_relation_size(s.oid)) as size,
	s.reltuples as rows_count,
	s.relnatts as cols_count,
	s.relname as table_name, n.nspname as table_schema
	from pg_class as s
	left join pg_namespace as n
	ON n.oid = s.relnamespace
) as p
on t.table_schema = p.table_schema and t.table_name = p.table_name

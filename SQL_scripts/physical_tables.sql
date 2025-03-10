SELECT
concat(table_catalog, '_', table_schema, '_', table_name) as id,
table_catalog as database, table_schema as schema_name, table_name as title,
table_name as description,
--pg_relation_size(p.oid) as size,
p.reltuples as size
FROM information_schema.tables as t
left join pg_class as p
on p.relname = t.table_name
WHERE table_type='BASE TABLE'
AND table_schema not in ('pg_catalog', 'information_schema');

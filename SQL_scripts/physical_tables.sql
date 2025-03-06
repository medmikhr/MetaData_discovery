SELECT table_catalog as database, table_schema as schema_name, table_name as title, pg_relation_size(p.oid), p.reltuples as size
FROM information_schema.tables as t
left join pg_class as p
on p.relname = t.table_name
WHERE table_type='BASE TABLE'
AND table_schema not in ('pg_catalog', 'information_schema');

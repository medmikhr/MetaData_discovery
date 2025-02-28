SELECT table_catalog, table_schema, table_name
, pg_relation_size(p.oid), p.reltuples
FROM information_schema.tables as t
left join pg_class as p
on p.relname = t.table_name
WHERE table_type='BASE TABLE'
AND table_schema not in ('pg_catalog', 'information_schema');

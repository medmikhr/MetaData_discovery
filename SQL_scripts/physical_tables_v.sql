select
concat(ns.nspname, '_', cls.relname) as id,
obj_description(cls.oid) as description,
pg_size_pretty(pg_relation_size(cls.oid)) as size,
cls.reltuples as rows_count,
cls.relnatts as cols_count,
case cls.relkind
    WHEN 'r' THEN 'BASE_TABLE'
    WHEN 'v' THEN 'VIEW'
    WHEN 'm' THEN 'MATERIALIZED_VIEW'
end as type,
cls.relname as title, ns.nspname as schema_name
from pg_class as cls
left join pg_namespace as ns
ON ns.oid = cls.relnamespace
where 1=1
and ns.nspname not in ('information_schema')
and ns.nspname not like 'pg_%'
and cls.relkind in ('r', 'v', 'm')
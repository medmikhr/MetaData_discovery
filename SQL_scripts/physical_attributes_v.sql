select
col.column_name as title,
col.description,
col.type,
constr.pk_flag,
constr.fk_flag,
concat(col.table_schema, '_', col.table_name, '.', col.column_name) as id,
concat(col.table_schema, '_', col.table_name) as physical_table
from
(

	select
	    ns.nspname as table_schema,
	    cls.relname as table_name,
	    attr.attname as column_name,
		pgd.description,
	    tp.typname as type
	from pg_catalog.pg_attribute as attr
	join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
	join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
	join pg_catalog.pg_type as tp on tp.oid = attr.atttypid
	left join pg_catalog.pg_description pgd on pgd.objoid = attr.attrelid
											and pgd.objsubid = attr.attnum
	where 1=1
	    and attr.attnum >= 1
		and cls.reltype <> 0
	    and ns.nspname not in ('information_schema')
		and ns.nspname not like 'pg_%'

) as col
left join
(
    select
        column_name,
        table_schema, table_name,
        sum(case when constraint_type = 'PRIMARY KEY' then 1 else 0 end) as pk_flag,
        sum(case when constraint_type = 'FOREIGN KEY' then 1 else 0 end) as fk_flag
    from(
        SELECT
            tc.table_schema as table_schema,
            tc.table_name as table_name,
            kcu.column_name as column_name,
            tc.constraint_type
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE 1=1
            AND tc.table_schema not in ('pg_catalog', 'information_schema')
    ) as t
    group by table_schema, table_name, column_name
) as constr
on col.table_schema = constr.table_schema
	and col.table_name = constr.table_name
	and col.column_name = constr.column_name

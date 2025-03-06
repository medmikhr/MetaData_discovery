select
col.column_name,
col.type,
constr.pk_flag,
constr.fk_flag,
concat(col.table_schema, '_', col.table_name) as table
from
(
	select
		t.table_schema as table_schema,
		t.table_name as table_name,
		c.column_name as column_name,
		c.data_type as type
	from
		information_schema.tables t
	inner join information_schema.columns c on
		t.table_name = c.table_name
		and t.table_schema = c.table_schema
	where 1=1
		and t.table_type= 'BASE TABLE'
		AND t.table_schema not in ('pg_catalog', 'information_schema')
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

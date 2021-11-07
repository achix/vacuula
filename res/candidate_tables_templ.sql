with globals as (
        select 
        current_setting('autovacuum'::text)::boolean AS autovacuum__global,
        current_setting('vacuum_freeze_table_age'::text)::integer AS vacuum_freeze_table_age__global, current_setting('autovacuum_freeze_max_age'::text)::integer AS autovacuum_freeze_max_age__global
)
,
user_tables as (
        select c.oid as coid,c.relkind,nsp.nspname||'.'||c.relname as fullname,
        (select nsp2.nspname||'.'||c2.relname FROM pg_namespace nsp2, pg_class c2 WHERE c2.reltoastrelid=c.oid AND c2.relnamespace=nsp2.oid)  as mainrel,
        pg_relation_size(c.oid), 
        (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_enabled' )::boolean as autovacuum__pertable , 
        (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_table_age' )::integer as vacuum_freeze_table_age__pertable, 
        (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_max_age' )::integer as autovacuum_freeze_max_age__pertable,
        age(c.relfrozenxid), mxid_age(c.relminmxid) 
        from pg_class c, pg_namespace nsp where c.relnamespace=nsp.oid and c.relkind in ('r','m','t') 
        and nsp.nspname NOT IN ('pg_catalog','information_schema')
)
,
candidate_tables as (
        select ut.*,globals.autovacuum__global,globals.vacuum_freeze_table_age__global,globals.autovacuum_freeze_max_age__global 
        from user_tables ut, globals  where 
        coalesce(ut.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and ut.pg_relation_size>=%s
)
,
candidate_tables_effective as (
        select coid,relkind,fullname,mainrel,pg_relation_size,age,mxid_age,
        COALESCE(autovacuum__pertable,autovacuum__global) AS autovacuum__effective,
        vacuum_freeze_table_age__pertable,
        autovacuum_freeze_max_age__pertable,
        COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global) as vacuum_freeze_table_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__effective,
        LEAST(
                COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global),
                0.95 * LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global)
        ) as vacuum_freeze_table_age__effective
        from candidate_tables
)
select * from candidate_tables_effective where age-vacuum_freeze_table_age__effective>%s;

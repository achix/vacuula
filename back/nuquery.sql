with globals as (
        select 
        current_setting('autovacuum'::text)::boolean AS autovacuum,
        current_setting('vacuum_freeze_table_age'::text)::integer AS vacuum_freeze_table_age__global, current_setting('autovacuum_freeze_max_age'::text)::integer AS autovacuum_freeze_max_age__global
)

select c.oid as coid,c.relkind,nsp.nspname||'.'||c.relname as fullname,pg_total_relation_size(c.oid),reloptions.option_name from pg_class c LEFT JOIN LATERAL pg_options_to_table(c.reloptions) reloptions(option_name,option_vale) ON (reloptions.option_name ~ 'vacuum'), pg_namespace nsp where c.relnamespace=nsp.oid and (c.relname = 'test' or c.relname = 'tfreeze' or c.relname ~ ('16662'));

select ('{'||string_agg(tmpq.rec,', ')||'}')::json FROM (select '"'||option_name||'": '||'"'||option_value||'"' as rec from pg_options_to_table('{fillfactor=10,autovacuum_enabled=off}') ) as tmpq;
                       json                        
---------------------------------------------------
 {"fillfactor": "10", "autovacuum_enabled": "off"}

 select c.oid as coid,c.relkind,nsp.nspname||'.'||c.relname as fullname,pg_total_relation_size(c.oid), ( select ('{'||string_agg('"'||option_name||'": '||'"'||option_value||'"',', ')||'}')::json as vacopts from pg_options_to_table(c.reloptions) tmpopts(option_name,option_value) where tmpopts.option_name ~ 'vacuum'  )->'autovacuum_enabled' from pg_class c, pg_namespace nsp where c.relnamespace=nsp.oid and (c.relname = 'test' or c.relname = 'tfreeze' or c.relname ~ ('16662'));

 select c.oid as coid,c.relkind,nsp.nspname||'.'||c.relname as fullname,
 (select nsp2.nspname||'.'||c2.relname FROM pg_namespace nsp2, pg_class c2 WHERE c2.reltoastrelid=c.oid AND c2.relnamespace=nsp2.oid)  as mainrel,
 pg_relation_size(c.oid), 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_enabled' ) as autovacuum__pertable , 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_table_age' ) as vacuum_freeze_table_age__pertable, 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_max_age' ) as autovacuum_freeze_max_age__pertable,
 age(c.relfrozenxid), mxid_age(c.relminmxid) 
 from pg_class c, pg_namespace nsp where c.relnamespace=nsp.oid and c.relkind in ('r','m','t') 
 and nsp.nspname NOT IN ('pg_catalog','information_schema')  
 AND COALESCE((select nsp2.nspname||'.'||c2.relname FROM pg_namespace nsp2, pg_class c2 WHERE c2.reltoastrelid=c.oid AND c2.relnamespace=nsp2.oid)  ,'') !~ 'pg_catalog.*|information_schema.*' ;
 

 
 with globals as (
        select 
        current_setting('autovacuum'::text)::boolean AS autovacuum,
        current_setting('vacuum_freeze_table_age'::text)::integer AS vacuum_freeze_table_age__global, current_setting('autovacuum_freeze_max_age'::text)::integer AS autovacuum_freeze_max_age__global
)
,
tables_candidates as (
select c.oid as coid,c.relkind,nsp.nspname||'.'||c.relname as fullname,
 (select nsp2.nspname||'.'||c2.relname FROM pg_namespace nsp2, pg_class c2 WHERE c2.reltoastrelid=c.oid AND c2.relnamespace=nsp2.oid)  as mainrel,
 pg_relation_size(c.oid), 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_enabled' ) as autovacuum__pertable , 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_table_age' ) as vacuum_freeze_table_age__pertable, 
 (select option_value from pg_options_to_table(c.reloptions) where option_name='autovacuum_freeze_max_age' ) as autovacuum_freeze_max_age__pertable,
 age(c.relfrozenxid), mxid_age(c.relminmxid) 
 from pg_class c, pg_namespace nsp where c.relnamespace=nsp.oid and c.relkind in ('r','m','t') 
 and nsp.nspname NOT IN ('pg_catalog','information_schema')
)
select tc.* from tables_candidates tc, globals  where coalesce(tc.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and tc.pg_relation_size>=0;

select ct.coid,ct.fullname,ct.mainrel,ct.pg_relation_size,autovacuum__pertable,vacuum_freeze_table_age__pertable,autovacuum_freeze_max_age__pertable,age,mxid_age, from candidate_tables ct;



coid                                | 16574
relkind                             | r
fullname                            | public.tfreeze
mainrel                             | 
pg_relation_size                    | 6553600
autovacuum__pertable                | off
vacuum_freeze_table_age__pertable   | 
autovacuum_freeze_max_age__pertable | 
age                                 | 433
mxid_age                            | 0
autovacuum__global                  | t
vacuum_freeze_table_age__global     | 90
autovacuum_freeze_max_age__global   | 100000

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
select ut.*,globals.autovacuum__global,globals.vacuum_freeze_table_age__global,autovacuum_freeze_max_age__global from user_tables ut, globals  where coalesce(ut.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and ut.pg_relation_size>=0
)
select coid,relkind,fullname,mainrel,pg_relation_size,
COALESCE(autovacuum__pertable,autovacuum__global) as autovacuum__effective,
COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global) as vacuum_freeze_table_age__pertable_global,* 
from candidate_tables;



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
        select ut.*,globals.autovacuum__global,globals.vacuum_freeze_table_age__global,globals.autovacuum_freeze_max_age__global from user_tables ut, globals  where coalesce(ut.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and ut.pg_relation_size>=0
)

select coid,relkind,fullname,mainrel,pg_relation_size,
        COALESCE(autovacuum__pertable,autovacuum__global) AS autovacuum__effective,
        COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global) as vacuum_freeze_table_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__effective,
        LEAST(
                COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global),
                0.95 * LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global)
        ) as vacuum_freeze_table_age__effective
        
from candidate_tables ;







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
        select ut.*,globals.autovacuum__global,globals.vacuum_freeze_table_age__global,globals.autovacuum_freeze_max_age__global from user_tables ut, globals  where coalesce(ut.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and ut.pg_relation_size>=0
)
,
candidate_tables_effective as (
        select coid,relkind,fullname,mainrel,pg_relation_size,age,mxid_age,
        COALESCE(autovacuum__pertable,autovacuum__global) AS autovacuum__effective,
        COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global) as vacuum_freeze_table_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__pertable_global,
        LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global) as autovacuum_freeze_max_age__effective,
        LEAST(
                COALESCE(vacuum_freeze_table_age__pertable,vacuum_freeze_table_age__global),
                0.95 * LEAST(autovacuum_freeze_max_age__pertable,autovacuum_freeze_max_age__global)
        ) as vacuum_freeze_table_age__effective
        from candidate_tables
)
select * from candidate_tables_effective ;



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
        coalesce(ut.mainrel,'') !~ 'pg_catalog\.|information_schema\.' and ut.pg_relation_size>=0
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
select * from candidate_tables_effective where age-vacuum_freeze_table_age__effective>-10000000;


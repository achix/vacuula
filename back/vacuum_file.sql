select nsp.nspname||'.'||c.relname as fqname from pg_class c, pg_namespace nsp where
c.relnamespace=nsp.oid and nsp.nspname NOT IN ('information_schema','pg_catalog') AND
c.relkind in ('r','m','t') ORDER BY 1
-- Stable catalog oracle: no OIDs, storage identifiers, database names or statistics.
WITH tables AS (
 SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND left(c.relname,14)='plenora_chain_'
 AND c.relkind IN ('r','p','v','m','f','S')
), objects AS (
 SELECT jsonb_build_array('table',t.relname,t.relkind,t.relpersistence,t.relrowsecurity,t.relforcerowsecurity,pg_get_userbyid(t.relowner),t.relacl,t.reloptions) AS object FROM tables t
 UNION ALL
 SELECT jsonb_build_array('column',t.relname,a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,a.attisdropped,pg_get_expr(d.adbin,d.adrelid),co.collname,cn.nspname,a.attacl)
 FROM tables t JOIN pg_attribute a ON a.attrelid=t.oid
 LEFT JOIN pg_attrdef d ON d.adrelid=t.oid AND d.adnum=a.attnum
 LEFT JOIN pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace WHERE a.attnum>0
 UNION ALL
 SELECT jsonb_build_array('constraint',t.relname,c.conname,c.contype,c.convalidated,c.condeferrable,c.condeferred,pg_get_constraintdef(c.oid)) FROM tables t JOIN pg_constraint c ON c.conrelid=t.oid
 UNION ALL
 SELECT jsonb_build_array('index',t.relname,c.relname,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indisunique,i.indisprimary,c.reloptions,pg_get_userbyid(c.relowner),c.relacl)
 FROM tables t JOIN pg_index i ON i.indrelid=t.oid JOIN pg_class c ON c.oid=i.indexrelid
 UNION ALL
 SELECT jsonb_build_array('trigger',t.relname,g.tgname,g.tgenabled,pg_get_triggerdef(g.oid)) FROM tables t JOIN pg_trigger g ON g.tgrelid=t.oid WHERE NOT g.tgisinternal
 UNION ALL
 SELECT jsonb_build_array('policy',t.relname,p.polname,p.polcmd,p.polpermissive,pg_get_expr(p.polqual,p.polrelid),pg_get_expr(p.polwithcheck,p.polrelid),(SELECT jsonb_agg(CASE WHEN role=0 THEN 'PUBLIC' ELSE pg_get_userbyid(role)::text END ORDER BY role) FROM unnest(p.polroles) role)) FROM tables t JOIN pg_policy p ON p.polrelid=t.oid
 UNION ALL
 SELECT jsonb_build_array('function',p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid),pg_get_userbyid(p.proowner),p.proacl) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind IN ('f','p')
 UNION ALL
 SELECT jsonb_build_array('schema',n.nspname,pg_get_userbyid(n.nspowner),n.nspacl) FROM pg_namespace n WHERE n.nspname='public'
 UNION ALL
 SELECT jsonb_build_array('default_acl',pg_get_userbyid(d.defaclrole),n.nspname,d.defaclobjtype,d.defaclacl) FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE d.defaclnamespace=0 OR n.nspname='public'
) SELECT jsonb_agg(object ORDER BY object::text COLLATE "C")::text FROM objects;

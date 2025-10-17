-- Limpieza de objetos heredados de motores de replicación basados en tablas/funciones 'sym_%'
-- Uso: ejecutar en ambas bases (local y remota) con un rol con permisos adecuados.
-- Advertencia: revisa previamente si tienes funciones/objetos con nombre 'sym_%' propios que debas conservar.

-- 1) Eliminar triggers 'sym_%' en todas las tablas
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           t.tgname AS trigger_name
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE NOT t.tgisinternal
      AND t.tgname ILIKE 'sym_%'
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I CASCADE', r.trigger_name, r.schema_name, r.table_name);
    RAISE NOTICE 'Dropped trigger % on %.%', r.trigger_name, r.schema_name, r.table_name;
  END LOOP;
END$$;

-- 2) Eliminar funciones 'sym_%' (todas las firmas)
DO $$
DECLARE f RECORD;
BEGIN
  FOR f IN
    SELECT n.nspname AS schema_name,
           p.proname AS function_name,
           pg_get_function_identity_arguments(p.oid) AS identity_args
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE p.proname ILIKE 'sym_%'
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I(%s) CASCADE', f.schema_name, f.function_name, f.identity_args);
    RAISE NOTICE 'Dropped function % (%) in schema %', f.function_name, f.identity_args, f.schema_name;
  END LOOP;
END$$;

-- 3) Eliminar tablas 'sym_%'
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE tablename ILIKE 'sym_%'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', t.schemaname, t.tablename);
    RAISE NOTICE 'Dropped table %.%', t.schemaname, t.tablename;
  END LOOP;
END$$;

-- 4) Eliminar secuencias 'sym_%'
DO $$
DECLARE s RECORD;
BEGIN
  FOR s IN
    SELECT sequence_schema, sequence_name
    FROM information_schema.sequences
    WHERE sequence_name ILIKE 'sym_%'
  LOOP
    EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I CASCADE', s.sequence_schema, s.sequence_name);
    RAISE NOTICE 'Dropped sequence % in schema %', s.sequence_name, s.sequence_schema;
  END LOOP;
END$$;

-- 5) Reporte de verificación: listar objetos remanentes con patrón 'sym_%'
WITH objs AS (
  SELECT 'table' AS kind, schemaname AS schema_name, tablename AS obj_name
  FROM pg_tables WHERE tablename ILIKE 'sym_%'
  UNION ALL
  SELECT 'sequence', sequence_schema, sequence_name FROM information_schema.sequences WHERE sequence_name ILIKE 'sym_%'
  UNION ALL
  SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname ILIKE 'sym_%'
)
SELECT * FROM objs ORDER BY kind, schema_name, obj_name;

-- Fin del script
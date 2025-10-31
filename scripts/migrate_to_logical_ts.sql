-- Migración robusta a esquema logical_ts BIGINT + last_op_id UUID
-- Aplica en LOCAL (desktop) y REMOTO (Railway)
-- - Elimina dependencias de updated_at
-- - Agrega logical_ts/last_op_id a todas las tablas de public (excepto meta)
-- - Crea tabla node_state y secuencias
-- - Inicializa valores para filas existentes
-- - Crea triggers para asegurar campos lógicos en escrituras

-- 1) Extensiones para UUID (opcional/según permisos)
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS pgcrypto';
  EXCEPTION WHEN others THEN
    BEGIN
      EXECUTE 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"';
    EXCEPTION WHEN others THEN
      -- Sin extensiones: la app generará UUIDs
      NULL;
    END;
  END;
END $$;

-- 2) Secuencias
CREATE SEQUENCE IF NOT EXISTS migrate_init_logical_ts_seq START 1;
CREATE SEQUENCE IF NOT EXISTS webapp_logical_ts_seq START 1;

-- 3) Tabla de estado del nodo (desktop/offline-first)
CREATE TABLE IF NOT EXISTS node_state (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Sembrar node_id si falta (texto único) evitando referencias de funciones en parseo
DO $$
DECLARE v_node_id TEXT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM node_state WHERE key = 'node_id') THEN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto') THEN
      EXECUTE 'SELECT gen_random_uuid()::text' INTO v_node_id;
    ELSIF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
      EXECUTE 'SELECT uuid_generate_v4()::text' INTO v_node_id;
    ELSE
      EXECUTE 'SELECT md5(random()::text || clock_timestamp()::text)' INTO v_node_id;
    END IF;
    INSERT INTO node_state(key, value) VALUES ('node_id', v_node_id);
  END IF;
END $$;

-- 4) Agregar columnas/índices y limpiar updated_at en todas las tablas de public
DO $$
DECLARE
  rec RECORD;
  has_pgcrypto BOOLEAN := EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto');
  has_uuid_ossp BOOLEAN := EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp');
  uuid_default TEXT;
BEGIN
  IF has_pgcrypto THEN
    uuid_default := 'gen_random_uuid()';
  ELSIF has_uuid_ossp THEN
    uuid_default := 'uuid_generate_v4()';
  ELSE
    uuid_default := NULL; -- sin default, la app asigna
  END IF;

  FOR rec IN
    SELECT c.relname AS tbl
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
  LOOP
    -- Omitir meta
    IF rec.tbl IN ('node_state') THEN
      CONTINUE;
    END IF;

    -- Eliminar trigger legacy de updated_at
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.%I', 'trg_' || rec.tbl || '_set_updated_at', rec.tbl);
    -- Eliminar función por tabla si existiera
    BEGIN
      EXECUTE format('DROP FUNCTION IF EXISTS public.%I_set_updated_at() CASCADE', rec.tbl);
    EXCEPTION WHEN others THEN NULL;
    END;

    -- Eliminar columna updated_at si existe
    EXECUTE format('ALTER TABLE public.%I DROP COLUMN IF EXISTS updated_at', rec.tbl);

    -- Agregar logical_ts
    EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS logical_ts BIGINT NOT NULL DEFAULT 0', rec.tbl);
    -- Agregar last_op_id con default si disponible
    IF uuid_default IS NULL THEN
      EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS last_op_id UUID', rec.tbl);
    ELSE
      EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS last_op_id UUID DEFAULT ' || uuid_default, rec.tbl);
    END IF;

    -- Inicializar filas existentes (ignorando restricciones de triggers que bloquean updates)
    BEGIN
      EXECUTE format('UPDATE public.%I SET logical_ts = CASE WHEN COALESCE(logical_ts, 0) <= 0 THEN nextval(''migrate_init_logical_ts_seq'') ELSE logical_ts END', rec.tbl);
    EXCEPTION WHEN others THEN
      -- Si algún trigger bloquea el UPDATE (p.ej. usuarios dueño), continuar
      NULL;
    END;

    BEGIN
      IF uuid_default IS NULL THEN
        EXECUTE format('UPDATE public.%I SET last_op_id = COALESCE(last_op_id, NULL)', rec.tbl);
      ELSE
        EXECUTE format('UPDATE public.%I SET last_op_id = COALESCE(last_op_id, ' || uuid_default || ')', rec.tbl);
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- Índice por logical_ts
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON public.%I (logical_ts)', 'idx_' || rec.tbl || '_logical_ts', rec.tbl);
  END LOOP;
END $$;

-- 5) Ajustar contador local y secuencia global en base a máximo observado
DO $$
DECLARE
  rec RECORD;
  max_ts BIGINT := 0;
  cur BIGINT := 0;
BEGIN
  FOR rec IN
    SELECT c.relname AS tbl
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
  LOOP
    IF rec.tbl = 'node_state' THEN CONTINUE; END IF;
    EXECUTE format('SELECT COALESCE(MAX(logical_ts), 0) FROM public.%I', rec.tbl) INTO cur;
    max_ts := GREATEST(max_ts, COALESCE(cur, 0));
  END LOOP;

  INSERT INTO node_state(key, value)
  SELECT 'logical_ts_counter', (max_ts + 1)::text
  WHERE NOT EXISTS (SELECT 1 FROM node_state WHERE key = 'logical_ts_counter');

  PERFORM setval('webapp_logical_ts_seq', max_ts + 1, true);
END $$;

-- 6) Función de trigger para asegurar campos lógicos
CREATE OR REPLACE FUNCTION public.ensure_logical_fields()
RETURNS TRIGGER AS $$
DECLARE v_uuid UUID;
BEGIN
  IF NEW.last_op_id IS NULL THEN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto') THEN
      EXECUTE 'SELECT gen_random_uuid()' INTO v_uuid;
      NEW.last_op_id := v_uuid;
    ELSIF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
      EXECUTE 'SELECT uuid_generate_v4()' INTO v_uuid;
      NEW.last_op_id := v_uuid;
    ELSE
      EXECUTE 'SELECT md5(random()::text || clock_timestamp()::text)::uuid' INTO v_uuid;
      NEW.last_op_id := v_uuid;
    END IF;
  END IF;

  IF NEW.logical_ts IS NULL OR NEW.logical_ts <= 0 THEN
    NEW.logical_ts := nextval('webapp_logical_ts_seq');
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 7) Crear triggers en todas las tablas
DO $$
DECLARE
  rec RECORD;
  tname TEXT;
BEGIN
  FOR rec IN
    SELECT c.relname AS tbl
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
  LOOP
    IF rec.tbl IN ('node_state') THEN CONTINUE; END IF;
    tname := 'trg_' || rec.tbl || '_ensure_logical_fields';
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON public.%I', tname, rec.tbl);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.ensure_logical_fields()', tname, rec.tbl);
  END LOOP;
END $$;

-- 8) Eliminar función legacy de updated_at si existe
DROP FUNCTION IF EXISTS public.set_updated_at() CASCADE;

-- Fin de migración
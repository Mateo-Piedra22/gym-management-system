-- SymmetricDS configuración en servidor (Railway)
-- Idempotente: asegura node groups/links, canal 'default', router 'toClients'
-- y crea triggers/trigger_router para todas las tablas públicas.

-- Node groups y links bidireccionales
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'server') THEN
        INSERT INTO sym_node_group (node_group_id) VALUES ('server');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM sym_node_group WHERE node_group_id = 'client') THEN
        INSERT INTO sym_node_group (node_group_id) VALUES ('client');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'server' AND target_node_group_id = 'client'
    ) THEN
        INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id)
        VALUES ('server', 'client');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM sym_node_group_link WHERE source_node_group_id = 'client' AND target_node_group_id = 'server'
    ) THEN
        INSERT INTO sym_node_group_link (source_node_group_id, target_node_group_id)
        VALUES ('client', 'server');
    END IF;
END$$;

-- Canal por defecto
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sym_channel WHERE channel_id = 'default') THEN
        INSERT INTO sym_channel (channel_id, processing_order, queue, enabled, max_batch_size)
        VALUES ('default', 1, 'default', 1, 1000);
    END IF;
END$$;

-- Router hacia clientes (grupo 'client')
DO $$
DECLARE has_sync BOOLEAN;
        has_enabled BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_router' AND column_name = 'sync_config'
    ) INTO has_sync;
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_router' AND column_name = 'enabled'
    ) INTO has_enabled;

    -- Si la tabla sym_router aún no existe (esquema en proceso), salir sin error
    IF to_regclass('public.sym_router') IS NULL THEN
        RAISE NOTICE 'sym_router no existe aún; saltando inserción de router en este momento.';
        RETURN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM sym_router WHERE router_id = 'toClients') THEN
        IF has_sync THEN
            INSERT INTO sym_router (
                router_id, source_node_group_id, target_node_group_id,
                router_type, router_expression, sync_config,
                create_time, last_update_time
            )
            VALUES (
                'toClients', 'server', 'client', 'default', NULL, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
        ELSIF has_enabled THEN
            INSERT INTO sym_router (
                router_id, source_node_group_id, target_node_group_id,
                router_type, router_expression, enabled,
                create_time, last_update_time
            )
            VALUES (
                'toClients', 'server', 'client', 'default', NULL, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
        ELSE
            INSERT INTO sym_router (
                router_id, source_node_group_id, target_node_group_id,
                router_type, router_expression,
                create_time, last_update_time
            )
            VALUES (
                'toClients', 'server', 'client', 'default', NULL,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
        END IF;
    END IF;
END$$;

-- Triggers para todas las tablas públicas (excluye sym_*, pg_*, sync_*)
DO $$
DECLARE r RECORD;
        trig_id TEXT;
        has_use_pk_data BOOLEAN;
        has_use_stream_lobs BOOLEAN;
        has_tr_create_time BOOLEAN;
        has_tr_last_update_time BOOLEAN;
BEGIN
    -- Si el esquema aún no tiene tablas de triggers, salir sin error
    IF to_regclass('public.sym_trigger') IS NULL OR to_regclass('public.sym_trigger_router') IS NULL THEN
        RAISE NOTICE 'sym_trigger/sym_trigger_router no existen aún; saltando creación de triggers en este momento.';
        RETURN;
    END IF;
    -- Detectar columnas opcionales según versión de esquema
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_trigger' AND column_name = 'use_pk_data'
    ) INTO has_use_pk_data;
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_trigger' AND column_name = 'use_stream_lobs'
    ) INTO has_use_stream_lobs;
    -- Detectar timestamps obligatorios en sym_trigger_router
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_trigger_router' AND column_name = 'create_time'
    ) INTO has_tr_create_time;
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sym_trigger_router' AND column_name = 'last_update_time'
    ) INTO has_tr_last_update_time;
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'sym_%'
          AND table_name NOT LIKE 'pg_%'
          AND table_name NOT LIKE 'sync_%'
    LOOP
        trig_id := 'trg_' || r.table_name;

        IF NOT EXISTS (SELECT 1 FROM sym_trigger WHERE trigger_id = trig_id) THEN
            IF has_use_stream_lobs AND has_use_pk_data THEN
                INSERT INTO sym_trigger (
                    trigger_id, source_table_name, channel_id,
                    sync_on_insert, sync_on_update, sync_on_delete,
                    use_stream_lobs, use_pk_data,
                    create_time, last_update_time
                ) VALUES (
                    trig_id, r.table_name, 'default', 1, 1, 1, 0, 1,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            ELSIF has_use_stream_lobs AND NOT has_use_pk_data THEN
                INSERT INTO sym_trigger (
                    trigger_id, source_table_name, channel_id,
                    sync_on_insert, sync_on_update, sync_on_delete,
                    use_stream_lobs,
                    create_time, last_update_time
                ) VALUES (
                    trig_id, r.table_name, 'default', 1, 1, 1, 0,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            ELSIF NOT has_use_stream_lobs AND has_use_pk_data THEN
                INSERT INTO sym_trigger (
                    trigger_id, source_table_name, channel_id,
                    sync_on_insert, sync_on_update, sync_on_delete,
                    use_pk_data,
                    create_time, last_update_time
                ) VALUES (
                    trig_id, r.table_name, 'default', 1, 1, 1, 1,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            ELSE
                INSERT INTO sym_trigger (
                    trigger_id, source_table_name, channel_id,
                    sync_on_insert, sync_on_update, sync_on_delete,
                    create_time, last_update_time
                ) VALUES (
                    trig_id, r.table_name, 'default', 1, 1, 1,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            END IF;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM sym_trigger_router WHERE trigger_id = trig_id AND router_id = 'toClients'
        ) THEN
            IF has_tr_create_time AND has_tr_last_update_time THEN
                INSERT INTO sym_trigger_router (
                    trigger_id, router_id, initial_load_order,
                    create_time, last_update_time
                ) VALUES (
                    trig_id, 'toClients', 1,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            ELSIF has_tr_create_time AND NOT has_tr_last_update_time THEN
                INSERT INTO sym_trigger_router (
                    trigger_id, router_id, initial_load_order,
                    create_time
                ) VALUES (
                    trig_id, 'toClients', 1,
                    CURRENT_TIMESTAMP
                );
            ELSIF NOT has_tr_create_time AND has_tr_last_update_time THEN
                INSERT INTO sym_trigger_router (
                    trigger_id, router_id, initial_load_order,
                    last_update_time
                ) VALUES (
                    trig_id, 'toClients', 1,
                    CURRENT_TIMESTAMP
                );
            ELSE
                INSERT INTO sym_trigger_router (trigger_id, router_id, initial_load_order)
                VALUES (trig_id, 'toClients', 1);
            END IF;
        END IF;
    END LOOP;
END$$;

-- Fin de configuración del servidor Railway
-- Solicita carga inicial para todos los triggers configurados hacia cada cliente
-- Adaptado al esquema actual (SymmetricDS 3.16.x) donde sym_table_reload_request
-- NO tiene columna table_name y usa trigger_id, router_id, channel_id, etc.
-- Ejecutar en el servidor (Railway) una vez que el cliente esté registrado y el esquema sym_* creado.

DO $$
DECLARE n RECORD;
        tr RECORD;
        server_id TEXT;
BEGIN
    -- Resolver el node_id del servidor de forma dinámica
    SELECT node_id INTO server_id FROM sym_node_identity;

    -- Para cada cliente registrado, solicitar recarga por cada trigger configurado
    FOR n IN SELECT node_id FROM sym_node WHERE node_group_id = 'client' LOOP
        FOR tr IN
            SELECT trigger_id, channel_id
            FROM sym_trigger
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM sym_table_reload_request
                WHERE target_node_id = n.node_id
                  AND trigger_id = tr.trigger_id
                  AND router_id = 'toClients'
            ) THEN
                INSERT INTO sym_table_reload_request (
                    target_node_id, source_node_id, trigger_id, router_id,
                    channel_id, create_table, delete_first, reload_select,
                    before_custom_sql, create_time, last_update_time
                ) VALUES (
                    n.node_id, server_id, tr.trigger_id, 'toClients',
                    COALESCE(tr.channel_id, 'default'), 0, 1, NULL,
                    NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                );
            END IF;
        END LOOP;
    END LOOP;
END$$;
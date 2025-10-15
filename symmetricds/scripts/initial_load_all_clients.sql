-- Solicita carga inicial de todas las tablas públicas para todos los clientes registrados
-- Ejecutar en el servidor (Railway) una vez que el cliente esté registrado y el esquema sym_* creado.

DO $$
DECLARE n RECORD;
        t RECORD;
BEGIN
    FOR n IN SELECT node_id FROM sym_node WHERE node_group_id = 'client' LOOP
        FOR t IN
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name NOT LIKE 'sym_%'
              AND table_name NOT LIKE 'pg_%'
              AND table_name NOT LIKE 'sync_%'
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM sym_table_reload_request
                WHERE target_node_id = n.node_id
                  AND table_name = t.table_name
                  AND router_id = 'toClients'
            ) THEN
                INSERT INTO sym_table_reload_request (
                    target_node_id, source_node_id, router_id,
                    channel_id, table_name, create_table,
                    delete_before_reload, reload_select, initial_load_id
                )
                SELECT n.node_id, node_id, 'toClients', 'default', t.table_name, 0, 0, NULL, nextval('sym_sequence')
                FROM sym_node WHERE node_id = 'railway';
            END IF;
        END LOOP;
    END LOOP;
END$$;
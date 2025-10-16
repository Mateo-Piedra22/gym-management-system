import psycopg2


def main():
    conn = psycopg2.connect(host="localhost", port=5432, dbname="gimnasio", user="postgres", password="Matute03")
    conn.autocommit = True
    cur = conn.cursor()
    print("sym_trigger for etiquetas:")
    cur.execute(
        """
        SELECT trigger_id, source_table_name, channel_id, sync_on_insert, sync_on_update, sync_on_delete
        FROM public.sym_trigger
        WHERE source_table_name='etiquetas'
        ORDER BY trigger_id
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(sin filas)")
    else:
        for r in rows:
            print(" | ".join(str(x) for x in r))

    print("\nsym_trigger_router for etiquetas:")
    cur.execute(
        """
        SELECT trigger_id, router_id, initial_load_order
        FROM public.sym_trigger_router
        WHERE trigger_id IN (
            SELECT trigger_id FROM public.sym_trigger WHERE source_table_name='etiquetas'
        )
        ORDER BY trigger_id, router_id
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(sin filas)")
    else:
        for r in rows:
            print(" | ".join(str(x) for x in r))

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
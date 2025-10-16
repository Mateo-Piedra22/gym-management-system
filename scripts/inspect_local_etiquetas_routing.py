import psycopg2


def main():
    conn = psycopg2.connect(host="localhost", port=5432, dbname="gimnasio", user="postgres", password="Matute03")
    conn.autocommit = True
    cur = conn.cursor()
    print("=== sym_data_event join sym_data for 'etiquetas' (últimos 20) ===")
    # Descubrir columnas disponibles en sym_data_event
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='sym_data_event'
        ORDER BY ordinal_position
        """
    )
    cols = [r[0] for r in cur.fetchall()]
    desired = ['data_id', 'batch_id', 'target_node_id', 'router_id', 'route_id', 'channel_id']
    sel = [c for c in desired if c in cols]
    if not sel:
        sel = cols
    selq = ", ".join(f"de.{c}" for c in sel)
    q = f"""
        SELECT {selq}
        FROM public.sym_data_event de
        JOIN public.sym_data d ON d.data_id = de.data_id
        WHERE d.table_name = 'etiquetas'
        ORDER BY de.data_id DESC
        LIMIT 20
    """
    cur.execute(q)
    rows = cur.fetchall()
    if not rows:
        print("(sin eventos de routing para etiquetas)")
    else:
        for r in rows:
            print(r)
        batch_ids = sorted({r[1] for r in rows if r[1] is not None}, reverse=True)
        if batch_ids:
            print("\n=== sym_outgoing_batch para esos batch_id ===")
            # Descubrir columnas disponibles en sym_outgoing_batch
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sym_outgoing_batch'
                ORDER BY ordinal_position
                """
            )
            out_cols = [r[0] for r in cur.fetchall()]
            desired_out = ['batch_id','node_id','channel_id','status','create_time','extract_time','load_time','error_flag']
            out_sel = [c for c in desired_out if c in out_cols]
            if not out_sel:
                out_sel = out_cols
            q2 = f"SELECT {', '.join(out_sel)} FROM public.sym_outgoing_batch WHERE batch_id = ANY(%s) ORDER BY batch_id DESC"
            cur.execute(q2, (batch_ids,))
            for r in cur.fetchall():
                print(r)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
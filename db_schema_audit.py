import argparse
import json
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

# Reutiliza el gestor de conexión del proyecto
from database import DatabaseManager


def collect_schema_info(conn, schema: str = "public", include_defs: bool = False) -> dict:
    cur = conn.cursor()
    info = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema": schema,
        "tables": {},
        "views": {},
        "materialized_views": {},
        "sequences": [],
        "functions": [],
        "procedures": [],
    }

    # Tablas
    cur.execute(
        """
        SELECT c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind = 'r'
        ORDER BY c.relname
        """,
        (schema,),
    )
    tables = [r[0] for r in cur.fetchall()]

    for table in tables:
        table_info = {"columns": [], "constraints": [], "indexes": [], "triggers": [], "policies": []}

        # Columnas
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        for col_name, data_type, is_nullable, column_default in cur.fetchall():
            table_info["columns"].append(
                {
                    "name": col_name,
                    "type": data_type,
                    "nullable": is_nullable == "YES",
                    "default": column_default,
                }
            )

        # Constraints (PK, UK, FK)
        cur.execute(
            """
            SELECT conname, contype, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = (%s || '.' || %s)::regclass
            ORDER BY contype, conname
            """,
            (schema, table),
        )
        for conname, contype, condef in cur.fetchall():
            kind = {"p": "primary_key", "u": "unique", "f": "foreign_key"}.get(contype, contype)
            table_info["constraints"].append({"name": conname, "type": kind, "definition": condef})

        # Índices
        cur.execute(
            """
            SELECT i.relname AS index_name, idx.indisunique, idx.indisprimary, pg_get_indexdef(i.oid)
            FROM pg_index idx
            JOIN pg_class i ON i.oid = idx.indexrelid
            JOIN pg_class t ON t.oid = idx.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = %s AND t.relname = %s
            ORDER BY i.relname
            """,
            (schema, table),
        )
        for name, is_unique, is_pk, idxdef in cur.fetchall():
            table_info["indexes"].append(
                {"name": name, "unique": bool(is_unique), "primary": bool(is_pk), "definition": idxdef}
            )

        # Triggers
        cur.execute(
            """
            SELECT tgname, pg_get_triggerdef(oid)
            FROM pg_trigger
            WHERE tgrelid = (%s || '.' || %s)::regclass AND NOT tgisinternal
            ORDER BY tgname
            """,
            (schema, table),
        )
        for tgname, tgdef in cur.fetchall():
            table_info["triggers"].append({"name": tgname, "definition": tgdef})

        # RLS Policies (si existen)
        try:
            cur.execute(
                """
                SELECT polname, polcmd, polpermissive, pg_get_expr(polqual, polrelid)
                FROM pg_policy p
                JOIN pg_class t ON t.oid = p.polrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s
                ORDER BY polname
                """,
                (schema, table),
            )
            for polname, polcmd, polperm, polqual in cur.fetchall():
                table_info["policies"].append(
                    {
                        "name": polname,
                        "command": polcmd,
                        "permissive": bool(polperm),
                        "qual": polqual,
                    }
                )
        except Exception:
            # Catálogo puede variar por versión, ignorar si no está
            pass

        info["tables"][table] = table_info

    # Views
    cur.execute(
        """
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        (schema,),
    )
    for name, definition in cur.fetchall():
        info["views"][name] = {"definition": definition if include_defs else None}

    # Materialized views (si disponibles)
    try:
        cur.execute(
            """
            SELECT matviewname, definition
            FROM pg_matviews
            WHERE schemaname = %s
            ORDER BY matviewname
            """,
            (schema,),
        )
        for name, definition in cur.fetchall():
            info["materialized_views"][name] = {"definition": definition if include_defs else None}
    except Exception:
        pass

    # Sequences
    cur.execute(
        """
        SELECT sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema = %s
        ORDER BY sequence_name
        """,
        (schema,),
    )
    info["sequences"] = [r[0] for r in cur.fetchall()]

    # Functions & Procedures (sin cuerpo por brevedad)
    cur.execute(
        """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args, p.prokind
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        ORDER BY p.proname
        """,
        (schema,),
    )
    for name, args, kind in cur.fetchall():
        entry = {"name": name, "args": args}
        if kind == "f":
            info["functions"].append(entry)
        elif kind == "p":
            info["procedures"].append(entry)

    return info


def to_markdown(info: dict) -> str:
    lines = []
    lines.append(f"# Esquema PostgreSQL: {info['schema']}")
    lines.append(f"Generado: {info['generated_at']}")
    lines.append("")

    lines.append("## Tablas")
    for tname, tinfo in info["tables"].items():
        lines.append(f"### {tname}")
        lines.append("- Columnas:")
        for c in tinfo["columns"]:
            default = f" default={c['default']}" if c["default"] else ""
            lines.append(f"  - {c['name']} {c['type']} {'NULL' if c['nullable'] else 'NOT NULL'}{default}")
        if tinfo["constraints"]:
            lines.append("- Constraints:")
            for con in tinfo["constraints"]:
                lines.append(f"  - {con['type']}: {con['name']} -> {con['definition']}")
        if tinfo["indexes"]:
            lines.append("- Índices:")
            for idx in tinfo["indexes"]:
                uniq = " UNIQUE" if idx["unique"] else ""
                pk = " PRIMARY" if idx["primary"] else ""
                lines.append(f"  - {idx['name']}{uniq}{pk}: {idx['definition']}")
        if tinfo["triggers"]:
            lines.append("- Triggers:")
            for tg in tinfo["triggers"]:
                lines.append(f"  - {tg['name']}: {tg['definition']}")
        if tinfo["policies"]:
            lines.append("- Policies:")
            for pol in tinfo["policies"]:
                lines.append(f"  - {pol['name']} ({pol['command']}): {pol['qual']}")
        lines.append("")

    if info["views"]:
        lines.append("## Vistas")
        for vname, vdef in info["views"].items():
            lines.append(f"- {vname}")
    if info["materialized_views"]:
        lines.append("## Vistas Materializadas")
        for mname, mdef in info["materialized_views"].items():
            lines.append(f"- {mname}")
    if info["sequences"]:
        lines.append("## Secuencias")
        for s in info["sequences"]:
            lines.append(f"- {s}")
    if info["functions"]:
        lines.append("## Funciones")
        for f in info["functions"]:
            lines.append(f"- {f['name']}({f['args']})")
    if info["procedures"]:
        lines.append("## Procedimientos")
        for p in info["procedures"]:
            lines.append(f"- {p['name']}({p['args']})")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Auditoría de estructura PostgreSQL del proyecto")
    parser.add_argument("--schema", default="public", help="Esquema a inspeccionar (default: public)")
    parser.add_argument("--format", choices=["json", "md"], default="json", help="Formato de salida")
    parser.add_argument("--output", default=os.path.join("logs", "db_schema_report.json"), help="Ruta de salida")
    parser.add_argument("--include-defs", action="store_true", help="Incluir definiciones de vistas/materializadas")
    args = parser.parse_args()

    db = DatabaseManager()
    with db.get_connection_context() as conn:
        info = collect_schema_info(conn, schema=args.schema, include_defs=args.include_defs)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    if args.format == "json":
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        print(f"Esquema exportado en JSON: {args.output}")
    else:
        md = to_markdown(info)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"Esquema exportado en Markdown: {args.output}")


if __name__ == "__main__":
    main()
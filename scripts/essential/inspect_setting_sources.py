#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Inspecciona origen de un parámetro en pg_file_settings (por ejemplo wal_level).
Uso: python scripts/inspect_setting_sources.py wal_level
"""

import json
import os
import sys
from typing import Any, Dict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore

from secure_config import config as secure_config  # type: ignore


def main() -> int:
    param = sys.argv[1] if len(sys.argv) > 1 else 'wal_level'
    out: Dict[str, Any] = {'ok': False, 'param': param}
    cfg = secure_config.get_db_config('local')
    try:
        with psycopg2.connect(**cfg) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT name, setting, applied, error, sourcefile, sourceline
                    FROM pg_file_settings
                    WHERE name = %s
                    ORDER BY applied DESC, sourceline ASC
                    """,
                    (param,),
                )
                rows = cur.fetchall() or []
                out['rows'] = rows
                cur.execute("SHOW %s" % param)
                val = cur.fetchone()[0] if cur.rowcount != -1 else None
                out['show'] = val
        out['ok'] = True
    except Exception as e:
        out['error'] = str(e)
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0 if out.get('ok') else 1


if __name__ == '__main__':
    sys.exit(main())


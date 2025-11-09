import re
from pathlib import Path
from typing import Dict


DEFAULT_THEME = {
    "--primary": "#2b8a3e",
    "--secondary": "#1e3a8a",
    "--accent": "#f59e0b",
    "--bg": "#0f172a",
    "--card": "#111827",
    "--text": "#e5e7eb",
    "--muted": "#9ca3af",
    "--border": "#374151",
}


def _sanitize_var_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_-]+", "-", name)
    return f"--{name}"


def _extract_vars_from_qss(content: str) -> Dict[str, str]:
    vars: Dict[str, str] = {}
    # Buscar patrones tipo: primary: #RRGGBB;  primary-color: rgba(...);
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("//") or line.startswith("/*"):
            continue
        # QSS puede tener comentarios al final
        line = re.sub(r"/\*.*?\*/", "", line).strip()
        m = re.match(r"([A-Za-z0-9_-]+)\s*:\s*([^;]+);?", line)
        if m:
            key = _sanitize_var_name(m.group(1))
            val = m.group(2).strip()
            if re.match(r"^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$", val) or val.startswith("rgb"):
                vars[key] = val
                continue
        # También detectar explicitamente colores dentro de reglas QSS conocidas
        color_match = re.findall(r"([A-Za-z0-9_-]+)\s*[:=]\s*(#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})|rgba?\([^\)]*\))", line)
        for name, val in color_match:
            key = _sanitize_var_name(name)
            vars[key] = val
    return vars


def generate_css_from_qss(qss_path: Path, css_path: Path) -> None:
    try:
        content = qss_path.read_text(encoding="utf-8")
    except Exception:
        content = ""
    vars = _extract_vars_from_qss(content)
    theme = {**DEFAULT_THEME, **vars}
    # Construir CSS con variables y estilos base
    lines = [":root {"]
    for k, v in theme.items():
        lines.append(f"  {k}: {v};")
    lines.append("}")
    lines.append("")
    lines.append("* { box-sizing: border-box; }")
    lines.append("body { margin: 0; font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial, 'Noto Sans', 'Apple Color Emoji', 'Segoe UI Emoji'; background: var(--bg); color: var(--text); }")
    lines.append(".card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }")
    lines.append(".btn { background: var(--primary); color: white; border: none; padding: 10px 16px; border-radius: 10px; cursor: pointer; }")
    lines.append(".btn.secondary { background: var(--secondary); }")
    lines.append(".muted { color: var(--muted); }")
    # Unificar apariencia de inputs/selects en toda la UI
    lines.append("input, select, textarea { background: var(--card); color: var(--text); border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; }")
    lines.append("input::placeholder, textarea::placeholder { color: var(--muted); }")
    lines.append(".control, .input { background: var(--card); color: var(--text); border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; }")
    lines.append("input:focus, select:focus, textarea:focus, .control:focus, .input:focus { outline: none; border-color: var(--primary); }")
    lines.append(".panel-controls label { display: inline-flex; align-items: center; gap: 8px; }")
    try:
        css_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # En entornos serverless el FS puede ser de solo lectura
        pass
    try:
        css_path.write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        # Silenciar si no se puede escribir; la UI usará DEFAULT_THEME
        pass


def read_theme_vars(css_path: Path) -> Dict[str, str]:
    try:
        css = css_path.read_text(encoding="utf-8")
    except Exception:
        return DEFAULT_THEME
    m = re.search(r":root\s*\{([^}]*)\}", css, re.S)
    if not m:
        return DEFAULT_THEME
    body = m.group(1)
    vars: Dict[str, str] = {}
    for line in body.splitlines():
        line = line.strip()
        m2 = re.match(r"(--[a-z0-9_-]+)\s*:\s*([^;]+);", line)
        if m2:
            vars[m2.group(1)] = m2.group(2).strip()
    return {**DEFAULT_THEME, **vars}
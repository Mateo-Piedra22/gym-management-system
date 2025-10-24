import os
import json
import shutil
import subprocess
from typing import Optional, Dict

# Utilidades locales simples (evitamos dependencia cruzada)

def _which(cmd: str) -> Optional[str]:
    try:
        return shutil.which(cmd)
    except Exception:
        return None


def _run(args: list, timeout: int = 60) -> Dict[str, str | int | bool]:
    try:
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "timeout": timeout,
            "text": True,
        }
        if os.name == 'nt':
            try:
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            except Exception:
                pass
        p = subprocess.run(args, **kwargs)
        return {"code": p.returncode, "out": (p.stdout or "").strip(), "err": (p.stderr or "").strip(), "ok": p.returncode == 0}
    except Exception as e:
        return {"code": 1, "out": "", "err": str(e), "ok": False}


# --- TAILSCALE ---

def _install_tailscale() -> Dict[str, str | int | bool]:
    if _which("tailscale"):
        return {"ok": True, "message": "tailscale ya presente"}
    if not _which("winget"):
        return {"ok": False, "message": "winget no disponible para instalar tailscale"}
    args = [
        "winget", "install", "-e", "--id", "Tailscale.Tailscale",
        "--source", "winget", "--silent", "--accept-package-agreements", "--accept-source-agreements",
    ]
    r = _run(args, timeout=1200)
    r["message"] = r.get("out") or r.get("err")
    # Verificar post-instalación
    if not _which("tailscale"):
        return {"ok": False, "message": f"tailscale no detectado tras instalación: {r.get('message','')}"}
    return {"ok": True, "message": r.get("message", "")}


def _tailscale_up(authkey: str, hostname: Optional[str] = None, accept_routes: bool = True, accept_dns: bool = True, advertise_tags: Optional[list] = None, control_url: Optional[str] = None) -> Dict[str, str | int | bool]:
    if not _which("tailscale"):
        inst = _install_tailscale()
        if not inst.get("ok"):
            return inst
    cmd = ["tailscale", "up", f"--authkey={authkey}"]
    if hostname:
        cmd.append(f"--hostname={hostname}")
    if accept_routes:
        cmd.append("--accept-routes=true")
    if accept_dns:
        cmd.append("--accept-dns=true")
    if advertise_tags:
        for t in advertise_tags:
            cmd.append(f"--advertise-tags={t}")
    if control_url:
        cmd.append(f"--login-server={control_url}")
    r = _run(cmd, timeout=120)
    r["message"] = r.get("out") or r.get("err")
    return r


def _tailscale_ip_v4() -> Optional[str]:
    if not _which("tailscale"):
        return None
    r = _run(["tailscale", "ip", "-4"], timeout=10)
    if not r.get("ok"):
        return None
    # Puede devolver múltiples líneas; tomamos la primera 100.x.x.x
    for line in (r.get("out") or "").splitlines():
        line = line.strip()
        if line.startswith("100."):
            return line
    return None


# --- WIREGUARD (opcional) ---

def _install_wireguard() -> Dict[str, str | int | bool]:
    if _which("wireguard") or _which("wg"):
        return {"ok": True, "message": "wireguard ya presente"}
    if not _which("winget"):
        return {"ok": False, "message": "winget no disponible para instalar wireguard"}
    args = [
        "winget", "install", "-e", "--id", "WireGuard.WireGuard",
        "--source", "winget", "--silent", "--accept-package-agreements", "--accept-source-agreements",
    ]
    r = _run(args, timeout=1200)
    r["message"] = r.get("out") or r.get("err")
    if not (_which("wireguard") or _which("wg")):
        return {"ok": False, "message": f"wireguard no detectado tras instalación: {r.get('message','')}"}
    return {"ok": True, "message": r.get("message", "")}


def _wireguard_up_from_config(config_path: str) -> Dict[str, str | int | bool]:
    # En Windows, método preferido: wireguard.exe /installtunnelservice <conf>
    wg_exe = _which("wireguard.exe") or _which("wireguard")
    if not wg_exe:
        inst = _install_wireguard()
        if not inst.get("ok"):
            return inst
        wg_exe = _which("wireguard.exe") or _which("wireguard")
    if not wg_exe:
        return {"ok": False, "message": "wireguard.exe no encontrado"}
    r = _run([wg_exe, "/installtunnelservice", config_path], timeout=60)
    r["message"] = r.get("out") or r.get("err")
    return r


# --- API pública ---

def ensure_vpn_connectivity(cfg: dict, device_id: str) -> Dict[str, object]:
    """
    Garantiza conectividad VPN según cfg['vpn'].
    - provider: 'tailscale' (recomendado) o 'wireguard'
    - Para tailscale: requiere TAILSCALE_AUTHKEY (env o cfg['vpn']['tailscale_auth_key']).
    Devuelve: { ok, provider, joined, ip, message }
    """
    vpn_cfg = cfg.get("vpn") or {}
    provider = (vpn_cfg.get("provider") or "tailscale").lower()
    res: Dict[str, object] = {"ok": False, "provider": provider, "joined": False, "ip": None, "message": ""}

    if provider == "tailscale":
        # Resolver authkey
        authkey = os.getenv("TAILSCALE_AUTHKEY") or vpn_cfg.get("tailscale_auth_key") or ""
        if not authkey:
            res["message"] = "Falta TAILSCALE_AUTHKEY para join automático"
            return res
        # Hostname amigable
        prefix = (vpn_cfg.get("hostname_prefix") or "GymMS").strip()
        hostname = f"{prefix}-{device_id}" if prefix else None
        accept_routes = bool(vpn_cfg.get("accept_routes", True))
        accept_dns = bool(vpn_cfg.get("accept_dns", True))
        advertise_tags = vpn_cfg.get("advertise_tags") or []
        control_url = vpn_cfg.get("control_url") or None

        inst = _install_tailscale()
        if not inst.get("ok"):
            res.update({"ok": False, "message": inst.get("message")})
            return res
        up = _tailscale_up(authkey, hostname=hostname, accept_routes=accept_routes, accept_dns=accept_dns, advertise_tags=advertise_tags, control_url=control_url)
        if not up.get("ok"):
            res.update({"ok": False, "message": up.get("message")})
            return res
        ip = _tailscale_ip_v4()
        if not ip:
            res.update({"ok": False, "message": "No se obtuvo IP Tailscale"})
            return res
        # Exportar útil para otros módulos
        try:
            os.environ["TAILSCALE_IPV4"] = ip
        except Exception:
            pass
        res.update({"ok": True, "joined": True, "ip": ip, "message": "tailscale up OK"})
        return res

    elif provider == "wireguard":
        # Se espera una config embebida base64 o ruta
        conf_b64 = vpn_cfg.get("wireguard_config_b64")
        conf_path = vpn_cfg.get("wireguard_config_path")
        if not (conf_b64 or conf_path):
            res["message"] = "wireguard requiere wireguard_config_b64 o wireguard_config_path"
            return res
        # Materializar config si viene en base64
        if conf_b64 and not conf_path:
            import base64, tempfile
            try:
                data = base64.b64decode(conf_b64)
                tmp_dir = os.path.join(os.path.dirname(__file__), "..", "config", "vpn")
                os.makedirs(tmp_dir, exist_ok=True)
                conf_path = os.path.join(tmp_dir, "gymms.conf")
                with open(conf_path, "wb") as f:
                    f.write(data)
            except Exception as e:
                res["message"] = f"No se pudo materializar config WireGuard: {e}"
                return res
        inst = _install_wireguard()
        if not inst.get("ok"):
            res.update({"ok": False, "message": inst.get("message")})
            return res
        up = _wireguard_up_from_config(conf_path)
        if not up.get("ok"):
            res.update({"ok": False, "message": up.get("message")})
            return res
        # No hay forma simple genérica de obtener IP desde CLI aquí; dejamos OK
        res.update({"ok": True, "joined": True, "ip": None, "message": "wireguard up OK"})
        return res

    else:
        res["message"] = f"Proveedor VPN no soportado: {provider}"
        return res
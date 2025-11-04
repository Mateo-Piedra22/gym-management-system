import csv
import subprocess
import re
import json
import argparse


def _compile_whitelist_patterns(spec: str):
    patterns = []
    names = []
    if not spec:
        return names, patterns
    tokens = [t.strip() for t in spec.split(',') if t.strip()]
    for t in tokens:
        if t.lower().startswith('re:'):
            try:
                patterns.append(re.compile(t[3:], re.IGNORECASE))
            except Exception:
                pass
        else:
            names.append(t)
    return names, patterns


def list_tasks():
    out = subprocess.run(["schtasks", "/query", "/fo", "CSV", "/v"], capture_output=True, text=True)
    if out.returncode != 0:
        return [], {"error": "schtasks query failed", "stderr": out.stderr}
    rows = list(csv.DictReader(out.stdout.splitlines()))
    return rows, {}


def filter_candidates(rows, name_pat: str = '', run_pat: str = '', whitelist_spec: str = ''):
    # Default patterns target GymMS tasks and known wrappers
    default_name = r"(?i)GymMS|Gym\s*Management|GymMSW|GymMS_"
    default_run = r"(?i)gym-management-system|GymMS|run_sync_uploader|run_reconcile|cleanup_data_retention|quick_backup_database|run_backup_scheduled|run_cleanup_scheduled|run_reconcile_scheduled|run_reconcile_remote_to_local_scheduled|run_outbox_flush_once|run_replication_health_check|verify_replication_health|verify_publication_options"
    pat_name = re.compile(name_pat or default_name, re.IGNORECASE)
    pat_run = re.compile(run_pat or default_run, re.IGNORECASE)
    names, pats = _compile_whitelist_patterns(whitelist_spec)
    cand = []
    for r in rows:
        tn = (r.get('TaskName') or '')
        tr = (r.get('Task To Run') or '')
        if pat_name.search(tn) or pat_run.search(tr):
            # Apply whitelist exclusions
            if tn and tn in names:
                continue
            if any(p.search(tn) for p in pats):
                continue
            cand.append(r)
    return cand


def delete_task(name, dry_run: bool = False):
    if dry_run:
        return {
            "name": name,
            "disable_code": None,
            "disable_out": "dry-run",
            "disable_err": "",
            "delete_code": None,
            "delete_out": "dry-run",
            "delete_err": "",
            "deleted": False,
            "dry_run": True,
        }
    disable = subprocess.run(["schtasks", "/Change", "/TN", name, "/Disable"], capture_output=True, text=True)
    delete = subprocess.run(["schtasks", "/Delete", "/TN", name, "/F"], capture_output=True, text=True)
    return {
        "name": name,
        "disable_code": disable.returncode,
        "disable_out": disable.stdout.strip(),
        "disable_err": disable.stderr.strip(),
        "delete_code": delete.returncode,
        "delete_out": delete.stdout.strip(),
        "delete_err": delete.stderr.strip(),
        "deleted": delete.returncode == 0
    }


def kill_processes(dry_run: bool = False):
    # List processes whose CommandLine references our scripts; then stop them
    ps_list_cmd = [
        "powershell", "-NoProfile", "-Command",
        "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_sync_uploader|reconcile_local_remote_once|reconcile_remote_to_local_once|cleanup_data_retention|quick_backup_database|run_backup_scheduled|run_cleanup_scheduled|run_reconcile_scheduled|run_reconcile_remote_to_local_scheduled' } | Select-Object ProcessId, CommandLine | Format-Table -HideTableHeaders"
    ]
    out = subprocess.run(ps_list_cmd, capture_output=True, text=True)
    killed = []
    if out.returncode == 0 and out.stdout:
        lines = [l for l in out.stdout.splitlines() if l.strip()]
        for l in lines:
            parts = l.strip().split(None, 1)
            if parts:
                pid = parts[0]
                if dry_run:
                    killed.append({"pid": pid, "result": "dry-run"})
                else:
                    stop_cmd = [
                        "powershell", "-NoProfile", "-Command",
                        f"try {{ Stop-Process -Id {pid} -Force -PassThru | Out-Null; Write-Output 'Stopped {pid}' }} catch {{ Write-Output 'Error stopping {pid}: ' + $_ }}"
                    ]
                    stop = subprocess.run(stop_cmd, capture_output=True, text=True)
                    killed.append({"pid": pid, "result": (stop.stdout.strip() or stop.stderr.strip())})
    return {
        "list_code": out.returncode,
        "list_out": out.stdout.strip(),
        "list_err": out.stderr.strip(),
        "killed": killed
    }


def main():
    parser = argparse.ArgumentParser(description="Cleanup GymMS scheduled tasks")
    parser.add_argument("--dry-run", action="store_true", help="Preview deletions without changing tasks")
    parser.add_argument("--whitelist", default="", help="Comma-separated task names or re:regex to keep")
    parser.add_argument("--name-pattern", default="", help="Override TaskName regex filter")
    parser.add_argument("--run-pattern", default="", help="Override Task To Run regex filter")
    parser.add_argument("--no-kill", action="store_true", help="Do not kill related running processes")
    args = parser.parse_args()

    rows, err = list_tasks()
    result = {"ok": True, "found": [], "deleted": [], "proc_kill": {}, "dry_run": bool(args.dry_run)}
    if err:
        result["ok"] = False
        result["error"] = err
    cand = filter_candidates(rows, args.name_pattern, args.run_pattern, args.whitelist)
    for r in cand:
        tn = r.get("TaskName", "")
        tr = r.get("Task To Run", "")
        result["found"].append({"TaskName": tn, "TaskToRun": tr})
    for r in result["found"]:
        name = r["TaskName"]
        if not name:
            continue
        del_res = delete_task(name, dry_run=args.dry_run)
        result["deleted"].append(del_res)
    if args.no_kill:
        result["proc_kill"] = {"skipped": True}
    else:
        result["proc_kill"] = kill_processes(dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
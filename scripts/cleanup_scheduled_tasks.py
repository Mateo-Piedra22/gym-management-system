import csv
import subprocess
import re
import json


def list_tasks():
    out = subprocess.run(["schtasks", "/query", "/fo", "CSV", "/v"], capture_output=True, text=True)
    if out.returncode != 0:
        return [], {"error": "schtasks query failed", "stderr": out.stderr}
    rows = list(csv.DictReader(out.stdout.splitlines()))
    return rows, {}


def filter_candidates(rows):
    pat_name = re.compile(r"(?i)GymMS|Gym\s*Management|GymMSW|GymMS_", re.IGNORECASE)
    pat_run = re.compile(r"(?i)gym-management-system|GymMS|run_sync_uploader|run_reconcile|cleanup_data_retention|quick_backup_database|run_backup_scheduled|run_cleanup_scheduled|run_reconcile_scheduled", re.IGNORECASE)
    cand = [r for r in rows if (pat_name.search((r.get('TaskName') or '')) or pat_run.search((r.get('Task To Run') or '')))]
    return cand


def delete_task(name):
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


def kill_processes():
    # List processes whose CommandLine references our scripts; then stop them
    ps_list_cmd = [
        "powershell", "-NoProfile", "-Command",
        "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_sync_uploader|reconcile_local_remote_once|cleanup_data_retention|quick_backup_database|run_backup_scheduled|run_cleanup_scheduled|run_reconcile_scheduled' } | Select-Object ProcessId, CommandLine | Format-Table -HideTableHeaders"
    ]
    out = subprocess.run(ps_list_cmd, capture_output=True, text=True)
    killed = []
    if out.returncode == 0 and out.stdout:
        lines = [l for l in out.stdout.splitlines() if l.strip()]
        for l in lines:
            parts = l.strip().split(None, 1)
            if parts:
                pid = parts[0]
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
    rows, err = list_tasks()
    result = {"ok": True, "found": [], "deleted": [], "proc_kill": {}}
    if err:
        result["ok"] = False
        result["error"] = err
    cand = filter_candidates(rows)
    for r in cand:
        tn = r.get("TaskName", "")
        tr = r.get("Task To Run", "")
        result["found"].append({"TaskName": tn, "TaskToRun": tr})
    for r in result["found"]:
        name = r["TaskName"]
        if not name:
            continue
        del_res = delete_task(name)
        result["deleted"].append(del_res)
    result["proc_kill"] = kill_processes()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
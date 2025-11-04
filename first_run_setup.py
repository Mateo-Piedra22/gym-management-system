#!/usr/bin/env python3
"""
First run setup module for Gym Management System.
This module handles automatic configuration on first run.
"""

import os
import sys
import json
import subprocess
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent

def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = PROJECT_ROOT
    return os.path.join(base_path, relative_path)

def log(message):
    """Simple logging function"""
    print(f"[FIRST RUN SETUP] {message}")

def is_first_run():
    """Check if this is the first run by checking for a setup marker file"""
    marker_path = resource_path("config/first_run_completed.marker")
    return not os.path.exists(marker_path)

def mark_first_run_completed():
    """Create a marker file to indicate first run setup is completed"""
    marker_path = resource_path("config/first_run_completed.marker")
    try:
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w") as f:
            f.write("First run setup completed")
        return True
    except Exception as e:
        log(f"Failed to create marker file: {e}")
        return False

def run_auto_setup():
    """Run the automatic setup script"""
    log("Running automatic setup...")
    
    try:
        # Import and run the auto setup script
        from scripts.auto_setup import main as auto_setup_main
        
        result = auto_setup_main()
        if result == 0:
            log("Automatic setup completed successfully")
            return True
        else:
            log("Automatic setup failed")
            return False
    except Exception as e:
        log(f"Error running automatic setup: {e}")
        return False

def run_admin_setup_vpn_postgres():
    """Run elevated admin setup for VPN/PostgreSQL firewall."""
    try:
        script_path = PROJECT_ROOT / "scripts" / "admin_setup_vpn_postgres.ps1"
        if not os.path.exists(script_path):
            log(f"Admin setup script not found: {script_path}")
            return False
        log("Running admin setup for firewall/VPN…")
        cmd = [
            "powershell.exe", "-NoProfile", "-NonInteractive", "-NoLogo",
            "-ExecutionPolicy", "Bypass", "-File", str(script_path)
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.stdout:
            log(res.stdout.strip())
        if res.returncode == 0:
            log("Admin setup completed successfully")
            return True
        else:
            log(f"Admin setup failed (code {res.returncode}): {res.stderr.strip()}")
            return False
    except Exception as e:
        log(f"Error running admin setup: {e}")
        return False

def first_run_setup():
    """Perform first run setup if needed"""
    if not is_first_run():
        return True
    
    log("First run detected, performing automatic setup...")
    
    # Run automatic setup
    if not run_auto_setup():
        log("Automatic setup failed, continuing anyway...")
    
    # Attempt admin-level setup for firewall/VPN on Windows
    try:
        if sys.platform.startswith("win"):
            run_admin_setup_vpn_postgres()
    except Exception:
        pass
    
    # Mark first run as completed
    if mark_first_run_completed():
        log("First run setup marked as completed")
        return True
    else:
        log("Failed to mark first run as completed")
        return False

if __name__ == "__main__":
    first_run_setup()
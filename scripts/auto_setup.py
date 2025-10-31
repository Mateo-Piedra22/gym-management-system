#!/usr/bin/env python3
"""
Automatic setup script for Gym Management System.
This script handles first-run configuration including:
- Dependency installation
- Database initialization
- Configuration setup
- Bidirectional synchronization setup
"""

import os
import sys
import json
import subprocess
import time
import shutil
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils import resource_path
from device_id import get_device_id

def log(message):
    """Simple logging function"""
    print(f"[AUTO SETUP] {message}")

def _is_headless_env() -> bool:
    try:
        if os.getenv("HEADLESS") == "1":
            return True
        if os.getenv("RAILWAY") or os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("PORT"):
            if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and not os.getenv("WAYLAND_DISPLAY"):
                return True
        if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and not os.getenv("WAYLAND_DISPLAY"):
            return True
    except Exception:
        pass
    return False

def check_and_install_dependencies():
    """Check and install required dependencies"""
    log("Checking dependencies...")
    
    # Read requirements.txt
    requirements_path = PROJECT_ROOT / "requirements.txt"
    if not requirements_path.exists():
        log("Requirements file not found, skipping dependency check")
        return True
    
    try:
        # Try to import key packages to see if they're installed
        import PyQt6
        import psycopg2
        import reportlab
        log("Core dependencies already installed")
        return True
    except ImportError:
        pass
    
    # Install dependencies
    log("Installing dependencies...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_path)
        ])
        log("Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        log(f"Failed to install dependencies: {e}")
        return False
    
    return True

def setup_config_directory():
    """Setup config directory with default files"""
    log("Setting up config directory...")
    
    config_dir = PROJECT_ROOT / "config"
    config_dir.mkdir(exist_ok=True)
    
    # Create default config.json if it doesn't exist
    config_file = config_dir / "config.json"
    if not config_file.exists():
        default_config = {
            "host": "localhost",
            "port": 5432,
            "database": "gimnasio",
            "user": "postgres",
            "sslmode": "prefer",
            "connect_timeout": 10,
            "application_name": "gym_management_system",
            "db_profile": "local",
            "db_local": {
                "host": "localhost",
                "port": 5432,
                "database": "gimnasio",
                "user": "postgres",
                "password": "Matute03",
                "sslmode": "prefer",
                "connect_timeout": 10,
                "application_name": "gym_management_system"
            },
            "db_remote": {
                "host": "shuttle.proxy.rlwy.net",
                "port": 45685,
                "database": "railway",
                "user": "postgres",
                "password": "uDEvhRmVlvaiyRWPPRuSPfVKavIKwmLm",
                "sslmode": "require",
                "connect_timeout": 10,
                "application_name": "gym_management_system"
            },
            "replication": {
                "subscription_name": "gym_sub",
                "publication_name": "gym_pub",
                "remote_can_reach_local": False
            },
            "webapp_base_url": "https://gym-ms-zrk.up.railway.app",
            "public_tunnel": {
                "subdomain": "gym-ms-zrk",
                "enabled": False
            },
            "client_base_url": "",
            "sync_upload_token": "gymms_sync_b3d2a9f6c1e5470ab9d83b7e4c59f12a7d8c3e1f5a9b2c4d6e7f8091a2b3c4d5",
            "webapp_session_secret": "XKxlGoO1rbwZqeKbfSTKJ_EoqqdARkI45w7qta5XsGY",
            "scheduled_tasks": {
                "enabled": True,
                "uploader": {
                    "enabled": True,
                    "interval_minutes": 3
                },
                "reconcile_r2l": {
                    "enabled": True,
                    "interval_minutes": 15
                },
                "reconcile_l2r": {
                    "enabled": True,
                    "time": "02:00"
                },
                "cleanup": {
                    "enabled": True,
                    "time": "03:15"
                },
                "backup": {
                    "enabled": True,
                    "time": "02:30"
                }
            },
            "vpn": {
                "provider": "wireguard",
                "wireguard_config_path": str(PROJECT_ROOT / "config" / "vpn" / "gymms.conf")
            }
        }
        
        with open(config_file, "w", encoding="utf-8") as f:
            json.dump(default_config, f, ensure_ascii=False, indent=2)
        log("Default config.json created")

def setup_device_id():
    """Setup device ID for this installation"""
    log("Setting up device ID...")
    
    device_id = get_device_id()
    log(f"Device ID: {device_id}")

def initialize_database():
    """Initialize the database"""
    log("Initializing database...")
    
    try:
        # Import and run database initialization
        sys.path.insert(0, str(PROJECT_ROOT))
        from initialize_database import main as init_db
        
        result = init_db()
        if result == 0:
            log("Database initialized successfully")
            return True
        else:
            log("Database initialization failed")
            return False
    except Exception as e:
        log(f"Error initializing database: {e}")
        return False

def setup_replication():
    """Setup bidirectional replication"""
    log("Setting up replication...")
    
    try:
        from utils_modules.replication_setup import ensure_bidirectional_replication
        
        # Load config
        config_path = PROJECT_ROOT / "config" / "config.json"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            result = ensure_bidirectional_replication(config)
            if result.get("ok"):
                log("Replication setup completed successfully")
                return True
            else:
                log(f"Replication setup failed: {result.get('error', 'Unknown error')}")
                return False
        else:
            log("Config file not found, skipping replication setup")
            return False
    except Exception as e:
        log(f"Error setting up replication: {e}")
        return False

def ensure_updated_at_triggers():
    """Ensure updated_at columns, indexes and triggers in sync tables"""
    log("Ensuring updated_at triggers...")
    try:
        from scripts.ensure_updated_at_triggers import run as ensure_updated_at
        if _is_headless_env():
            # Remote-only in container/headless environments
            ensure_updated_at(schema='public', tables=None, apply_local=False, apply_remote=True, dry_run=False, all_tables=True)
            log("Updated_at triggers ensured on REMOTE")
        else:
            # Local first
            try:
                ensure_updated_at(schema='public', tables=None, apply_local=True, apply_remote=False, dry_run=False, all_tables=True)
                log("Updated_at triggers ensured on LOCAL")
            except Exception as e_loc:
                log(f"Failed to ensure updated_at on LOCAL: {e_loc}")
            # Remote then
            try:
                ensure_updated_at(schema='public', tables=None, apply_local=False, apply_remote=True, dry_run=False, all_tables=True)
                log("Updated_at triggers ensured on REMOTE")
            except Exception as e_rem:
                log(f"Failed to ensure updated_at on REMOTE: {e_rem}")
        return True
    except Exception as e:
        log(f"Error ensuring updated_at triggers: {e}")
        return False

def setup_scheduled_tasks():
    """Setup scheduled tasks"""
    log("Setting up scheduled tasks...")
    
    try:
        # Copy task scripts to appropriate locations
        scripts_dir = PROJECT_ROOT / "scripts"
        
        # On Windows, we might want to create scheduled tasks
        if os.name == "nt":
            log("Windows detected, setting up scheduled tasks...")
            # This would typically involve creating Windows Scheduled Tasks
            # For now, we'll just ensure the scripts are in place
            pass
        
        log("Scheduled tasks setup completed")
        return True
    except Exception as e:
        log(f"Error setting up scheduled tasks: {e}")
        return False

def main():
    """Main setup function"""
    log("Starting automatic setup...")
    
    # Check and install dependencies
    if not check_and_install_dependencies():
        log("Failed to install dependencies, exiting")
        return 1
    
    # Setup config directory
    setup_config_directory()
    
    # Setup device ID
    setup_device_id()
    
    # Initialize database
    if not initialize_database():
        log("Database initialization failed, continuing anyway...")
    else:
        # Ensure updated_at after local DB init
        ensure_updated_at_triggers()
    
    # Setup replication
    if not setup_replication():
        log("Replication setup failed, continuing anyway...")
    else:
        # Ensure updated_at again to cover remote after replication params
        try:
            ensure_updated_at_triggers()
        except Exception:
            pass
    
    # Setup scheduled tasks
    if not setup_scheduled_tasks():
        log("Scheduled tasks setup failed, continuing anyway...")
    
    log("Automatic setup completed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
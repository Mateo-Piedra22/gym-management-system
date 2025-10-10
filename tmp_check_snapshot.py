from offline_sync_manager import OfflineSyncManager
mgr = OfflineSyncManager()
# No adjunto managers para simular entorno sin WhatsApp listo
snap = mgr.get_connectivity_snapshot()
print('internet_ok=', snap['internet_ok'])
print('db_ok=', snap['db_ok'])
print('whatsapp_ok=', snap['whatsapp_ok'])
print('pending_ops_actionable=', snap['pending_ops'])
print('pending_ops_total=', snap['pending_ops_total'])
print('breakdown=', snap['pending_breakdown'])

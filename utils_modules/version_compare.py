from typing import Literal, Optional

Decision = Literal['local', 'remote', 'conflict']

def compare_versions(
    local_ts: Optional[int],
    local_op_id: Optional[str],
    remote_ts: Optional[int],
    remote_op_id: Optional[str],
    tie_breaker: Literal['webapp', 'local', 'remote', 'none'] = 'webapp'
) -> Decision:
    """
    Compara versiones de registros usando logical_ts y last_op_id.
    Retorna 'local' si local es más nuevo, 'remote' si remoto es más nuevo,
    o 'conflict' si no hay regla clara y tie_breaker='none'.
    """
    l_ts = int(local_ts or 0)
    r_ts = int(remote_ts or 0)
    if l_ts > r_ts:
        return 'local'
    if l_ts < r_ts:
        return 'remote'
    # Empate por logical_ts: comparar lexicográficamente last_op_id si ambos existen
    l_id = str(local_op_id or '')
    r_id = str(remote_op_id or '')
    if l_id and r_id:
        if l_id > r_id:
            return 'local'
        if l_id < r_id:
            return 'remote'
    # Empate total: aplicar regla
    if tie_breaker == 'webapp':
        return 'remote'
    if tie_breaker == 'local':
        return 'local'
    if tie_breaker == 'remote':
        return 'remote'
    return 'conflict'
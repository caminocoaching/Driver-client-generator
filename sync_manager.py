"""sync_manager.py — Reliable Airtable sync layer.

Replaces fire-and-forget background threads with synchronous saves
that verify success, retry on failure, and maintain a visible queue
of pending operations.

Why this exists:
  Previous pattern → threading.Thread(target=save, ...).start()
    - No error checking, no retry, silent data loss overnight
    - Streamlit Cloud recycles containers; in-memory data lost

  New pattern → sync_save(airtable, data, ...)
    - Waits for Airtable confirmation (synchronous)
    - Retries once automatically on failure
    - Queues persistent failures for manual retry
    - Failed saves are visible in sidebar so user knows data is at risk
"""

import streamlit as st
from datetime import datetime
import time

# Session state keys
_QUEUE_KEY = '_airtable_sync_queue'
_LOG_KEY = '_airtable_sync_log'


def _ensure_state():
    """Initialize session state keys if not present.
    Returns True if session_state is accessible."""
    try:
        if _QUEUE_KEY not in st.session_state:
            st.session_state[_QUEUE_KEY] = []
        if _LOG_KEY not in st.session_state:
            st.session_state[_LOG_KEY] = []
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
#  CORE SAVE FUNCTIONS
# ═══════════════════════════════════════════════════════════

def sync_save(airtable, data: dict, record_id: str = None,
              description: str = "", max_retries: int = 1) -> bool:
    """Synchronous Airtable save with verification.

    Replaces the old fire-and-forget pattern:
        threading.Thread(target=..., daemon=True).start()

    Args:
        airtable: AirtableManager instance
        data: Dict of fields to save (Airtable column names as keys)
        record_id: Optional Airtable record ID for direct update
        description: Human-readable label for the sync log
        max_retries: Number of retry attempts (default 1 = try twice total)

    Returns:
        True if save succeeded and was confirmed by Airtable.
        False if all attempts failed (save is queued for manual retry).
    """
    has_state = _ensure_state()
    desc = description or _describe(data)

    if not airtable:
        if has_state:
            _enqueue(data, record_id, desc, "No Airtable connection")
        print(f"[Sync] ❌ FAILED (no Airtable): {desc}")
        return False

    last_error = ""
    for attempt in range(max_retries + 1):
        try:
            result = airtable.upsert_driver(data, record_id=record_id)
            if result:
                if has_state:
                    _log_entry('✅', desc)
                print(f"[Sync] ✅ {desc}")
                return True
            last_error = "upsert_driver returned False"
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                time.sleep(0.5)  # Brief pause before retry

    # All attempts failed — queue for manual retry
    if has_state:
        _enqueue(data, record_id, desc, last_error)
    print(f"[Sync] ❌ FAILED after {max_retries + 1} attempts: {desc} — {last_error}")
    return False


# ═══════════════════════════════════════════════════════════
#  RETRY QUEUE
# ═══════════════════════════════════════════════════════════

def retry_all(airtable) -> tuple:
    """Retry all queued saves.

    Returns:
        (succeeded_count, still_failed_count)
    """
    if not _ensure_state():
        return 0, 0

    queue = list(st.session_state.get(_QUEUE_KEY, []))
    if not queue:
        return 0, 0

    succeeded = 0
    still_failed = []

    for item in queue:
        try:
            result = airtable.upsert_driver(
                item['data'],
                record_id=item.get('record_id')
            )
            if result:
                succeeded += 1
                _log_entry('🔄✅', f"Recovered: {item['description']}")
            else:
                item['retries'] = item.get('retries', 0) + 1
                still_failed.append(item)
        except Exception as e:
            item['retries'] = item.get('retries', 0) + 1
            item['error'] = str(e)
            still_failed.append(item)

    st.session_state[_QUEUE_KEY] = still_failed
    return succeeded, len(still_failed)


# ═══════════════════════════════════════════════════════════
#  STATUS / QUERY
# ═══════════════════════════════════════════════════════════

def pending_count() -> int:
    """Number of saves waiting for retry."""
    try:
        return len(st.session_state.get(_QUEUE_KEY, []))
    except Exception:
        return 0


def get_pending() -> list:
    """Get list of pending (failed) saves."""
    try:
        return list(st.session_state.get(_QUEUE_KEY, []))
    except Exception:
        return []


def get_log(limit: int = 20) -> list:
    """Get recent sync log entries."""
    try:
        return list(st.session_state.get(_LOG_KEY, []))[-limit:]
    except Exception:
        return []


def clear_queue():
    """Discard all pending saves (use with caution)."""
    try:
        st.session_state[_QUEUE_KEY] = []
        _log_entry('🗑️', 'Queue cleared manually')
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════
#  UI COMPONENT
# ═══════════════════════════════════════════════════════════

def render_sync_status(airtable, location=None):
    """Render sync health indicator in sidebar or given container.

    Call this from app.py sidebar to show:
      🟢 All synced  — when no pending saves
      🔴 X saves failed — with retry button
      📋 Recent sync log
    """
    where = location or st.sidebar
    _pending = pending_count()

    if _pending > 0:
        where.error(
            f"🔴 **{_pending} save{'s' if _pending != 1 else ''} failed** — "
            f"data may not persist!"
        )
        c1, c2 = where.columns(2)
        if c1.button("🔄 Retry", key="_sync_retry_btn", use_container_width=True):
            ok, fail = retry_all(airtable)
            if ok:
                st.toast(f"✅ Recovered {ok} save(s)!")
            if fail:
                st.toast(f"❌ {fail} still failing")
            st.rerun()
        if c2.button("🗑️ Clear", key="_sync_clear_btn", use_container_width=True,
                      help="Discard failed saves (data will be lost)"):
            clear_queue()
            st.toast("Queue cleared")
            st.rerun()

        # Show what's pending
        with where.expander(f"📋 Pending saves ({_pending})", expanded=False):
            for item in get_pending():
                ts = item.get('timestamp', '?')
                if 'T' in str(ts):
                    ts = str(ts).split('T')[1][:8]
                st.caption(
                    f"⏳ {item['description']}  \n"
                    f"   Error: {item['error'][:80]}  \n"
                    f"   Retries: {item.get('retries', 0)} · {ts}"
                )
    else:
        where.success("🟢 Airtable synced")

    # Sync log (always available)
    log = get_log(10)
    if log:
        with where.expander("📋 Sync log", expanded=False):
            for entry in reversed(log):
                st.caption(f"{entry['time']} {entry['icon']} {entry['message']}")


# ═══════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════

def _enqueue(data, record_id, description, error):
    """Add a failed save to the retry queue."""
    queue = st.session_state.get(_QUEUE_KEY, [])
    queue.append({
        'data': data,
        'record_id': record_id,
        'description': description,
        'error': str(error)[:200],
        'timestamp': datetime.now().isoformat(),
        'retries': 0,
    })
    st.session_state[_QUEUE_KEY] = queue
    _log_entry('❌', f"FAILED: {description} — {str(error)[:80]}")


def _log_entry(icon, message):
    """Append to sync log."""
    try:
        log = st.session_state.get(_LOG_KEY, [])
        log.append({
            'time': datetime.now().strftime('%H:%M:%S'),
            'icon': icon,
            'message': message,
        })
        st.session_state[_LOG_KEY] = log[-50:]  # Keep last 50
    except Exception:
        pass  # Session state not available (e.g., during import)


def _describe(data):
    """Auto-generate a human-readable description from save data."""
    name = f"{data.get('First Name', '')} {data.get('Last Name', '')}".strip()
    stage = data.get('Stage', '')
    if name and stage:
        return f"{name} → {stage}"
    elif name:
        return f"Save {name}"
    elif 'Email' in data:
        return f"Save {data['Email']}"
    return "Airtable save"

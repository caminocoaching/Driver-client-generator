// Antigravity Driver Connect — Streamlit Sync Content Script
// Runs on the Streamlit app page. Syncs circuit/championship/driver data
// to chrome.storage.local so the FB/IG content scripts can use it.
//
// PRIMARY: Listens for postMessage events from st.components.v1.html()
// FALLBACK: Polls for #ag-active-circuit element in DOM/iframes

(function () {
  'use strict';

  let lastCircuit = '';
  let lastChamp = '';
  let lastDriver = '';
  let lastAiMsg = '';
  let lastAiMessagesJson = '';
  let lastOutreachMode = '';

  // Guard: check if extension context is still valid before using chrome APIs
  function isExtensionValid() {
    try {
      return !!(chrome && chrome.storage && chrome.storage.local);
    } catch (e) {
      return false;
    }
  }

  // ── PRIMARY: postMessage listener ──────────────────────────────────────
  // The Streamlit app sends data via st.components.v1.html() postMessage.
  // This is the most reliable method — no iframe access issues.
  window.addEventListener('message', (event) => {
    if (!event.data || event.data.type !== 'ag_ext_sync') return;
    if (!isExtensionValid()) return;

    const d = event.data;
    console.log('[AG] Received postMessage sync:', {
      circuit: d.circuit,
      champ: d.champ,
      driver: d.driver,
      mode: d.outreachMode
    });

    try {
      // Circuit
      const circuit = (d.circuit || '').trim();
      if (circuit && circuit !== lastCircuit) {
        lastCircuit = circuit;
        chrome.storage.local.set({ ag_circuit: circuit });
        console.log('[AG] ✅ Circuit synced:', circuit);
      }

      // Championship
      const champ = (d.champ || '').trim();
      if (champ && champ !== lastChamp) {
        lastChamp = champ;
        chrome.storage.local.set({ ag_championship: champ });
        console.log('[AG] ✅ Championship synced:', champ);
      }

      // Driver
      const driver = (d.driver || '').trim();
      if (driver && driver !== lastDriver) {
        lastDriver = driver;
        chrome.storage.local.set({ ag_current_driver: driver });
        console.log('[AG] ✅ Driver synced:', driver);
      }

      // Outreach mode
      const outreachMode = (d.outreachMode || '').trim();
      if (outreachMode && outreachMode !== lastOutreachMode) {
        lastOutreachMode = outreachMode;
        chrome.storage.local.set({ ag_outreach_mode: outreachMode });
        console.log('[AG] ✅ Outreach mode synced:', outreachMode);
      }

      // AI message (single)
      const aiMsg = (d.aiMsg || '').trim();
      if (aiMsg && aiMsg !== lastAiMsg) {
        lastAiMsg = aiMsg;
        chrome.storage.local.set({ ag_ai_outreach_msg: aiMsg });
        console.log('[AG] ✅ AI message synced:', aiMsg.substring(0, 50) + '...');
      }

      // AI messages dict (JSON)
      const aiMessagesRaw = (d.aiMessages || '').trim();
      if (aiMessagesRaw && aiMessagesRaw !== lastAiMessagesJson) {
        lastAiMessagesJson = aiMessagesRaw;
        try {
          const aiMessages = JSON.parse(aiMessagesRaw);
          chrome.storage.local.set({ ag_ai_messages: aiMessages });
          console.log('[AG] ✅ AI messages dict synced:', Object.keys(aiMessages).length, 'drivers');
        } catch (e) {
          console.warn('[AG] Could not parse AI messages JSON:', e.message);
        }
      }
    } catch (e) {
      console.warn('[AG] postMessage sync error:', e.message);
    }
  });

  // ── FALLBACK: DOM polling ─────────────────────────────────────────────
  // Searches for #ag-active-circuit in the main document, iframes, and
  // shadow DOM. This is a backup in case postMessage doesn't fire.

  function findElementInFrames(id) {
    // 1. Try the main document first
    const el = document.getElementById(id);
    if (el) return el;

    // 2. Search inside all iframes (Streamlit wraps HTML in iframes)
    const iframes = document.querySelectorAll('iframe');
    for (const iframe of iframes) {
      try {
        const doc = iframe.contentDocument || iframe.contentWindow?.document;
        if (doc) {
          const found = doc.getElementById(id);
          if (found) return found;
        }
      } catch (e) {
        // Cross-origin iframe — skip silently
      }
    }

    // 3. Search inside Shadow DOM roots (newer Streamlit may use shadow DOM)
    const allElements = document.querySelectorAll('*');
    for (const host of allElements) {
      if (host.shadowRoot) {
        const found = host.shadowRoot.getElementById(id);
        if (found) return found;
        const shadowIframes = host.shadowRoot.querySelectorAll('iframe');
        for (const sIframe of shadowIframes) {
          try {
            const sDoc = sIframe.contentDocument || sIframe.contentWindow?.document;
            if (sDoc) {
              const sFound = sDoc.getElementById(id);
              if (sFound) return sFound;
            }
          } catch (e) { /* skip */ }
        }
      }
    }

    return null;
  }

  function syncCircuit() {
    if (!isExtensionValid()) {
      console.warn('[AG] Extension context invalidated — refresh this tab to reconnect');
      clearInterval(pollInterval);
      return;
    }

    try {
      const el = findElementInFrames('ag-active-circuit');
      if (!el) return;  // Element not found — postMessage is primary, this is just fallback

      const circuit = (el.dataset.circuit || el.getAttribute('data-circuit') || '').trim();
      const champ = (el.dataset.champ || el.getAttribute('data-champ') || '').trim();

      if (circuit && circuit !== lastCircuit) {
        lastCircuit = circuit;
        chrome.storage.local.set({ ag_circuit: circuit });
        console.log('[AG] Circuit synced (DOM fallback):', circuit);
      }
      if (champ && champ !== lastChamp) {
        lastChamp = champ;
        chrome.storage.local.set({ ag_championship: champ });
        console.log('[AG] Championship synced (DOM fallback):', champ);
      }

      const driver = (el.dataset.driver || el.getAttribute('data-driver') || '').trim();
      if (driver && driver !== lastDriver) {
        lastDriver = driver;
        chrome.storage.local.set({ ag_current_driver: driver });
        console.log('[AG] Driver synced (DOM fallback):', driver);
      }

      const outreachMode = (el.dataset.outreachMode || el.getAttribute('data-outreach-mode') || '').trim();
      if (outreachMode && outreachMode !== lastOutreachMode) {
        lastOutreachMode = outreachMode;
        chrome.storage.local.set({ ag_outreach_mode: outreachMode });
        console.log('[AG] Outreach mode synced (DOM fallback):', outreachMode);
      }

      const aiMsg = (el.dataset.aiMsg || el.getAttribute('data-ai-msg') || '').trim();
      if (aiMsg && aiMsg !== lastAiMsg) {
        lastAiMsg = aiMsg;
        chrome.storage.local.set({ ag_ai_outreach_msg: aiMsg });
        console.log('[AG] AI message synced (DOM fallback):', aiMsg.substring(0, 50) + '...');
      }

      const aiMessagesRaw = el.dataset.aiMessages || el.getAttribute('data-ai-messages') || '';
      if (aiMessagesRaw && aiMessagesRaw !== lastAiMessagesJson) {
        lastAiMessagesJson = aiMessagesRaw;
        try {
          const aiMessages = JSON.parse(aiMessagesRaw);
          chrome.storage.local.set({ ag_ai_messages: aiMessages });
          console.log('[AG] AI messages dict synced (DOM fallback):', Object.keys(aiMessages).length, 'drivers');
        } catch (e) {
          console.warn('[AG] Could not parse AI messages JSON:', e.message);
        }
      }
    } catch (e) {
      console.warn('[AG] DOM sync error (extension may have been reloaded):', e.message);
    }
  }

  // Poll every 2 seconds as fallback (postMessage is primary)
  const pollInterval = setInterval(syncCircuit, 2000);

  // Also run once on load
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(syncCircuit, 3000);
    setTimeout(syncCircuit, 6000);
  } else {
    window.addEventListener('load', () => {
      setTimeout(syncCircuit, 3000);
      setTimeout(syncCircuit, 6000);
    });
  }

  // ── Save handler: receive params from background.js and inject into URL ──
  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.type === 'ag_inject_params') {
      try {
        const currentUrl = new URL(window.location.href);
        const newParams = new URLSearchParams(message.params);

        for (const [key, value] of newParams) {
          currentUrl.searchParams.set(key, value);
        }

        console.log('[AG] Injecting save params into Streamlit URL:', message.params.substring(0, 80));
        window.location.href = currentUrl.toString();

        sendResponse({ success: true });
      } catch (e) {
        console.error('[AG] Failed to inject params:', e.message);
        sendResponse({ success: false, error: e.message });
      }
      return true;
    }
  });
})();

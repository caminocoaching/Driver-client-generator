// Antigravity Driver Connect — Streamlit Sync Content Script
// Runs on the Streamlit app page. Watches for the hidden #ag-active-circuit
// element and syncs its value to chrome.storage.local so the FB/IG content
// scripts can pre-populate the circuit input in the outreach panel.
//
// IMPORTANT: Streamlit renders st.markdown(unsafe_allow_html=True) inside
// iframes, so we must search inside ALL same-origin iframes on the page.

(function () {
  'use strict';

  let lastCircuit = '';
  let lastChamp = '';
  let lastDriver = '';
  let lastAiMsg = '';
  let lastAiMessagesJson = '';

  // Guard: check if extension context is still valid before using chrome APIs
  function isExtensionValid() {
    try {
      return !!(chrome && chrome.storage && chrome.storage.local);
    } catch (e) {
      return false;
    }
  }

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
        // Also check iframes inside shadow roots
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
    // If extension was reloaded, stop polling — user needs to refresh page
    if (!isExtensionValid()) {
      console.warn('[AG] Extension context invalidated — refresh this tab to reconnect');
      clearInterval(pollInterval);
      return;
    }

    try {
      const el = findElementInFrames('ag-active-circuit');
      if (!el) return;

      const circuit = (el.dataset.circuit || el.getAttribute('data-circuit') || '').trim();
      const champ = (el.dataset.champ || el.getAttribute('data-champ') || '').trim();

      // Only write to storage when the value actually changes
      if (circuit && circuit !== lastCircuit) {
        lastCircuit = circuit;
        chrome.storage.local.set({ ag_circuit: circuit });
        console.log('[AG] Circuit synced to extension:', circuit);
      }
      if (champ && champ !== lastChamp) {
        lastChamp = champ;
        chrome.storage.local.set({ ag_championship: champ });
        console.log('[AG] Championship synced to extension:', champ);
      }

      const driver = (el.dataset.driver || el.getAttribute('data-driver') || '').trim();
      if (driver && driver !== lastDriver) {
        lastDriver = driver;
        chrome.storage.local.set({ ag_current_driver: driver });
        console.log('[AG] Driver synced to extension:', driver);
      }

      // Sync AI-generated outreach message (single — last card rendered)
      const aiMsg = (el.dataset.aiMsg || el.getAttribute('data-ai-msg') || '').trim();
      if (aiMsg && aiMsg !== lastAiMsg) {
        lastAiMsg = aiMsg;
        chrome.storage.local.set({ ag_ai_outreach_msg: aiMsg });
        console.log('[AG] AI outreach message synced to extension:', aiMsg.substring(0, 50) + '...');
      }

      // Sync per-driver AI message dictionary (JSON)
      const aiMessagesRaw = el.dataset.aiMessages || el.getAttribute('data-ai-messages') || '';
      if (aiMessagesRaw && aiMessagesRaw !== lastAiMessagesJson) {
        lastAiMessagesJson = aiMessagesRaw;
        try {
          const aiMessages = JSON.parse(aiMessagesRaw);
          chrome.storage.local.set({ ag_ai_messages: aiMessages });
          console.log('[AG] AI messages dict synced:', Object.keys(aiMessages).length, 'drivers');
        } catch (e) {
          console.warn('[AG] Could not parse AI messages JSON:', e.message);
        }
      }
    } catch (e) {
      console.warn('[AG] Sync error (extension may have been reloaded):', e.message);
    }
  }

  // Streamlit re-renders the DOM frequently via WebSocket updates,
  // so we poll rather than relying on a single MutationObserver callback.
  const pollInterval = setInterval(syncCircuit, 2000);

  // Also run once on load (with extra delay for Streamlit to render iframes)
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(syncCircuit, 3000);
    setTimeout(syncCircuit, 6000);  // Extra retry for slow loads
  } else {
    window.addEventListener('load', () => {
      setTimeout(syncCircuit, 3000);
      setTimeout(syncCircuit, 6000);
    });
  }

  // ── Save handler: receive params from background.js and inject into URL ──
  // This triggers a Streamlit rerun using the EXISTING warm session (instant)
  // instead of opening a cold new tab (15-45 second delay).
  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (message.type === 'ag_inject_params') {
      try {
        const currentUrl = new URL(window.location.href);
        const newParams = new URLSearchParams(message.params);

        // Append new params to current URL
        for (const [key, value] of newParams) {
          currentUrl.searchParams.set(key, value);
        }

        console.log('[AG] Injecting save params into Streamlit URL:', message.params.substring(0, 80));

        // Navigate — triggers Streamlit rerun with the save handler
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

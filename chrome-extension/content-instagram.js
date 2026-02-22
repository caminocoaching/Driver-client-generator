// Antigravity Driver Connect — Instagram DMs Content Script
// Shows a slide-out contact card panel with reply templates directly on the
// Instagram Direct page. Pick a reply, it gets pasted into the message input.

(function () {
  'use strict';

  const APP_URL = 'https://driver-client-generator.streamlit.app';
  const BUTTON_ID = 'antigravity-driver-btn';
  const PANEL_ID = 'antigravity-driver-panel';

  // Robust message sender — retries once if service worker was asleep
  function sendRuntimeMessage(msg, callback) {
    // Guard: after extension reload, old content scripts lose their context
    if (!chrome?.runtime?.id) {
      console.warn('[AG] Extension context invalidated — refresh this tab');
      showToast('⚠️ Extension reloaded — refresh this page', '#ef4444');
      return;
    }
    try {
      chrome.runtime.sendMessage(msg, (response) => {
        if (chrome.runtime.lastError) {
          console.warn('[AG] Runtime message failed, retrying:', chrome.runtime.lastError.message);
          setTimeout(() => {
            if (!chrome?.runtime?.id) return;
            try {
              chrome.runtime.sendMessage(msg, (r2) => {
                if (chrome.runtime.lastError) {
                  console.error('[AG] Retry also failed:', chrome.runtime.lastError.message);
                }
                if (callback) callback(r2);
              });
            } catch (e) {
              console.error('[AG] Retry exception:', e);
            }
          }, 300);
        } else {
          if (callback) callback(response);
        }
      });
    } catch (e) {
      console.warn('[AG] sendMessage exception:', e.message);
    }
  }
  let currentName = null;
  let pipelineDriverName = null;  // Real name from pipeline app (via #ag_driver= hash or chrome.storage)
  let panelOpen = false;
  let observer = null;
  let autoSavedUrl = null;  // Track which URL we already auto-saved

  // Check URL hash for driver name passed from pipeline app search buttons
  function checkHashForDriverName() {
    const hash = window.location.hash;
    if (hash && hash.includes('ag_driver=')) {
      const name = decodeURIComponent(hash.split('ag_driver=')[1].replace(/\+/g, ' '));
      if (name && name.length > 1) {
        pipelineDriverName = name;
        chrome.storage.local.set({ agd_current_driver: name });
        // Clean the hash so it doesn't persist on navigation
        history.replaceState(null, '', window.location.href.split('#')[0]);
      }
    }
  }
  checkHashForDriverName();

  // NOTE: We intentionally do NOT load agd_current_driver from chrome.storage
  // on init. The hash (#ag_driver=) is the only reliable source for pipeline
  // driver name. Loading stale storage data caused wrong names when navigating
  // between profiles.

  // Listen for driver name changes from the Streamlit app
  chrome.storage.onChanged.addListener((changes) => {
    if (changes.agd_current_driver && changes.agd_current_driver.newValue) {
      pipelineDriverName = changes.agd_current_driver.newValue;
    }
  });

  // ── Reply Templates (synced from ui_components.py) ──────────────────────
  const REPLY_TEMPLATES = {
    // --- COLD OUTREACH (randomly picks variation when clicked) ---
    "Cold Outreach": `__RANDOM_OUTREACH__`,

    // --- COLD OUTREACH RESPONSES / REPLIES ---
    "Great Work (Reply)": `Thanks for the reply {name},\nThat's Great work well done!\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    "Productive (Reply)": `Thanks for the reply {name},\nSounds like you had a productive weekend.\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    "Tough Weekend (Reply)": `Thanks for the reply {name}, it Sounds like you had a tough weekend.\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    // --- SEND LINKS ---
    "Send Link (Yes)": `Superb, {name} Here is the link to The Post-Race Weekend Performance Score\nhttps://improve-driver.scoreapp.com\n\nThis short review zeroes in on where you're losing lap time, where any gaps are showing up and how to fill them 🚀\n\nAt the bottom of the results page is some free training on how to fill those gaps 👍🏻`,

    "Send Blueprint Link": `OK {name} here you go, instant access to the Podium Contenders Blueprint\nhttps://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/\n\n📚 What you'll learn:\n✓ Day 1: The 7 biggest mistakes costing you lap times\n✓ Day 2: The 5-pillar system for accessing flow state on command\n✓ Day 3: Your race weekend mental preparation protocol\n\nComplete all 3 days, and you'll unlock a free strategy call where we'll create your personalised performance roadmap for 2026.\nSee you inside! 🏁\nCraig`,

    "Offer Free Training": `Hey {name}, Great to see you will be lining up on the grid this season\nWe have some pre-season free training that many drivers are using to ensure they are on point from the first round this season.\nWant me to send it over?`,

    // --- REVIEW DONE → NEXT STEP ---
    "Follow-Up (Review Done → Blueprint)": `Hey {name},\nSaw you completed the Race Weekend Review — nice one. Most drivers never even get that far. They just keep doing the same thing and wondering why nothing changes.\nYour results actually flagged a couple of areas that the Free Training covers in detail, specifically how the top drivers manage those exact patterns you scored on.\n\nWant me to send you the link?\nCraig`,

    // --- REVIEW STALLED ---
    "Stalled: Review Started": `Hey {name}, I see you started the Race Weekend Review but didn't get to the results page - that's where the good stuff is.\n\nYour results break down exactly where you're losing time and why - most drivers tell me they had no idea THAT was the thing holding them back.\n\nPlus the results page unlocks access to free training that covers how to fix those exact gaps before the next round.\n\nTakes about 3 minutes to finish - want me to resend the link?`,

    // --- FOLLOW-UPS ---
    "Follow-Up (Link Sent Check)": `Hi {name} did you manage to take a look at the race weekend review I sent over?`,

    "Follow-Up (Review 2 Days) V1": `Hey {name}\nJust checking in - did you get a chance to go through the post-race review I sent over?\nTakes about 5 minutes and shows exactly where the gains are hiding for you.\nLet me know if the link didn't work or if you had any issues with it 👍`,

    "Follow-Up (Review 2 Days) V2": `{name} - wanted to circle back on the race weekend assessment\nMost drivers who complete it say the same thing: 'I didn't realise THAT was what was holding me back'\nIf you're still interested, the link's below. If not, no worries - good luck with the rest of the season 👍`,

    // --- STALLED TRAINING NUDGES ---
    "Stalled: Signed In": `Hi {name} I see you signed into the free training but didn't go much further was everything ok with the link and the platform for you?`,

    "Stalled: Day 1 Only": `Hey, {name}, Great work on completing the first day of the free training how was it for you?`,

    "Stalled: Day 2 Only": `Hey {name}, I see you completed the first 2 days of the Free Training but missed the third, is everything ok with the link and platform for you?`,

    "Stalled: Day 3 Only": `Hey {name}, I see you completed the Free Training but haven't booked your free strategy call yet.\nI have a few slots open this week if you want to dial in your plan for the season?`,

    // --- RESCUE DMs ---
    "Rescue: Day 1 Nudge": `Hey {name}! 👋\n\nNoticed you signed up for the Podium Contenders Blueprint but haven't done Day 1 yet.\n\nThe 7 Biggest Mistakes assessment only takes 20 mins and drivers are telling me it's been a game-changer for understanding where they're leaving time on track.\n\nYour link's still active - want me to resend it?\n\nLet me know if you have any questions!`,

    "Rescue: Day 2 Nudge": `Hey {name}!\n\nLoved seeing your Day 1 results - some really interesting patterns there.\n\nDay 2's 5-Pillar Assessment is where it all comes together though. It shows you exactly which areas will give you the biggest gains.\n\nTakes about 15 mins - you ready to dive in?\n\nHere's your link: https://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/`,

    "Rescue: Book Strategy Call": `Hey {name}!\n\nYou've done Day 1 AND Day 2 - that's awesome! You're clearly serious about this.\n\nThe next step is a Strategy Call where we look at your results together and figure out the best path forward for you.\n\nNo pressure, no hard sell - just a real conversation about your racing goals.\n\nI've got some spots open - shall I send the booking link?`
  };

  const TEMPLATE_GROUPS = {
    "🏁 COLD OUTREACH": ["Cold Outreach", "Offer Free Training"],
    "REPLIES": ["Great Work (Reply)", "Productive (Reply)", "Tough Weekend (Reply)"],
    "SEND LINKS": ["Send Link (Yes)", "Send Blueprint Link"],
    "FOLLOW-UPS": ["Follow-Up (Review Done → Blueprint)", "Follow-Up (Link Sent Check)", "Follow-Up (Review 2 Days) V1", "Follow-Up (Review 2 Days) V2"],
    "STALLED NUDGES": ["Stalled: Review Started", "Stalled: Signed In", "Stalled: Day 1 Only", "Stalled: Day 2 Only", "Stalled: Day 3 Only"],
    "RESCUE DMs": ["Rescue: Day 1 Nudge", "Rescue: Day 2 Nudge", "Rescue: Book Strategy Call"]
  };

  // Map each template to the pipeline stage it should advance the driver to
  const TEMPLATE_STAGE_MAP = {
    "Cold Outreach": "Messaged",
    "Great Work (Reply)": "Replied",
    "Productive (Reply)": "Replied",
    "Tough Weekend (Reply)": "Replied",
    "Send Link (Yes)": "Link Sent",
    "Send Blueprint Link": "Blueprint Link Sent",
    "Offer Free Training": "Messaged",
    "Follow-Up (Review Done → Blueprint)": null,
    "Follow-Up (Link Sent Check)": null,
    "Follow-Up (Review 2 Days) V1": null,
    "Follow-Up (Review 2 Days) V2": null,
    "Stalled: Review Started": null,
    "Stalled: Signed In": null,
    "Stalled: Day 1 Only": null,
    "Stalled: Day 2 Only": null,
    "Stalled: Day 3 Only": null,
    "Rescue: Day 1 Nudge": null,
    "Rescue: Day 2 Nudge": null,
    "Rescue: Book Strategy Call": null,
  };

  // Templates that should create the driver in the database if they don't exist
  const CREATE_DRIVER_TEMPLATES = ["Cold Outreach", "Offer Free Training"];

  // Race weekend outreach — uses saveOutreach (no stage change, panel stays open)
  // Only Cold Outreach is race-weekend related and needs circuit
  const RACE_OUTREACH_TEMPLATES = ["Cold Outreach"];

  // Cold outreach message variations — one is picked randomly per click
  const COLD_OUTREACH_VARIATIONS = [
    `Hey {name}, I see you were out at {circuit} at the weekend. How was it for you?`,
    `Hi {name}, I see you were out at {circuit} at the weekend. How did it go?`,
    `Hello {name}, I see you were out at {circuit} at the weekend. How was the weekend?`,
    `Hey {name}, I noticed you were at {circuit} at the weekend. How did you get on?`,
    `Hi {name}, saw you were out at {circuit} at the weekend. How was it?`,
    `Hiya {name}, I see you were racing at {circuit} at the weekend. How did it go for you?`,
  ];

  function getRandomOutreach() {
    return COLD_OUTREACH_VARIATIONS[Math.floor(Math.random() * COLD_OUTREACH_VARIATIONS.length)];
  }

  // Update driver's pipeline stage via the background service worker.
  // Streamlit needs a full browser page load (WebSocket) to execute its
  // Python script — a simple fetch() just gets the HTML shell. The
  // background worker opens the app in an inactive tab that auto-closes.
  function updateDriverStage(driverName, stageName, createDriver = false) {
    if (!stageName || !driverName) return;
    // Prefer the real name from the pipeline/outreach card over social page names
    const bestName = pipelineDriverName || driverName;
    const readable = usernameToReadable(bestName);
    const circuitInput = document.getElementById('ag-circuit-input');
    const circuit = circuitInput ? circuitInput.value.trim() : '';
    sendRuntimeMessage({
      type: 'updateStage',
      driver: readable,
      stage: stageName,
      createDriver: createDriver,
      socialUrl: window.location.href,
      circuit: circuit
    });

    // Also capture and save the conversation when a template is used
    if (isDMPage()) {
      setTimeout(() => captureCurrentConversation(), 500);
    }
  }

  // Save outreach WITHOUT changing stage — saves social URL, message text,
  // and creates driver if needed. Stage is set manually from the contact card.
  // Extract the IG profile URL — even when on a DM page
  function getIgProfileUrl() {
    const url = window.location.href;
    // Already on a profile page — use directly
    if (isProfilePage()) return url.split('?')[0].split('#')[0];

    // Detect logged-in user's own username to exclude it
    const selfUsernames = new Set(['_caminocoaching']);
    // Also try to detect from page — the nav usually has a profile link with the logged-in username
    const navProfileLink = document.querySelector('a[href="/accounts/edit/"]') ||
      document.querySelector('span[dir="auto"]');
    // Extract from the page title or meta
    const metaUser = document.querySelector('meta[property="al:ios:url"]');
    if (metaUser) {
      const mu = (metaUser.content || '').match(/user\?username=([^&]+)/);
      if (mu) selfUsernames.add(mu[1].toLowerCase());
    }

    // Blocked path prefixes (navigation, not profile links)
    const BLOCKED = ['/direct', '/accounts', '/explore', '/stories', '/reels',
      '/inbox', '/settings', '/p/', '/api', '/static', '/about',
      '/nametag', '/developer', '/legal', '/privacy'];

    function isBlockedHref(href) {
      return BLOCKED.some(b => href.includes(b));
    }

    function extractUsername(href) {
      // Full URL: instagram.com/username or instagram.com/username/
      const m1 = href.match(/instagram\.com\/([A-Za-z0-9_.]+)\/?$/);
      if (m1 && m1[1].length >= 2) return m1[1];
      // Relative: /username or /username/
      const m2 = href.match(/^\/([A-Za-z0-9_.]+)\/?$/);
      if (m2 && m2[1].length >= 2) return m2[1];
      return null;
    }

    // Priority 1: Look in the DM conversation header area specifically
    // Instagram DM header has the recipient's avatar + name as a clickable link
    const headerSelectors = [
      'div[role="main"] header a[href]',           // Main area header
      'div[role="banner"] a[href]',                 // Banner area
      'section > div > div > div > div > a[href]',  // DM thread header
    ];

    for (const sel of headerSelectors) {
      const links = document.querySelectorAll(sel);
      for (const a of links) {
        const href = a.getAttribute('href') || '';
        if (isBlockedHref(href)) continue;
        const username = extractUsername(a.href || '') || extractUsername(href);
        if (username && !selfUsernames.has(username.toLowerCase())) {
          return 'https://www.instagram.com/' + username + '/';
        }
      }
    }

    // Priority 2: Scan all links but exclude self + navigation
    const allLinks = document.querySelectorAll('a[href]');
    for (const a of allLinks) {
      const href = a.href || '';
      if (isBlockedHref(href)) continue;
      const username = extractUsername(href);
      if (username && !selfUsernames.has(username.toLowerCase())) {
        return 'https://www.instagram.com/' + username + '/';
      }
    }

    // Priority 3: Relative links
    for (const a of allLinks) {
      const href = a.getAttribute('href') || '';
      if (!href.startsWith('/') || isBlockedHref(href)) continue;
      const username = extractUsername(href);
      if (username && !selfUsernames.has(username.toLowerCase())) {
        return 'https://www.instagram.com/' + username + '/';
      }
    }

    // Priority 4: Extract from currentName if it looks like a username
    if (typeof currentName !== 'undefined' && currentName) {
      const un = currentName.replace(/\s+/g, '').toLowerCase();
      if (currentName.indexOf(' ') === -1 && un.match(/^[a-z0-9_.]+$/) && !selfUsernames.has(un)) {
        return 'https://www.instagram.com/' + un + '/';
      }
    }

    return null;
  }

  function saveOutreachToApp(driverName, messageText, templateName, createDriver = false) {
    if (!driverName) return;
    // Prefer the real name from the pipeline/outreach card over social page names
    const bestName = pipelineDriverName || driverName;
    const readable = usernameToReadable(bestName);
    const circuitInput = document.getElementById('ag-circuit-input');
    const circuit = circuitInput ? circuitInput.value.trim() : '';
    const platform = window.location.href.includes('instagram.com') ? 'IG' : 'FB';

    // Get the actual profile URL (not the DM URL)
    const profileUrl = getIgProfileUrl() || '';

    // Get championship from chrome.storage (synced from Streamlit)
    chrome.storage.local.get('agd_championship', (data) => {
      const championship = data.agd_championship || '';
      sendRuntimeMessage({
        type: 'saveOutreach',
        driver: readable,
        socialUrl: profileUrl,
        platform: platform,
        message: messageText.substring(0, 1800),
        template: templateName,
        createDriver: createDriver,
        circuit: circuit,
        championship: championship
      }, (response) => {
        if (response && response.success) {
          showSavedTick();
          console.log('[AG] ✅ Outreach + URL saved for', readable);
        } else {
          showToast('⚠️ Save may have failed — check app');
          console.warn('[AG] Save response:', response);
        }
      });
    });
  }

  // ── Auto-save social URL when driver detected from pipeline ──────────────
  // Saves the IG URL silently. NO tick shown — tick only appears
  // when user takes an explicit action (template click, manual save).
  function autoSaveSocialUrl() {
    const profileUrl = getIgProfileUrl();
    if (!profileUrl) return;  // Don't save DM/thread URLs
    if (autoSavedUrl === profileUrl) return;
    if (!currentName && !pipelineDriverName) return;

    const driverName = pipelineDriverName || currentName;
    if (!driverName) return;

    autoSavedUrl = profileUrl;

    sendRuntimeMessage({
      type: 'saveUrl',
      driver: driverName,
      igUrl: profileUrl
    }, (response) => {
      if (response && response.success) {
        console.log('[AG] Auto-saved IG URL for', driverName, '(silent)');
      }
    });
  }

  function showSavedTick() {
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;
    let tick = btn.querySelector('.ag-saved-tick');
    if (!tick) {
      tick = document.createElement('div');
      tick.className = 'ag-saved-tick';
      btn.appendChild(tick);
    }
    tick.textContent = '✓';
    tick.style.background = '#22c55e';
    tick.style.display = 'flex';
    tick.style.animation = 'none';
    tick.offsetHeight;
    tick.style.animation = 'ag-tick-pulse 0.4s ease-out';
  }

  function showStageBadge(stageName) {
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;
    let badge = btn.querySelector('.ag-saved-tick');
    if (!badge) {
      badge = document.createElement('div');
      badge.className = 'ag-saved-tick';
      btn.appendChild(badge);
    }
    badge.textContent = '↑';
    badge.style.background = '#3b82f6';
    badge.style.display = 'flex';
    badge.style.animation = 'none';
    badge.offsetHeight;
    badge.style.animation = 'ag-tick-pulse 0.4s ease-out';
    showToast(`🔷 Stage → ${stageName}`, '#3b82f6');
  }

  // ── Styles ──────────────────────────────────────────────────────────────
  function injectStyles() {
    if (document.getElementById('antigravity-driver-styles')) return;
    const style = document.createElement('style');
    style.id = 'antigravity-driver-styles';
    style.textContent = `
      #${BUTTON_ID} {
        position: fixed;
        top: 24px;
        right: 24px;
        width: 56px;
        height: 56px;
        border-radius: 50%;
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        border: none;
        cursor: pointer;
        z-index: 100000;
        box-shadow: 0 4px 14px rgba(59, 130, 246, 0.35);
        display: flex;
        align-items: center;
        justify-content: center;
        transition: all 0.2s ease;
        padding: 0;
      }
      #${BUTTON_ID}:hover {
        transform: scale(1.1);
        box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
      }
      #${BUTTON_ID}:active { transform: scale(0.95); }
      #${BUTTON_ID} svg { width: 28px; height: 28px; fill: white; }
      #${BUTTON_ID}.no-driver {
        background: linear-gradient(135deg, #6b7280 0%, #4b5563 100%);
        box-shadow: 0 4px 14px rgba(107, 114, 128, 0.35);
      }
      #${BUTTON_ID}.panel-open {
        background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
        box-shadow: 0 4px 14px rgba(239, 68, 68, 0.35);
      }
      #${BUTTON_ID}.panel-open svg { transform: rotate(45deg); }
      .ag-saved-tick {
        position: absolute;
        top: -4px;
        right: -4px;
        width: 22px;
        height: 22px;
        background: #22c55e;
        border-radius: 50%;
        color: white;
        font-size: 14px;
        font-weight: bold;
        display: none;
        align-items: center;
        justify-content: center;
        border: 2px solid white;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        pointer-events: none;
      }
      @keyframes ag-tick-pulse {
        0% { transform: scale(0); }
        50% { transform: scale(1.3); }
        100% { transform: scale(1); }
      }

      /* ── Slide-out Panel — anchored TOP so message input stays visible ── */
      #${PANEL_ID} {
        position: fixed;
        top: 0;
        right: -380px;
        width: 360px;
        height: 45vh;
        max-height: 420px;
        background: #111827;
        color: #f9fafb;
        z-index: 99999;
        box-shadow: -4px 2px 24px rgba(0,0,0,0.4);
        border-radius: 0 0 0 12px;
        transition: right 0.3s ease;
        display: flex;
        flex-direction: column;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      }
      #${PANEL_ID}.open { right: 0; }

      .ag-panel-header {
        padding: 10px 14px;
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        flex-shrink: 0;
      }
      .ag-panel-header h2 {
        margin: 0 0 2px 0; font-size: 15px; font-weight: 700; color: white;
      }
      .ag-panel-header .ag-subtitle {
        font-size: 11px; color: rgba(255,255,255,0.8); margin: 0; display: none;
      }
      .ag-circuit-row {
        margin-top: 10px; display: flex; align-items: center; gap: 8px;
      }
      .ag-circuit-row label {
        font-size: 11px; font-weight: 600; color: rgba(255,255,255,0.9); white-space: nowrap;
      }
      #ag-circuit-input {
        flex: 1; padding: 5px 10px; border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.3); background: rgba(255,255,255,0.15);
        color: white; font-size: 13px; font-family: inherit; outline: none;
      }
      #ag-circuit-input::placeholder { color: rgba(255,255,255,0.5); }
      #ag-circuit-input:focus { border-color: white; background: rgba(255,255,255,0.25); }

      .ag-panel-body {
        flex: 1; overflow-y: auto; padding: 8px 12px;
      }
      .ag-panel-body::-webkit-scrollbar { width: 6px; }
      .ag-panel-body::-webkit-scrollbar-track { background: transparent; }
      .ag-panel-body::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }

      .ag-group-title {
        font-size: 11px; font-weight: 700; text-transform: uppercase;
        letter-spacing: 0.5px; color: #3b82f6;
        padding: 12px 0 6px; margin: 0; border-bottom: 1px solid #1f2937;
      }

      .ag-template-btn {
        display: block; width: 100%; text-align: left;
        padding: 7px 10px; margin: 3px 0;
        background: #1f2937; border: 1px solid #374151; border-radius: 6px;
        color: #e5e7eb; font-size: 12px; cursor: pointer;
        transition: all 0.15s ease; font-family: inherit; line-height: 1.2;
      }
      .ag-template-btn:hover {
        background: #374151; border-color: #3b82f6; color: white;
      }
      .ag-template-btn.ag-outreach {
        background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%);
        border-color: #3b82f6; padding: 9px 10px;
      }
      .ag-template-btn.ag-outreach:hover {
        background: linear-gradient(135deg, #1e40af 0%, #1d4ed8 100%);
        border-color: #60a5fa;
      }
      .ag-template-btn.ag-outreach .ag-tmpl-name { font-size: 12px; color: #60a5fa; }
      .ag-template-btn .ag-tmpl-name {
        font-weight: 600; display: block; margin-bottom: 2px;
        color: #3b82f6; font-size: 12px;
      }
      .ag-template-btn .ag-tmpl-preview {
        color: #9ca3af; font-size: 11px; display: block;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
      }

      .ag-copied-toast {
        position: fixed; top: 90px; right: 380px;
        background: #3b82f6; color: white;
        padding: 10px 20px; border-radius: 8px;
        font-size: 13px; font-weight: 600; z-index: 100001;
        opacity: 0; transition: opacity 0.2s ease; pointer-events: none;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      }
      .ag-copied-toast.show { opacity: 1; }

      .ag-panel-footer {
        padding: 8px 12px; border-top: 1px solid #1f2937; flex-shrink: 0;
      }
      .ag-panel-footer a {
        display: block; text-align: center; color: #3b82f6;
        text-decoration: none; font-size: 11px; padding: 6px;
        border: 1px solid #3b82f6; border-radius: 6px; transition: all 0.15s;
      }
      .ag-panel-footer a:hover { background: rgba(59, 130, 246, 0.1); }
    `;
    document.head.appendChild(style);
  }

  // ── Name Extraction ─────────────────────────────────────────────────────
  function isProfilePage() {
    const url = window.location.href;
    // Profile pages: instagram.com/username (NOT /direct/, /explore/, /reels/, etc.)
    if (url.includes('/direct') || url.includes('/explore') || url.includes('/reels') ||
      url.includes('/accounts') || url.includes('/p/') || url.includes('/stories/')) return false;
    return /instagram\.com\/[A-Za-z0-9._]+\/?(\?|$)/.test(url);
  }

  function extractConversationName() {
    const skipWords = ['Instagram', 'Direct', 'Messages', 'New message', 'New Message',
      'Active', 'Search', 'Chats', 'Primary', 'General', 'Requests',
      'Message...', 'Send', 'Audio call ended', 'Video',
      '_caminocoaching', 'Your note', 'Ask friends anything',
      'Threads', 'Suggested for you', 'Following', 'Followers',
      'Posts', 'Reels', 'Tagged', 'Edit profile', 'Log in', 'Sign up',
      // Instagram navigation items (sidebar/bottom nav)
      'Home', 'Explore', 'Create', 'Profile', 'More', 'Notifications',
      'Shop', 'Settings', 'Saved', 'Switch accounts', 'Log out',
      'Report a problem', 'Note', 'Notes', 'Reel'];

    // Instagram notification / inbox preview patterns that should never be treated as names
    const notificationPatterns = [
      /\bmessaged you\b/i,
      /\bsent (a |an )?/i,           // "sent an attachment", "sent a photo"
      /\bliked a message\b/i,
      /\breacted to\b/i,
      /\breplied to\b/i,
      /\bshared a\b/i,               // "shared a post", "shared a reel"
      /\bmentioned you\b/i,
      /\bstarted a video\b/i,
      /\bstarted a call\b/i,
      /\bis typing\b/i,
      /\bwaved at you\b/i,
      /\bunsent a message\b/i,
      /\bnamed the group\b/i,
      /\badded you\b/i,
      /\bchanged the\b/i,
      /\bleft the group\b/i,
      /\bYou're now connected\b/i,
      /\bsays? hi\b/i,
      /\bwants to send\b/i,
      /\brequested to\b/i,
    ];

    function isValidName(text) {
      if (!text || text.length < 2 || text.length > 60) return false;
      if (!/[a-zA-Z]/.test(text)) return false;
      if (skipWords.some(w => text === w || text.startsWith(w))) return false;
      // Reject Instagram notification / inbox preview text
      if (notificationPatterns.some(rx => rx.test(text))) return false;
      return true;
    }

    // ── PROFILE PAGE: name from header or page title ──
    if (isProfilePage()) {
      // Pipeline driver name — ALWAYS preferred on profile pages.
      // The pipeline app passes the real name via #ag_driver= hash when user
      // clicks search buttons. This avoids picking up usernames like "jsmith_91".
      if (pipelineDriverName) return cleanName(pipelineDriverName);

      // IG profile pages show the display name in a header/span near the top
      // Try the page title first: "Display Name (@username) • Instagram"
      const title = document.title;
      if (title) {
        const cleaned = title.replace(/^\(\d+\)\s*/, '');
        // Format: "Display Name (@username) • Instagram photos and videos"
        const match = cleaned.match(/^(.+?)\s*\(@/);
        if (match && isValidName(match[1].trim())) {
          return cleanName(match[1].trim());
        }
        // Fallback: split on bullet
        const parts = cleaned.split(/[•·|]/);
        if (parts.length > 0) {
          // First part might be "Display Name (@username)"
          const first = parts[0].replace(/\(@.*?\)/, '').trim();
          if (isValidName(first)) return cleanName(first);
        }
      }
      // Try header elements on the profile page
      const profileHeader = document.querySelector('header section h1, header section h2');
      if (profileHeader) {
        const text = profileHeader.textContent.trim();
        if (isValidName(text)) return cleanName(text);
      }
      // Try spans inside the header area
      const headerSpans = document.querySelectorAll('header span');
      for (const span of headerSpans) {
        const text = span.textContent.trim();
        // Skip usernames (contain underscores/dots typically) and numbers
        if (text.includes('_') || /^\d/.test(text)) continue;
        if (isValidName(text) && text.includes(' ')) return cleanName(text);
      }
      return null;
    }

    // ── DM PAGE strategies ──
    // Strategy 1: Page title — IG may use "Instagram • Name" or "Chats • Instagram"
    const title = document.title;
    if (title) {
      const cleaned = title.replace(/^\(\d+\)\s*/, '');
      const parts = cleaned.split(/[•·|]/);
      if (parts.length > 1) {
        for (const part of [...parts].reverse()) {
          const name = part.trim();
          if (isValidName(name)) return cleanName(name);
        }
      }
    }

    // Strategy 2: DOM — the conversation header area
    // IG DMs show the name in a prominent header above the chat.
    // From the screenshot: "Jacob Pierce" appears as bold text, "jacob.pierce54" below it
    const selectors = [
      // Top header of the conversation panel — name as link or heading
      'div[role="main"] header a div span',
      'div[role="main"] header h2',
      'div[role="main"] header section h1',
      'div[role="main"] header span[dir="auto"]',
      // Heading roles ONLY within main content area (not nav)
      'div[role="main"] div[role="heading"] span',
      'main header a span',
      // Conversation title area
      'section > div > div > div > div > div > a > div > span',
    ];

    for (const sel of selectors) {
      const els = document.querySelectorAll(sel);
      for (const el of els) {
        const text = el.textContent.trim();
        if (isValidName(text)) {
          return cleanName(text);
        }
      }
    }

    // Strategy 2b: Profile links ONLY inside the main conversation area
    // More targeted than Strategy 3 — only searches within div[role="main"]
    const mainArea = document.querySelector('div[role="main"]');
    if (mainArea) {
      const mainLinks = mainArea.querySelectorAll('a[role="link"]');
      for (const a of mainLinks) {
        const href = a.href || '';
        if (href.includes('instagram.com/') && !href.includes('/direct') && !href.includes('/accounts')) {
          const pathMatch = href.match(/instagram\.com\/([A-Za-z0-9._]+)/);
          if (!pathMatch || pathMatch[1].length < 2) continue;
          const spans = a.querySelectorAll('span');
          for (const span of spans) {
            const text = span.textContent.trim();
            if (isValidName(text)) return cleanName(text);
          }
          // If the link has no valid span text, derive name from username
          const username = pathMatch[1];
          if (username.length >= 3 && !username.startsWith('_')) {
            const derived = username.replace(/[_\.]/g, ' ').replace(/\d+$/g, '').trim();
            if (derived.includes(' ') && isValidName(derived)) return cleanName(derived);
          }
        }
      }
    }

    // Strategy 3: Look for the name from ALL profile links on the page (fallback)
    // IG often wraps the name in a clickable link inside the header
    const igNavPages = ['explore', 'reels', 'stories', 'direct', 'accounts', 'p', 'tv',
      'about', 'terms', 'privacy', 'settings', 'nametag', 'directory', 'lite'];
    const allLinks = document.querySelectorAll('a[role="link"]');
    for (const a of allLinks) {
      const href = a.href || '';
      // Links to IG profiles look like instagram.com/username/
      if (href.includes('instagram.com/') && !href.includes('/direct') && !href.includes('/accounts')) {
        // Skip root/nav links — extract the first path segment
        const pathMatch = href.match(/instagram\.com\/([A-Za-z0-9._]+)/);
        if (!pathMatch || pathMatch[1].length < 2) continue;
        if (igNavPages.includes(pathMatch[1].toLowerCase())) continue;
        const spans = a.querySelectorAll('span');
        for (const span of spans) {
          const text = span.textContent.trim();
          if (isValidName(text)) return cleanName(text);
        }
      }
    }

    return null;
  }

  function cleanName(raw) {
    if (!raw) return null;
    let name = raw
      .replace(/[\u{1F000}-\u{1FFFF}]/gu, '')
      .replace(/[\u{2600}-\u{27BF}]/gu, '')
      .replace(/[\u{FE00}-\u{FEFF}]/gu, '')
      .replace(/\(.*?\)/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (name.startsWith('@')) name = name.substring(1);
    return name;
  }

  function usernameToReadable(name) {
    // "john_smith.racing" → "john smith racing" for template {name}
    return name.replace(/[_\.]/g, ' ').trim();
  }

  function getFirstName(fullName) {
    if (!fullName) return 'Mate';
    // If we have a pipeline driver name, always use that for the first name
    // (the IG page name might be a username like "jacob.pierce54")
    const nameToUse = pipelineDriverName || fullName;
    const readable = usernameToReadable(nameToUse);
    const first = readable.split(' ')[0];
    return first.charAt(0).toUpperCase() + first.slice(1);
  }

  // ── Paste into Instagram DM input ───────────────────────────────────────
  function findMessageInput() {
    const inputSelectors = [
      'div[role="textbox"][contenteditable="true"]',
      'textarea[placeholder*="Message"]',
      'div[contenteditable="true"][aria-label*="Message"]',
      'div[contenteditable="true"][aria-label*="message"]',
      'div[role="textbox"]',
    ];
    for (const sel of inputSelectors) {
      const el = document.querySelector(sel);
      if (el) return el;
    }
    return null;
  }

  async function openMessagePopup() {
    if (!isProfilePage()) return false;
    // On Instagram profile pages, click the "Message" button
    const btns = document.querySelectorAll('div[role="button"], a[role="button"], button');
    for (const el of btns) {
      const text = el.textContent.trim();
      if (text === 'Message') {
        el.click();
        return new Promise(resolve => {
          let attempts = 0;
          const check = setInterval(() => {
            attempts++;
            const input = findMessageInput();
            if (input) { clearInterval(check); resolve(true); }
            else if (attempts > 30) { clearInterval(check); resolve(false); }
          }, 100);
        });
      }
    }
    return false;
  }

  async function pasteIntoInput(text) {
    let inputEl = findMessageInput();

    // If no input found, try opening a message popup on profile pages
    if (!inputEl) {
      const opened = await openMessagePopup();
      if (opened) inputEl = findMessageInput();
    }

    if (inputEl) {
      inputEl.focus();

      // Use clipboard API + paste event — most reliable for React inputs
      try {
        await navigator.clipboard.writeText(text);

        const pasteEvent = new ClipboardEvent('paste', {
          bubbles: true,
          cancelable: true,
          clipboardData: new DataTransfer()
        });
        pasteEvent.clipboardData.setData('text/plain', text);
        inputEl.dispatchEvent(pasteEvent);

        if (!pasteEvent.defaultPrevented) {
          document.execCommand('insertText', false, text);
        }

        return true;
      } catch (e) {
        return false;
      }
    }
    return false;
  }

  // ── Message Extraction ─────────────────────────────────────────────────
  // Scrapes the last ~10 visible messages from the active Instagram DM
  // conversation to send to the Streamlit app as a conversation log reference.

  let lastCapturedName = null;
  let captureTimeout = null;

  function isDMPage() {
    return window.location.href.includes('/direct');
  }

  function extractConversationMessages() {
    const messages = [];
    const mainArea = document.querySelector('div[role="main"]');
    if (!mainArea) return messages;

    const mainRect = mainArea.getBoundingClientRect();
    const midX = mainRect.left + mainRect.width / 2;

    // IG DMs: messages are in div[dir="auto"] inside the main conversation area
    const candidates = mainArea.querySelectorAll('div[dir="auto"]');
    const skipTexts = new Set([
      'Reply', 'React', 'More', 'Unsend', 'Forward', 'Copy', 'Like',
      'Active now', 'Instagram', 'Send', 'Message...', 'Message…',
      'GIF', 'Voice message', 'Quick camera', 'Gallery',
      'Search', 'Chats', 'Primary', 'General', 'Requests',
      'Audio call ended', 'Video call ended', 'Seen',
      'Delivered', 'Sent', 'Typing...', 'Typing…',
      'New message', 'Translate message', 'See translation'
    ]);

    for (const el of candidates) {
      const text = el.textContent.trim();
      if (!text || text.length < 1 || text.length > 500) continue;
      if (skipTexts.has(text)) continue;

      const rect = el.getBoundingClientRect();
      if (rect.width < 30 || rect.height < 12) continue;

      // Skip elements outside the chat scroll area
      if (rect.top < mainRect.top + 60 || rect.bottom > mainRect.bottom - 40) continue;

      // Walk up to find the message bubble
      let bubble = el;
      for (let i = 0; i < 8; i++) {
        if (!bubble.parentElement) break;
        bubble = bubble.parentElement;
        const style = window.getComputedStyle(bubble);
        const br = parseFloat(style.borderRadius);
        if (br >= 8 && bubble.getBoundingClientRect().width < mainRect.width * 0.8) break;
      }

      const bubbleRect = bubble.getBoundingClientRect();
      const bubbleCenterX = bubbleRect.left + bubbleRect.width / 2;
      const isYou = bubbleCenterX > midX;

      // Skip tiny meta text (timestamps, status)
      const style = window.getComputedStyle(el);
      const fontSize = parseFloat(style.fontSize);
      if (fontSize < 11) continue;

      // Avoid duplicates from nested elements
      const lastMsg = messages[messages.length - 1];
      if (lastMsg && lastMsg.text === text.substring(0, 150)) continue;

      messages.push({
        sender: isYou ? 'You' : 'Them',
        text: text.substring(0, 150)
      });
    }

    return messages.slice(-10);
  }

  function formatMessagesForTransport(messages) {
    if (!messages || messages.length === 0) return '';
    return messages
      .map(m => `${m.sender === 'You' ? 'Y' : 'T'}>${m.text.replace(/\|/g, ' ').replace(/\n/g, ' ')}`)
      .join('||')
      .substring(0, 1800);
  }

  function saveConversationToApp(driverName, messages) {
    if (!driverName || !messages || messages.length === 0) return;
    const formatted = formatMessagesForTransport(messages);
    if (!formatted) return;

    try {
      const key = `agd_msgs_${driverName.toLowerCase().replace(/\s+/g, '_')}`;
      chrome.storage.local.set({ [key]: { messages, timestamp: Date.now() } });
    } catch (e) { /* ignore */ }

    sendRuntimeMessage({
      type: 'saveConversation',
      driver: usernameToReadable(driverName),
      messages: formatted,
      platform: 'IG'
    });
  }

  function captureCurrentConversation() {
    if (!currentName || !isDMPage()) return;
    const messages = extractConversationMessages();
    if (messages.length >= 2) {
      saveConversationToApp(currentName, messages);
    }
  }

  // ── Toast ───────────────────────────────────────────────────────────────
  function showToast(message, color) {
    let toast = document.querySelector('.ag-copied-toast');
    if (!toast) {
      toast = document.createElement('div');
      toast.className = 'ag-copied-toast';
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.style.background = color || '#3b82f6';
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2500);
  }

  // ── Build the panel ─────────────────────────────────────────────────────
  function createPanel() {
    if (document.getElementById(PANEL_ID)) return;

    const panel = document.createElement('div');
    panel.id = PANEL_ID;
    const firstName = getFirstName(currentName);

    const header = document.createElement('div');
    header.className = 'ag-panel-header';
    header.innerHTML = `
      <h2>🏁 ${currentName || 'Driver'}</h2>
      <p class="ag-subtitle">Pick a reply template below</p>
      <div class="ag-circuit-row">
        <label>Circuit / Track:</label>
        <input type="text" id="ag-circuit-input" placeholder="e.g. Brands Hatch" autocomplete="off" />
      </div>
    `;

    // Pre-populate circuit from storage, and save on change
    const circuitEl = header.querySelector('#ag-circuit-input');
    if (circuitEl) {
      chrome.storage.local.get('agd_circuit', (data) => {
        if (data.agd_circuit && !circuitEl.value) circuitEl.value = data.agd_circuit;
      });
      circuitEl.addEventListener('input', () => {
        const val = circuitEl.value.trim();
        if (val) chrome.storage.local.set({ agd_circuit: val });
      });
    }

    const body = document.createElement('div');
    body.className = 'ag-panel-body';

    // --- CAPTURE THREAD BUTTON (prominent, at the top) ---
    const captureBtn = document.createElement('button');
    captureBtn.className = 'ag-template-btn ag-capture-btn';
    captureBtn.innerHTML = `
      <span class="ag-tmpl-name">📤 Send Thread to App</span>
      <span class="ag-tmpl-preview">Captures this conversation and sends it to the Pipeline App for AI follow-up</span>
    `;
    captureBtn.style.cssText = 'background:#E1306C;color:white;border:none;margin-bottom:12px;';
    captureBtn.addEventListener('click', () => {
      if (!currentName || !isDMPage()) {
        showToast('Open a DM conversation first');
        return;
      }
      const messages = extractConversationMessages();
      if (messages.length < 1) {
        showToast('No messages found in this conversation');
        return;
      }
      saveConversationToApp(currentName, messages);
      showToast(`📤 Sent ${messages.length} messages to app`);
    });
    body.appendChild(captureBtn);



    // --- AI OUTREACH MESSAGE (synced from webapp) ---
    const aiMsgContainer = document.createElement('div');
    aiMsgContainer.id = 'ag-ai-outreach-container';
    aiMsgContainer.style.display = 'none'; // Hidden until loaded

    const aiBtn = document.createElement('button');
    aiBtn.className = 'ag-template-btn ag-outreach';
    aiBtn.style.cssText = 'background:#2563eb;color:white;border:none;margin-bottom:12px;';
    aiBtn.innerHTML = `
      <span class="ag-tmpl-name">🤖 AI Race Outreach</span>
      <span class="ag-tmpl-preview">Loading from app...</span>
    `;
    aiMsgContainer.appendChild(aiBtn);
    body.appendChild(aiMsgContainer);

    // Load AI message from chrome.storage
    // AI message may use {name} placeholder — replaced at click time with current driver's first name
    let currentAiTemplate = '';

    function updateAiButton(template) {
      currentAiTemplate = template;
      const preview = template.replace(/\{name\}/g, firstName).replace(/\n/g, ' ').substring(0, 80);
      const previewEl = aiBtn.querySelector('.ag-tmpl-preview');
      if (previewEl) previewEl.textContent = preview + (template.length > 80 ? '...' : '');
      aiMsgContainer.style.display = 'block';
    }

    // Look up driver-specific AI message from the per-driver dictionary,
    // falling back to the single default message if no match found.
    function loadAiMessage() {
      try {
        chrome.storage.local.get(['agd_ai_messages', 'agd_ai_outreach_msg', 'agd_current_driver'], (data) => {
          const driverName = pipelineDriverName || (data && data.agd_current_driver) || currentName || '';
          const msgs = (data && data.agd_ai_messages) || {};

          // Try exact match, then partial match
          let template = msgs[driverName];
          if (!template && driverName) {
            const lower = driverName.toLowerCase();
            for (const [key, val] of Object.entries(msgs)) {
              if (key.toLowerCase() === lower ||
                key.toLowerCase().includes(lower) ||
                lower.includes(key.toLowerCase())) {
                template = val;
                break;
              }
            }
          }

          // Fall back to single message
          if (!template && data && data.agd_ai_outreach_msg) {
            template = data.agd_ai_outreach_msg;
          }

          if (template) {
            updateAiButton(template.replace(/\\n/g, '\n'));
          }
        });
      } catch (e) {
        console.warn('[AG] Could not load AI message from storage:', e.message);
      }
    }
    loadAiMessage();

    // Re-check when storage changes
    try {
      chrome.storage.onChanged.addListener((changes) => {
        if (changes.agd_ai_messages || changes.agd_ai_outreach_msg || changes.agd_current_driver) {
          loadAiMessage();
        }
      });
    } catch (e) {
      console.warn('[AG] Could not set up storage listener:', e.message);
    }

    aiBtn.addEventListener('click', async () => {
      if (!currentAiTemplate) { showToast('No AI message — open app first'); return; }
      const msg = currentAiTemplate.replace(/\{name\}/g, firstName);
      const pasted = await pasteIntoInput(msg);
      if (pasted) {
        showToast('🤖 AI message pasted!');
        saveOutreachToApp(currentName, msg, 'AI Race Outreach', true);
      } else {
        try {
          await navigator.clipboard.writeText(msg);
          showToast('📋 AI message copied!');
        } catch (e) {
          showToast('Could not paste — copy manually');
        }
      }
    });

    for (const [groupName, templateKeys] of Object.entries(TEMPLATE_GROUPS)) {
      const groupTitle = document.createElement('div');
      groupTitle.className = 'ag-group-title';
      groupTitle.textContent = groupName;
      body.appendChild(groupTitle);

      for (const key of templateKeys) {
        const template = REPLY_TEMPLATES[key];
        if (!template) continue;

        const btn = document.createElement('button');
        btn.className = CREATE_DRIVER_TEMPLATES.includes(key)
          ? 'ag-template-btn ag-outreach'
          : 'ag-template-btn';
        const preview = template.replace('{name}', firstName).replace('{circuit}', '___').substring(0, 80);
        btn.innerHTML = `
          <span class="ag-tmpl-name">${key}</span>
          <span class="ag-tmpl-preview">${preview}...</span>
        `;

        btn.addEventListener('click', async () => {
          const circuitInput = document.getElementById('ag-circuit-input');
          const circuit = circuitInput ? circuitInput.value.trim() : '';
          if (template.includes('{circuit}') && !circuit) {
            showToast('Enter the circuit name first ↑');
            if (circuitInput) circuitInput.focus();
            return;
          }
          let tmpl = template.includes('__RANDOM_OUTREACH__') ? getRandomOutreach() : template;
          const msg = tmpl.replace(/\{name\}/g, firstName).replace(/\{circuit\}/g, circuit);
          const pasted = await pasteIntoInput(msg);
          const stage = TEMPLATE_STAGE_MAP[key];
          const shouldCreate = CREATE_DRIVER_TEMPLATES.includes(key);
          const isOutreach = RACE_OUTREACH_TEMPLATES.includes(key);

          if (isOutreach) {
            saveOutreachToApp(currentName, msg, key, shouldCreate);
            if (pasted) {
              showToast('📤 Pasted & saved — URL + message recorded');
            } else {
              await navigator.clipboard.writeText(msg);
              showToast('📤 Copied & saved — URL + message recorded');
            }
          } else {
            if (pasted) {
              if (stage) {
                updateDriverStage(currentName, stage, shouldCreate);
                showStageBadge(stage);
              } else {
                showToast('Pasted — review & hit send');
              }
            } else {
              await navigator.clipboard.writeText(msg);
              if (stage) {
                updateDriverStage(currentName, stage, shouldCreate);
                showStageBadge(stage);
              } else {
                showToast('📋 Copied — paste with Ctrl+V');
              }
            }
            if (panelOpen) togglePanel();
          }
        });
        body.appendChild(btn);
      }
    }

    const footer = document.createElement('div');
    footer.className = 'ag-panel-footer';
    const driverParam = encodeURIComponent(usernameToReadable(currentName || ''));
    footer.innerHTML = `<a href="${APP_URL}?driver=${driverParam}&tab=dashboard" target="_blank">Open full card in Pipeline App →</a>`;

    panel.appendChild(header);
    panel.appendChild(body);
    panel.appendChild(footer);
    document.body.appendChild(panel);
  }

  function updatePanel() {
    const panel = document.getElementById(PANEL_ID);
    if (!panel) return;
    const firstName = getFirstName(currentName);

    const h2 = panel.querySelector('.ag-panel-header h2');
    if (h2) h2.innerHTML = `🏁 ${currentName || 'Driver'}`;

    const link = panel.querySelector('.ag-panel-footer a');
    if (link) {
      const driverParam = encodeURIComponent(usernameToReadable(currentName || ''));
      link.href = `${APP_URL}?driver=${driverParam}&tab=dashboard`;
    }

    // Get ONLY the template buttons (skip special buttons: capture, save URL, AI outreach)
    // Template buttons are the ones that come AFTER ag-group-title divs
    const allBtns = Array.from(panel.querySelectorAll('.ag-template-btn'));
    const specialIds = ['ag-save-url-btn', 'ag-ai-outreach-container'];
    const templateBtns = allBtns.filter(btn => {
      if (btn.classList.contains('ag-capture-btn')) return false;
      if (btn.id === 'ag-save-url-btn') return false;
      if (btn.closest('#ag-ai-outreach-container')) return false;
      return true;
    });

    let idx = 0;
    for (const [, templateKeys] of Object.entries(TEMPLATE_GROUPS)) {
      for (const key of templateKeys) {
        const template = REPLY_TEMPLATES[key];
        if (!template || idx >= templateBtns.length) continue;
        const btn = templateBtns[idx];
        const preview = template.replace('{name}', firstName).replace('{circuit}', '___').substring(0, 80);
        const previewEl = btn.querySelector('.ag-tmpl-preview');
        if (previewEl) previewEl.textContent = preview + '...';
        const nameEl = btn.querySelector('.ag-tmpl-name');
        if (nameEl) nameEl.textContent = key;

        const newBtn = btn.cloneNode(true);
        btn.parentNode.replaceChild(newBtn, btn);
        newBtn.addEventListener('click', async () => {
          const circuitInput = document.getElementById('ag-circuit-input');
          const circuit = circuitInput ? circuitInput.value.trim() : '';
          if (template.includes('{circuit}') && !circuit) {
            showToast('Enter the circuit name first ↑');
            if (circuitInput) circuitInput.focus();
            return;
          }
          let tmpl = template.includes('__RANDOM_OUTREACH__') ? getRandomOutreach() : template;
          const msg = tmpl.replace(/\{name\}/g, firstName).replace(/\{circuit\}/g, circuit);
          const pasted = await pasteIntoInput(msg);
          const stage = TEMPLATE_STAGE_MAP[key];
          const shouldCreate = CREATE_DRIVER_TEMPLATES.includes(key);
          const isOutreach = RACE_OUTREACH_TEMPLATES.includes(key);

          if (isOutreach) {
            saveOutreachToApp(currentName, msg, key, shouldCreate);
            if (pasted) {
              showToast('📤 Pasted & saved — URL + message recorded');
            } else {
              await navigator.clipboard.writeText(msg);
              showToast('📤 Copied & saved — URL + message recorded');
            }
          } else {
            if (pasted) {
              if (stage) {
                updateDriverStage(currentName, stage, shouldCreate);
                showToast(`✅ Pasted · stage → ${stage.replace(/_/g, ' ')} · URL saved`);
              } else {
                showToast('Pasted — review & hit send');
              }
            } else {
              await navigator.clipboard.writeText(msg);
              if (stage) updateDriverStage(currentName, stage, shouldCreate);
              showToast(`📋 Copied · stage → ${stage ? stage.replace(/_/g, ' ') : 'unchanged'} · URL saved`);
            }
            if (panelOpen) togglePanel();
          }
        });
        idx++;
      }
    }
  }

  function togglePanel() {
    const panel = document.getElementById(PANEL_ID);
    const btn = document.getElementById(BUTTON_ID);
    if (!panel) return;
    panelOpen = !panelOpen;
    if (panelOpen) {
      panel.classList.add('open');
      btn.classList.add('panel-open');
      btn.title = 'Close driver card';
    } else {
      panel.classList.remove('open');
      btn.classList.remove('panel-open');
      btn.title = `Open ${currentName || 'driver'} card`;
    }
  }

  // ── Button ──────────────────────────────────────────────────────────────
  function createButton() {
    if (document.getElementById(BUTTON_ID)) return;
    const btn = document.createElement('button');
    btn.id = BUTTON_ID;
    btn.title = 'Open driver contact card (drag to move)';
    btn.innerHTML = `<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
    </svg>`;

    // ── Drag-to-move logic ─────────────────────────────────────
    let isDragging = false;
    let dragStartX, dragStartY, btnStartX, btnStartY;
    const DRAG_THRESHOLD = 5;

    btn.addEventListener('mousedown', (e) => {
      if (e.button !== 0) return;
      isDragging = false;
      dragStartX = e.clientX;
      dragStartY = e.clientY;
      const rect = btn.getBoundingClientRect();
      btnStartX = rect.left;
      btnStartY = rect.top;
      e.preventDefault();
    });

    document.addEventListener('mousemove', (e) => {
      if (dragStartX == null) return;
      const dx = e.clientX - dragStartX;
      const dy = e.clientY - dragStartY;
      if (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD) {
        isDragging = true;
        btn.style.transition = 'none';
        btn.style.right = 'auto';
        btn.style.left = Math.min(Math.max(0, btnStartX + dx), window.innerWidth - 56) + 'px';
        btn.style.top = Math.min(Math.max(0, btnStartY + dy), window.innerHeight - 56) + 'px';
      }
    });

    document.addEventListener('mouseup', () => {
      if (dragStartX == null) return;
      if (isDragging) {
        btn.style.transition = 'all 0.2s ease';
        try {
          chrome.storage.local.set({ ag_btn_pos_ig: { left: btn.style.left, top: btn.style.top } });
        } catch (e) { }
      }
      dragStartX = null;
    });

    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (isDragging) { isDragging = false; return; }
      const name = extractConversationName();
      if (name) currentName = name;
      if (!currentName) {
        showToast('Open a conversation first');
        return;
      }
      if (!document.getElementById(PANEL_ID)) {
        createPanel();
      } else {
        updatePanel();
      }
      togglePanel();
    });

    // Restore saved position
    try {
      chrome.storage.local.get('ag_btn_pos_ig', (data) => {
        if (data.ag_btn_pos_ig) {
          btn.style.right = 'auto';
          btn.style.left = data.ag_btn_pos_ig.left;
          btn.style.top = data.ag_btn_pos_ig.top;
        }
      });
    } catch (e) { }

    document.body.appendChild(btn);
  }

  let lastAutoOpenName = null;

  function updateButton() {
    const name = extractConversationName();
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;
    if (name && name !== currentName) {
      // Capture messages from the PREVIOUS conversation before switching
      if (currentName && currentName !== name && isDMPage()) {
        const prevMessages = extractConversationMessages();
        if (prevMessages.length >= 2) {
          saveConversationToApp(currentName, prevMessages);
        }
      }

      currentName = name;
      btn.classList.remove('no-driver');
      btn.title = `Open ${name}'s contact card`;

      // Auto-save social URL on profile pages (not DMs)
      setTimeout(() => autoSaveSocialUrl(), 1500);

      // Schedule capture of the NEW conversation after it loads
      if (captureTimeout) clearTimeout(captureTimeout);
      captureTimeout = setTimeout(() => {
        captureCurrentConversation();
      }, 3000);

      // Auto-open panel when a new conversation is detected
      if (name !== lastAutoOpenName) {
        lastAutoOpenName = name;
        if (!document.getElementById(PANEL_ID)) {
          createPanel();
        } else {
          updatePanel();
        }
        if (!panelOpen) {
          togglePanel();
        } else {
          updatePanel();
        }
      } else if (panelOpen) {
        updatePanel();
      }
    } else if (!name) {
      currentName = null;
      btn.classList.add('no-driver');
      btn.title = 'Open a conversation first';
    }
  }

  let updateTimeout = null;
  function startThrottledObserver() {
    if (observer) observer.disconnect();
    observer = new MutationObserver(() => {
      if (updateTimeout) clearTimeout(updateTimeout);
      updateTimeout = setTimeout(updateButton, 500);
    });
    const target = document.querySelector('div[role="main"]') || document.body;
    observer.observe(target, { childList: true, subtree: true });
  }

  function init() {
    injectStyles();
    createButton();
    currentName = extractConversationName();
    updateButton();
    startThrottledObserver();
    let lastUrl = location.href;
    setInterval(() => {
      if (location.href !== lastUrl) {
        lastUrl = location.href;
        // Reset auto-save tracking on navigation
        autoSavedUrl = null;
        // Clear stale pipeline name BEFORE checking hash — prevents wrong
        // driver showing when navigating between profiles without #ag_driver=
        pipelineDriverName = null;
        // Check for new #ag_driver= hash on navigation (sets pipelineDriverName if present)
        checkHashForDriverName();
        // Clear pipeline name when entering DMs (use real chat name)
        if (isDMPage()) {
          pipelineDriverName = null;
        } else {
          // Auto-save URL on profile/search page navigation
          setTimeout(() => autoSaveSocialUrl(), 2000);
        }
        setTimeout(updateButton, 800);
        setTimeout(updateButton, 2000);
      }
    }, 1000);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(init, 1500);
  } else {
    window.addEventListener('load', () => setTimeout(init, 1500));
  }
})();

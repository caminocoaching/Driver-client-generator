// Antigravity Driver Connect — Facebook Messenger Content Script
// Shows a slide-out contact card panel with reply templates directly on the
// Messenger page. Pick a reply, it gets pasted into the message input.

(function () {
  'use strict';

  const APP_URL = 'https://driver-client-generator.streamlit.app';
  const BUTTON_ID = 'antigravity-rider-btn';
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
          // Service worker may have been asleep — retry once
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
  let _nameSetByApp = false;     // True if pipelineDriverName was set by the app (not DB lookup)
  let panelOpen = false;
  let observer = null;
  let autoSavedUrl = null;  // Track which URL we already auto-saved
  let dbLookupDone = false;  // Wait for DB lookup before auto-saving (avoids duplicate records)

  // Check URL hash for driver name passed from pipeline app search buttons
  function checkHashForRiderName() {
    const hash = window.location.hash;
    if (hash && hash.includes('ag_driver=')) {
      const name = decodeURIComponent(hash.split('ag_driver=')[1].replace(/\+/g, ' '));
      if (name && name.length > 1) {
        pipelineDriverName = name;
        _nameSetByApp = true;
        chrome.storage.local.set({ ag_current_driver: name });
        // Clean the hash so it doesn't persist on navigation
        history.replaceState(null, '', window.location.href.split('#')[0]);
      }
    }
  }
  checkHashForRiderName();

  // NOTE: We intentionally do NOT load ag_current_driver from chrome.storage
  // on init. The hash (#ag_driver=) is the only reliable source for pipeline
  // driver name. Loading stale storage data caused wrong names when navigating
  // between profiles. The page-based name detection (extractConversationName)
  // handles cases where no hash is present.

  // Listen for driver name changes from the Streamlit app
  chrome.storage.onChanged.addListener((changes) => {
    if (changes.ag_current_driver && changes.ag_current_driver.newValue) {
      pipelineDriverName = changes.ag_current_driver.newValue;
      _nameSetByApp = true;
    }
  });

  // Clean championship name for messages — remove governing body suffixes
  function cleanChampionshipName(name) {
    if (!name) return '';
    return name
      .replace(/\s*[-–—]?\s*certified by FIA\b/gi, '')
      .replace(/\s*[-–—]?\s*sanctioned by FIA\b/gi, '')
      .replace(/\s*[-–—]\s*$/, '')
      .trim();
  }

  // ── Reply Templates (synced from ui_components.py) ──────────────────────
  const REPLY_TEMPLATES = {
    // --- COLD OUTREACH (randomly picks variation when clicked) ---
    "Cold Outreach": `__RANDOM_OUTREACH__`,

    // --- END OF SEASON OUTREACH (randomly picks variation when clicked) ---
    "End of Season": `__RANDOM_END_OF_SEASON__`,

    // --- COLD OUTREACH RESPONSES / REPLIES ---
    "Great Work (Reply)": `Thanks for the reply {name},\nThat's Great work well done!\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    "Productive (Reply)": `Thanks for the reply {name},\nSounds like you had a productive weekend.\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    "Tough Weekend (Reply)": `Thanks for the reply {name}, it Sounds like you had a tough weekend.\n\nNot sure if you know, I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?`,

    // --- SEND LINKS ---
    "Send Link (Yes)": `Superb, {name} Here is the link to The Post-Race Weekend Performance Score\nhttps://improve-rider.scoreapp.com\n\nThis short review zeroes in on where you're losing lap time, where any gaps are showing up and how to fill them 🚀\n\nAt the bottom of the results page is some free training on how to fill those gaps 👍🏻`,

    "Send Season Review": `Great to hear {name}! Here's the link to the End of Season Review\nhttps://driverseason.scoreapp.com\n\nIt takes about 5 minutes and breaks down your whole season — what went well, where the gaps are, and what to focus on heading into next season 🏁\n\nAt the bottom of the results page there's some free training on how to close those gaps 👍🏻`,

    "Send Blueprint Link": `OK {name} here you go, instant access to the Podium Contenders Blueprint\nhttps://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/\n\n📚 What you'll learn:\n✓ Day 1: The 7 biggest mistakes costing you lap times\n✓ Day 2: The 5-pillar system for accessing flow state on command\n✓ Day 3: Your race weekend mental preparation protocol\n\nComplete all 3 days, and you'll unlock a free strategy call where we'll create your personalised performance roadmap for 2026.\nSee you inside! 🏁\nCraig`,

    "Offer Free Training": `Hey {name}, Great to see you will be lining up on the grid this season\nWe have some pre-season free training that many riders are using to ensure they are on point from the first round this season.\nWant me to send it over?`,

    // --- END OF SEASON REPLIES ---
    "End of Season (Reply)": `Thanks for the reply, {name}.\n\nSounds like you had a solid season!\n\nNot sure if you know — I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing — helping them access the Flow State, where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free end-of-season review that breaks down where your biggest gains are hiding heading into next season.\n\nWant me to send it over?`,

    // --- REVIEW DONE → NEXT STEP ---
    "Follow-Up (Review Done → Blueprint)": `Hey {name},\nSaw you completed the Race Weekend Review — nice one. Most riders never even get that far. They just keep doing the same thing and wondering why nothing changes.\nYour results actually flagged a couple of areas that the Free Training covers in detail, specifically how the top riders manage those exact patterns you scored on.\n\nWant me to send you the link?\nCraig`,

    // --- REVIEW STALLED ---
    "Stalled: Review Started": `Hey {name}, I see you started the Race Weekend Review but didn't get to the results page - that's where the good stuff is.\n\nYour results break down exactly where you're losing time and why - most riders tell me they had no idea THAT was the thing holding them back.\n\nPlus the results page unlocks access to free training that covers how to fix those exact gaps before the next round.\n\nTakes about 3 minutes to finish - want me to resend the link?`,

    // --- FOLLOW-UPS ---
    "Follow-Up (Link Sent Check)": `Hi {name} did you manage to take a look at the race weekend review I sent over?`,

    "Follow-Up (Review 2 Days) V1": `Hey {name}\nJust checking in - did you get a chance to go through the post-race review I sent over?\nTakes about 5 minutes and shows exactly where the gains are hiding for you.\nLet me know if the link didn't work or if you had any issues with it 👍`,

    "Follow-Up (Review 2 Days) V2": `{name} - wanted to circle back on the race weekend assessment\nMost riders who complete it say the same thing: 'I didn't realise THAT was what was holding me back'\nIf you're still interested, the link's below. If not, no worries - good luck with the rest of the season 👍`,

    // --- STALLED TRAINING NUDGES ---
    "Stalled: Signed In": `Hi {name} I see you signed into the free training but didn't go much further was everything ok with the link and the platform for you?`,

    "Stalled: Day 1 Only": `Hey, {name}, Great work on completing the first day of the free training how was it for you?`,

    "Stalled: Day 2 Only": `Hey {name}, I see you completed the first 2 days of the Free Training but missed the third, is everything ok with the link and platform for you?`,

    "Stalled: Day 3 Only": `Hey {name}, I see you completed the Free Training but haven't booked your free strategy call yet.\nI have a few slots open this week if you want to dial in your plan for the season?`,

    // --- RESCUE DMs ---
    "Rescue: Day 1 Nudge": `Hey {name}! 👋\n\nNoticed you signed up for the Podium Contenders Blueprint but haven't done Day 1 yet.\n\nThe 7 Biggest Mistakes assessment only takes 20 mins and riders are telling me it's been a game-changer for understanding where they're leaving time on track.\n\nYour link's still active - want me to resend it?\n\nLet me know if you have any questions!`,

    "Rescue: Day 2 Nudge": `Hey {name}!\n\nLoved seeing your Day 1 results - some really interesting patterns there.\n\nDay 2's 5-Pillar Assessment is where it all comes together though. It shows you exactly which areas will give you the biggest gains.\n\nTakes about 15 mins - you ready to dive in?\n\nHere's your link: https://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/`,

    "Rescue: Book Strategy Call": `Hey {name}!\n\nYou've done Day 1 AND Day 2 - that's awesome! You're clearly serious about this.\n\nThe next step is a Strategy Call where we look at your results together and figure out the best path forward for you.\n\nNo pressure, no hard sell - just a real conversation about your racing goals.\n\nI've got some spots open - shall I send the booking link?`
  };

  // Template categories for grouping — Cold Outreach first so it's prominent
  const TEMPLATE_GROUPS = {
    "🏁 COLD OUTREACH": ["Cold Outreach", "Offer Free Training"],
    "🏆 END OF SEASON": ["End of Season", "End of Season (Reply)", "Send Season Review"],
    "REPLIES": ["Great Work (Reply)", "Productive (Reply)", "Tough Weekend (Reply)"],
    "SEND LINKS": ["Send Link (Yes)", "Send Season Review", "Send Blueprint Link"],
    "FOLLOW-UPS": ["Follow-Up (Review Done → Blueprint)", "Follow-Up (Link Sent Check)", "Follow-Up (Review 2 Days) V1", "Follow-Up (Review 2 Days) V2"],
    "STALLED NUDGES": ["Stalled: Review Started", "Stalled: Signed In", "Stalled: Day 1 Only", "Stalled: Day 2 Only", "Stalled: Day 3 Only"],
    "RESCUE DMs": ["Rescue: Day 1 Nudge", "Rescue: Day 2 Nudge", "Rescue: Book Strategy Call"]
  };

  // Map each template to the pipeline stage it should advance the driver to
  // null = no stage change (follow-ups don't advance)
  const TEMPLATE_STAGE_MAP = {
    "Cold Outreach": "Messaged",
    "End of Season": "Messaged",
    "End of Season (Reply)": "Replied",
    "Send Season Review": "Link Sent",
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

  // Templates that should create the rider in the database if they don't exist
  const CREATE_RIDER_TEMPLATES = ["Cold Outreach", "End of Season", "Offer Free Training"];

  // Race weekend outreach — uses saveOutreach (no stage change, panel stays open)
  const RACE_OUTREACH_TEMPLATES = ["Cold Outreach", "End of Season"];

  // Cold outreach message variations — one is picked randomly per click
  const COLD_OUTREACH_VARIATIONS = [
    `Hey {name}, I see you were out at {circuit} at the weekend. How was it for you?`,
    `Hi {name}, I see you were out at {circuit} at the weekend. How did it go?`,
    `Hello {name}, I see you were out at {circuit} at the weekend. How was the weekend?`,
    `Hey {name}, I noticed you were at {circuit} at the weekend. How did you get on?`,
    `Hi {name}, saw you were out at {circuit} at the weekend. How was it?`,
    `Hiya {name}, I see you were racing at {circuit} at the weekend. How did it go for you?`,
  ];

  // End of season outreach variations — uses {championship} placeholder
  const END_OF_SEASON_VARIATIONS = [
    `Hey {name}\n\nI see you competed in the {championship} this season\n\nHow was your season?`,
    `Hey {name}\n\nI see you competed in the {championship} this season\n\nHow did it go?`,
    `Hey {name}\n\nI see you competed in the {championship} this season\n\nHow was it for you?`,
    `Hey {name}\n\nI see you competed in the {championship} this season\n\nHow are you feeling about your performance?`,
    `Hey {name}\n\nI see you competed in the {championship} this season\n\nHow did you get on?`,
  ];

  function getRandomOutreach() {
    return COLD_OUTREACH_VARIATIONS[Math.floor(Math.random() * COLD_OUTREACH_VARIATIONS.length)];
  }

  function getRandomEndOfSeason() {
    return END_OF_SEASON_VARIATIONS[Math.floor(Math.random() * END_OF_SEASON_VARIATIONS.length)];
  }

  // Update rider's pipeline stage via the background service worker.
  // Streamlit needs a full browser page load (WebSocket) to execute its
  // Python script — a simple fetch() just gets the HTML shell. The
  // background worker opens the app in an inactive tab that auto-closes.
  function updateRiderStage(driverName, stageName, createRider = false) {
    if (!stageName || !driverName) return;
    // Prefer the real name from the pipeline/outreach card over social page names
    const bestName = pipelineDriverName || driverName;
    const circuitInput = document.getElementById('ag-circuit-input');
    const circuit = circuitInput ? circuitInput.value.trim() : '';
    sendRuntimeMessage({
      type: 'updateStage',
      driver: bestName,
      stage: stageName,
      createRider: createRider,
      socialUrl: getFbProfileUrl() || '',
      circuit: circuit
    });

    // Also capture and save the conversation when a template is used
    if (isMessengerPage()) {
      setTimeout(() => captureCurrentConversation(), 500);
    }
  }

  // Save outreach WITHOUT changing stage — saves social URL, message text,
  // and creates rider if needed. Stage is set manually from the contact card.
  // Extract the FB profile URL — even when on a Messenger/DM page
  function getFbProfileUrl() {
    const url = window.location.href;
    // Already on a profile page — use directly
    if (isProfilePage()) return url.split('?')[0].split('#')[0];
    // On Messenger page — find the profile link in the header
    const headerLinks = document.querySelectorAll('a[href*="facebook.com/"]');
    for (const a of headerLinks) {
      const href = a.href || '';
      if (href.includes('/messages') || href.includes('/t/') || href.includes('/groups/') ||
        href.includes('/settings') || href.includes('/direct')) continue;
      const m = href.match(/facebook\.com\/(profile\.php\?id=[0-9]+|[A-Za-z0-9._]+)/);
      if (m) {
        const rect = a.getBoundingClientRect();
        // Check header area (full Messenger page) OR bottom area (popup chat)
        const inHeader = rect.top > 0 && rect.top < 150 && rect.width > 15;
        const inPopup = rect.top > window.innerHeight * 0.5 && rect.width > 15;
        if (inHeader || inPopup) {
          return 'https://www.facebook.com/' + m[1];
        }
      }
    }
    return null;
  }

  function saveOutreachToApp(driverName, messageText, templateName, createRider = false) {
    if (!driverName) return;
    // Prefer the real name from the pipeline/outreach card over social page names
    var bestName = pipelineDriverName || driverName;
    // Fix camelCase URL slugs: "TrevorGilreath" -> "Trevor Gilreath"
    if (bestName && !bestName.includes(' ')) {
      var spaced = bestName.replace(/([a-z])([A-Z])/g, '$1 $2');
      if (spaced.includes(' ')) bestName = spaced;
      else if (bestName.includes('.')) bestName = bestName.split('.').map(function (w) { return w.charAt(0).toUpperCase() + w.slice(1); }).join(' ');
      else if (bestName.includes('_')) bestName = bestName.split('_').map(function (w) { return w.charAt(0).toUpperCase() + w.slice(1); }).join(' ');
    }
    const circuitInput = document.getElementById('ag-circuit-input');
    const circuit = circuitInput ? circuitInput.value.trim() : '';
    const platform = window.location.href.includes('instagram.com') ? 'IG' : 'FB';

    // Get the actual profile URL (not the Messenger thread URL)
    const profileUrl = getFbProfileUrl() || '';

    // Get championship from chrome.storage (synced from Streamlit)
    chrome.storage.local.get('ag_championship', (data) => {
      const championship = cleanChampionshipName(data.ag_championship || '');
      sendRuntimeMessage({
        type: 'saveOutreach',
        driver: bestName,
        socialUrl: profileUrl,
        platform: platform,
        message: messageText.substring(0, 1800),
        template: templateName,
        createRider: createRider,
        circuit: circuit,
        championship: championship
      }, (response) => {
        if (response && response.success) {
          showSavedTick();
          console.log('[AG] ✅ Outreach + URL saved for', driverName);
        } else {
          showToast('⚠️ Save may have failed — check app');
          console.warn('[AG] Save response:', response);
        }
      });
    });
  }

  // ── Auto-save social URL when rider detected from pipeline ──────────────
  // Saves the FB/Messenger URL silently. NO tick shown — tick only appears
  // when user takes an explicit action (template click, manual save).
  function autoSaveSocialUrl() {
    const url = window.location.href;
    if (autoSavedUrl === url) return;  // Already saved this URL

    // Only auto-save when user came from the pipeline app (pipelineDriverName set)
    // Don't save URLs just because someone is browsing FB/Messenger
    if (!pipelineDriverName) return;

    const driverName = pipelineDriverName || currentName;
    if (!driverName) return;

    // Wait for DB lookup to finish so we use the real name, not the social username
    // This prevents creating duplicate records
    if (!dbLookupDone) {
      console.log('[AG] Auto-save deferred — waiting for DB lookup');
      return;
    }

    // Final safety check: reject notification text that slipped through
    if (/\b(messaged you|sent you|replied to|liked your|mentioned you|reacted to|is typing|was active|shared a|tagged you|commented on)\b/i.test(driverName)) {
      console.warn('[AG] Blocked auto-save — name looks like notification text:', driverName);
      return;
    }

    autoSavedUrl = url;

    // Save the URL silently via background.js — no visual indicator
    sendRuntimeMessage({
      type: 'saveUrl',
      driver: driverName,
      fbUrl: url
    }, (response) => {
      if (response && response.success) {
        console.log('[AG] Auto-saved FB URL for', driverName, '(silent)');
      }
    });
  }

  function showSavedTick() {
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;

    // Add or update tick badge
    let tick = btn.querySelector('.ag-saved-tick');
    if (!tick) {
      tick = document.createElement('div');
      tick.className = 'ag-saved-tick';
      btn.appendChild(tick);
    }
    tick.textContent = '✓';
    tick.style.background = '#22c55e';
    tick.style.display = 'flex';

    // Pulse animation
    tick.style.animation = 'none';
    tick.offsetHeight; // Trigger reflow
    tick.style.animation = 'ag-tick-pulse 0.4s ease-out';
  }

  function showStageBadge(stageName) {
    // Show a distinct blue badge for stage moves (vs green for URL saves)
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
    if (document.getElementById('antigravity-styles')) return;
    const style = document.createElement('style');
    style.id = 'antigravity-styles';
    style.textContent = `
      #${BUTTON_ID} {
        position: fixed;
        top: 24px;
        right: 24px;
        width: 56px;
        height: 56px;
        border-radius: 50%;
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        border: none;
        cursor: pointer;
        z-index: 100000;
        box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35);
        display: flex;
        align-items: center;
        justify-content: center;
        transition: all 0.2s ease;
        padding: 0;
      }
      #${BUTTON_ID}:hover {
        transform: scale(1.1);
        box-shadow: 0 6px 20px rgba(16, 185, 129, 0.5);
      }
      #${BUTTON_ID}:active { transform: scale(0.95); }
      #${BUTTON_ID} svg { width: 28px; height: 28px; fill: white; }
      #${BUTTON_ID}.no-rider {
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
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        flex-shrink: 0;
      }
      .ag-panel-header h2 {
        margin: 0 0 2px 0;
        font-size: 15px;
        font-weight: 700;
        color: white;
      }
      .ag-panel-header .ag-subtitle {
        font-size: 11px;
        color: rgba(255,255,255,0.8);
        margin: 0;
        display: none;
      }
      .ag-circuit-row {
        margin-top: 10px;
        display: flex;
        align-items: center;
        gap: 8px;
      }
      .ag-circuit-row label {
        font-size: 11px;
        font-weight: 600;
        color: rgba(255,255,255,0.9);
        white-space: nowrap;
      }
      #ag-circuit-input {
        flex: 1;
        padding: 5px 10px;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.3);
        background: rgba(255,255,255,0.15);
        color: white;
        font-size: 13px;
        font-family: inherit;
        outline: none;
      }
      #ag-circuit-input::placeholder { color: rgba(255,255,255,0.5); }
      #ag-circuit-input:focus { border-color: white; background: rgba(255,255,255,0.25); }

      .ag-panel-body {
        flex: 1;
        overflow-y: auto;
        padding: 8px 12px;
      }
      .ag-panel-body::-webkit-scrollbar { width: 6px; }
      .ag-panel-body::-webkit-scrollbar-track { background: transparent; }
      .ag-panel-body::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }

      .ag-group-title {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #10b981;
        padding: 12px 0 6px;
        margin: 0;
        border-bottom: 1px solid #1f2937;
      }

      .ag-template-btn {
        display: block;
        width: 100%;
        text-align: left;
        padding: 7px 10px;
        margin: 3px 0;
        background: #1f2937;
        border: 1px solid #374151;
        border-radius: 6px;
        color: #e5e7eb;
        font-size: 12px;
        cursor: pointer;
        transition: all 0.15s ease;
        font-family: inherit;
        line-height: 1.2;
      }
      .ag-template-btn:hover {
        background: #374151;
        border-color: #10b981;
        color: white;
      }
      .ag-template-btn .ag-tmpl-name {
        font-weight: 600;
        display: block;
        margin-bottom: 2px;
        color: #10b981;
        font-size: 12px;
      }
      .ag-template-btn.ag-outreach {
        background: linear-gradient(135deg, #064e3b 0%, #065f46 100%);
        border-color: #10b981;
        padding: 9px 10px;
      }
      .ag-template-btn.ag-outreach:hover {
        background: linear-gradient(135deg, #065f46 0%, #047857 100%);
        border-color: #34d399;
      }
      .ag-template-btn.ag-outreach .ag-tmpl-name {
        font-size: 12px;
        color: #34d399;
      }
      .ag-template-btn .ag-tmpl-preview {
        color: #9ca3af;
        font-size: 11px;
        display: block;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .ag-copied-toast {
        position: fixed;
        top: 90px;
        right: 380px;
        background: #10b981;
        color: white;
        padding: 10px 20px;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 600;
        z-index: 100001;
        opacity: 0;
        transition: opacity 0.2s ease;
        pointer-events: none;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      }
      .ag-copied-toast.show { opacity: 1; }

      .ag-cross-links {
        display: flex;
        gap: 6px;
        margin-top: 8px;
        flex-wrap: wrap;
      }
      .ag-cross-link {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 5px 10px;
        border-radius: 6px;
        font-size: 11px;
        font-weight: 600;
        text-decoration: none;
        cursor: pointer;
        transition: all 0.2s ease;
        border: 1px solid;
      }
      .ag-cross-link.ag-link-ig {
        background: linear-gradient(135deg, #833AB4 0%, #E1306C 50%, #F77737 100%);
        color: white;
        border-color: transparent;
      }
      .ag-cross-link.ag-link-ig:hover {
        transform: scale(1.05);
        box-shadow: 0 2px 8px rgba(225, 48, 108, 0.4);
      }
      .ag-cross-link.ag-link-fb {
        background: #1877F2;
        color: white;
        border-color: transparent;
      }
      .ag-cross-link.ag-link-fb:hover {
        transform: scale(1.05);
        box-shadow: 0 2px 8px rgba(24, 119, 242, 0.4);
      }
      .ag-cross-link.ag-link-app {
        background: #059669;
        color: white;
        border-color: transparent;
      }
      .ag-cross-link.ag-link-app:hover {
        transform: scale(1.05);
        box-shadow: 0 2px 8px rgba(5, 150, 105, 0.4);
      }

      .ag-panel-footer {
        padding: 8px 12px;
        border-top: 1px solid #1f2937;
        flex-shrink: 0;
      }
      .ag-panel-footer a {
        display: block;
        text-align: center;
        color: #10b981;
        text-decoration: none;
        font-size: 11px;
        padding: 6px;
        border: 1px solid #10b981;
        border-radius: 6px;
        transition: all 0.15s;
      }
      .ag-panel-footer a:hover {
        background: rgba(16, 185, 129, 0.1);
      }
    `;
    document.head.appendChild(style);
  }

  // ── Name Extraction ─────────────────────────────────────────────────────
  // PRIORITY: If we have a driver name from the pipeline app (via search button),
  // use that instead of scraping the Facebook page title (which can be a racing
  // page name like "AidanHancock91 is with Hook and Cleaver Ranch").
  // Pipeline name is ONLY used on profile/search pages, not in Messenger
  // (where the chat header name is always the person's real name).

  const SKIP_NAMES = ['facebook', 'messenger', 'new message', 'chats', 'search messenger',
    'search facebook', 'active', 'end-to-end', 'media and files', 'privacy',
    'mute', 'search', 'learn more', 'encrypted', 'messages',
    'communities', 'groups', 'unread', 'all',
    'team boyd racing', 'marketplace', 'gaming', 'watch',
    'home', 'video', 'notifications', 'personal details', 'friends',
    'photos', 'about', 'highlights', 'posts', 'add friend', 'message',
    'check-ins', 'more', 'filters', 'reels', 'family', 'male', 'female',
    'collection', 'see all', 'edit profile', 'log in', 'sign up',
    'verified', 'link', 'follow', 'following', 'like', 'share', 'comment',
    'sportsperson', 'athlete', 'public figure'];

  function isValidRiderName(text) {
    if (!text || text.length < 2 || text.length > 50) return false;
    const lower = text.toLowerCase().trim();
    // Must contain at least one letter
    if (!/[a-zA-Z]/.test(text)) return false;
    // Must look like a name: at least 2 parts OR be reasonably long
    // Skip known UI words
    if (SKIP_NAMES.some(w => lower === w || lower.startsWith(w + ' '))) return false;
    // Skip things that look like UI (timestamps, status text, etc.)
    if (/^\d/.test(text)) return false; // Starts with number
    if (/^(you:|reply\?|·|\d+[hmd]\b)/i.test(text)) return false;
    // Skip notification/activity text (e.g. "Clint messaged you", "Nick sent you a message")
    if (/\b(messaged you|sent you|replied to|liked your|mentioned you|reacted to|started a|is typing|is online|was active|shared a|tagged you|commented on|invited you|accepted your|poked you)\b/i.test(lower)) return false;
    return true;
  }

  function isProfilePage() {
    const url = window.location.href;
    // Profile pages: facebook.com/username or facebook.com/profile.php?id=
    // Also handles sub-paths like /username/about, /username/friends, etc.
    // NOT messages, groups, marketplace, watch, gaming, etc.
    const nonProfilePaths = ['/messages', '/t/', '/groups/', '/marketplace', '/watch',
      '/gaming', '/settings', '/notifications', '/events/', '/pages/',
      '/reel/', '/reels/', '/stories/', '/login', '/recover/', '/help/',
      '/search/', '/bookmarks', '/saved', '/feeds/', '/fundraisers/',
      '/professional_dashboard', '/ads/', '/privacy/'];
    if (nonProfilePaths.some(p => url.includes(p))) return false;
    // Must be on facebook.com (not messenger.com)
    if (!url.includes('facebook.com/')) return false;
    // Match facebook.com/username (with optional sub-paths and query strings)
    // Also match facebook.com/profile.php?id=
    return /facebook\.com\/(profile\.php\?id=|[A-Za-z0-9._]+)/.test(url);
  }

  function isMessengerPage() {
    const url = window.location.href;
    return url.includes('/messages') || url.includes('/t/') || url.includes('messenger.com');
  }

  // Check if user is inside an ACTIVE conversation (not just the inbox list)
  // /messages/t/123456 = active conversation
  // /messages or /messages/inbox = just the list view — NO name extraction here
  function isActiveConversation() {
    const url = window.location.href;
    // On messenger.com: must have /t/ segment (active thread)
    if (url.includes('messenger.com')) return url.includes('/t/');
    // On facebook.com/messages: must have /t/ (specific thread open)
    if (url.includes('/messages')) return url.includes('/t/');
    // Not a messenger page at all
    return false;
  }

  // ── Messenger Popup Chat Detection ──────────────────────────────────────
  // Detects floating Messenger popup chats at the bottom of any Facebook page
  // (Groups, Feed, Events, etc.) and extracts the contact name from the header.
  function extractPopupChatName() {
    // Find Messenger popup chat windows — fixed-position containers at bottom
    // of screen with a chat input (textbox) inside
    const allTextboxes = document.querySelectorAll('div[role="textbox"][contenteditable="true"]');
    for (const textbox of allTextboxes) {
      const label = (textbox.getAttribute('aria-label') || '').toLowerCase();
      if (label !== 'message' && label !== 'message…' && label !== 'message...' &&
        label !== 'type a message' && label !== 'type a message…' && label !== 'aa') continue;

      // Walk up to find a fixed-position popup container
      let container = textbox.parentElement;
      let depth = 0;
      while (container && depth < 30) {
        const style = window.getComputedStyle(container);
        if (style.position === 'fixed') break;
        container = container.parentElement;
        depth++;
      }
      if (!container || depth >= 30) continue;

      const containerRect = container.getBoundingClientRect();
      // Popup chats are at the bottom of the screen
      if (containerRect.bottom < window.innerHeight * 0.5) continue;

      // Found a fixed chat popup — look for the name in the header area
      // Strategy CP1: Profile links in the popup header
      const links = container.querySelectorAll('a[href*="facebook.com/"], a[href^="/"]');
      for (const link of links) {
        const href = link.href || link.getAttribute('href') || '';
        if (href.includes('/messages') || href.includes('/t/') || href.includes('/groups/') ||
          href.includes('/settings') || href.includes('/notifications')) continue;
        // Must be a profile-like URL
        if (!href.match(/facebook\.com\/[A-Za-z0-9._]+/) && !href.match(/^\/[A-Za-z0-9._]+\/?$/)) continue;

        const rect = link.getBoundingClientRect();
        // Must be in the top portion of the popup (header area)
        if (rect.top < containerRect.top || rect.top > containerRect.top + 60) continue;
        if (rect.width < 15) continue;

        const spans = link.querySelectorAll('span');
        for (const span of spans) {
          const text = span.textContent.trim();
          if (isValidRiderName(text)) return text;
        }
        const linkText = link.textContent.trim();
        if (isValidRiderName(linkText)) return linkText;
      }

      // Strategy CP2: Bold/prominent spans in the popup header
      const popupSpans = container.querySelectorAll('span[dir="auto"], span');
      for (const span of popupSpans) {
        const rect = span.getBoundingClientRect();
        // Must be in the top area of the popup (header)
        if (rect.top < containerRect.top || rect.top > containerRect.top + 55) continue;
        if (rect.width < 20) continue;

        const style = window.getComputedStyle(span);
        const fontWeight = parseInt(style.fontWeight) || 400;
        const fontSize = parseFloat(style.fontSize);
        // Chat header names are typically bold (600+) and decent size (13px+)
        if (fontWeight >= 600 && fontSize >= 13) {
          const text = span.textContent.trim();
          if (isValidRiderName(text)) return text;
        }
      }

      // Strategy CP3: Any heading elements in the popup header area
      const headings = container.querySelectorAll('h1, h2, h3, h4, [role="heading"]');
      for (const h of headings) {
        const rect = h.getBoundingClientRect();
        if (rect.top < containerRect.top || rect.top > containerRect.top + 60) continue;
        const text = h.textContent.trim();
        if (isValidRiderName(text)) return text;
      }
    }
    return null;
  }

  function extractConversationName() {
    const DEBUG = false; // flip to true in DevTools: document.querySelector('#antigravity-rider-btn').__AG_DEBUG = true
    function dbg(...args) { if (DEBUG) console.log('[AG]', ...args); }

    // TOP PRIORITY: If we have a pipeline driver name and we're NOT in Messenger,
    // always use it. This covers profile pages, racing pages, and search results.
    if (pipelineDriverName && !isMessengerPage()) {
      dbg('Pipeline name (top):', pipelineDriverName);
      return cleanName(pipelineDriverName);
    }

    // ── PROFILE PAGE: Name is in the big h1 heading ──
    if (isProfilePage()) {
      dbg('Profile page detected:', window.location.href);

      // Strategy P0: Pipeline driver name — ALWAYS preferred on profile pages.
      // The pipeline app passes the real name via #ag_driver= hash when user
      // clicks search buttons. This avoids picking up racing page names like
      // "AidanHancock91 is with Hook and Cleaver Ranch".
      if (pipelineDriverName) {
        dbg('P0 pipeline name:', pipelineDriverName);
        return cleanName(pipelineDriverName);
      }

      // Strategy P1: Page title — most reliable. "FirstName LastName | Facebook"
      const title = document.title;
      if (title) {
        const cleaned = title.replace(/^\(\d+\)\s*/, '');
        const separators = ['|', ' - ', '•', '·', ' — '];
        for (const sep of separators) {
          if (cleaned.includes(sep)) {
            const name = cleaned.split(sep)[0].trim();
            // Strip parenthetical like "(1 mutual friend)" etc
            const stripped = name.replace(/\(.*?\)/g, '').trim();
            if (isValidRiderName(stripped) && stripped.includes(' ')) {
              dbg('P1 title match:', stripped);
              return cleanName(stripped);
            }
          }
        }
        // Title might just be the name with no separator
        const plainTitle = cleaned.replace(/\(.*?\)/g, '').trim();
        if (isValidRiderName(plainTitle) && plainTitle.includes(' ')) {
          dbg('P1 plain title match:', plainTitle);
          return cleanName(plainTitle);
        }
      }

      // Strategy P2: h1 > span (modern layout)
      const h1Elements = document.querySelectorAll('h1');
      for (const h1 of h1Elements) {
        const spans = h1.querySelectorAll('span');
        for (const span of spans) {
          const text = span.textContent.trim();
          if (isValidRiderName(text) && text.includes(' ')) {
            dbg('P2 h1>span match:', text);
            return cleanName(text);
          }
        }
        const text = h1.textContent.trim();
        if (isValidRiderName(text) && text.includes(' ')) {
          dbg('P2 h1 text match:', text);
          return cleanName(text);
        }
      }

      // Strategy P3: [role="heading"] elements in upper page area
      const headings = document.querySelectorAll('[role="heading"]');
      for (const heading of headings) {
        const rect = heading.getBoundingClientRect();
        if (rect.top > 50 && rect.top < 500 && rect.width > 50) {
          const spans = heading.querySelectorAll('span');
          for (const span of spans) {
            const text = span.textContent.trim();
            if (isValidRiderName(text) && text.includes(' ')) {
              dbg('P3 heading>span match:', text);
              return cleanName(text);
            }
          }
          const text = heading.textContent.trim();
          if (isValidRiderName(text) && text.includes(' ')) {
            dbg('P3 heading text match:', text);
            return cleanName(text);
          }
        }
      }

      // Strategy P4: h2 elements in upper page area
      const h2Elements = document.querySelectorAll('h2');
      for (const h2 of h2Elements) {
        const rect = h2.getBoundingClientRect();
        if (rect.top > 50 && rect.top < 500) {
          const spans = h2.querySelectorAll('span');
          for (const span of spans) {
            const text = span.textContent.trim();
            if (isValidRiderName(text) && text.includes(' ')) {
              dbg('P4 h2>span match:', text);
              return cleanName(text);
            }
          }
          const text = h2.textContent.trim();
          if (isValidRiderName(text) && text.includes(' ')) {
            dbg('P4 h2 text match:', text);
            return cleanName(text);
          }
        }
      }

      // Strategy P5: og:title meta tag
      const ogTitle = document.querySelector('meta[property="og:title"]');
      if (ogTitle) {
        const text = ogTitle.content.trim().replace(/\(.*?\)/g, '').trim();
        if (isValidRiderName(text) && text.includes(' ')) {
          dbg('P5 og:title match:', text);
          return cleanName(text);
        }
      }

      // Strategy P6: Prominent span[dir="auto"] in the profile header area
      const profileSpans = document.querySelectorAll('span[dir="auto"]');
      for (const span of profileSpans) {
        const rect = span.getBoundingClientRect();
        if (rect.top < 50 || rect.top > 450 || rect.width < 30) continue;
        const style = window.getComputedStyle(span);
        const fontSize = parseFloat(style.fontSize);
        if (fontSize >= 18) {
          const text = span.textContent.trim();
          if (isValidRiderName(text) && text.includes(' ')) {
            dbg('P6 large span match:', text, 'fontSize:', fontSize);
            return cleanName(text);
          }
        }
      }

      dbg('Profile page: no name found');
      return null;
    }

    // ── MESSENGER POPUP: Detect chat popups floating on any Facebook page ──
    // Facebook's Messenger popup appears at bottom-right as a fixed-position
    // chat window. This works on Group pages, Feed, Events, etc.
    const popupName = extractPopupChatName();
    if (popupName) {
      dbg('Popup chat name:', popupName);
      return cleanName(popupName);
    }

    // ── MESSENGER PAGE: Chat header strategies ──
    // CRITICAL: Only extract names if we're inside an active conversation.
    // On the inbox list (/messages), heading text like "Clint messaged you"
    // would be scraped as driver names.
    if (!isActiveConversation()) {
      dbg('Messenger inbox (no active conversation) — skipping name extraction');
      return null;
    }
    dbg('Active conversation detected:', window.location.href);

    // Strategy M1: Page title — most reliable cross-platform
    // Formats: "Messenger | FirstName LastName", "(1) FirstName LastName | Facebook",
    //          "FirstName LastName - Messenger", "Messenger · FirstName LastName"
    const title = document.title;
    if (title) {
      const cleaned = title.replace(/^\(\d+\)\s*/, '');
      dbg('M1 title:', cleaned);
      const separators = ['|', ' - ', '•', '·', ' — '];
      for (const sep of separators) {
        if (cleaned.includes(sep)) {
          const parts = cleaned.split(sep);
          // Try all parts, preferring the non-"Messenger"/"Facebook" ones
          for (const part of [...parts].reverse()) {
            const name = part.trim();
            if (isValidRiderName(name)) {
              dbg('M1 title match:', name);
              return cleanName(name);
            }
          }
        }
      }
      // Title might just be the person's name (no separator)
      const plainTitle = cleaned.trim();
      if (isValidRiderName(plainTitle) && plainTitle.includes(' ')) {
        dbg('M1 plain title match:', plainTitle);
        return cleanName(plainTitle);
      }
    }

    // Strategy M2: Header links pointing to facebook.com profiles
    const headerLinks = document.querySelectorAll('a[href*="facebook.com/"], a[href^="/"]');
    for (const a of headerLinks) {
      const href = a.href || a.getAttribute('href') || '';
      if (href.includes('/messages') || href.includes('/t/') || href.includes('/groups/') ||
        href.includes('/settings') || href.includes('/notifications')) continue;
      // Must be a profile-like URL
      if (!href.match(/facebook\.com\/[A-Za-z0-9._]+/) && !href.match(/^\/[A-Za-z0-9._]+\/?$/)) continue;

      const rect = a.getBoundingClientRect();
      if (rect.top > 0 && rect.top < 150 && rect.width > 15) {
        const spans = a.querySelectorAll('span');
        for (const span of spans) {
          const text = span.textContent.trim();
          if (isValidRiderName(text)) {
            dbg('M2 link>span match:', text, 'href:', href);
            return cleanName(text);
          }
        }
        const linkText = a.textContent.trim();
        if (isValidRiderName(linkText)) {
          dbg('M2 link text match:', linkText);
          return cleanName(linkText);
        }
      }
    }

    // Strategy M3: Heading elements in the chat header area (expanded range)
    const headings = document.querySelectorAll('h1, h2, h3, h4, [role="heading"]');
    for (const h of headings) {
      const rect = h.getBoundingClientRect();
      if (rect.top > 0 && rect.top < 150 && rect.width > 30) {
        const spans = h.querySelectorAll('span');
        for (const span of spans) {
          const text = span.textContent.trim();
          if (isValidRiderName(text)) {
            dbg('M3 heading>span match:', text);
            return cleanName(text);
          }
        }
        const text = h.textContent.trim();
        if (isValidRiderName(text)) {
          dbg('M3 heading text match:', text);
          return cleanName(text);
        }
      }
    }

    // Strategy M4: Bold/prominent span[dir="auto"] in the header area
    // Facebook uses span[dir="auto"] for most text, with font-weight for names
    const headerSpans = document.querySelectorAll('span[dir="auto"]');
    for (const span of headerSpans) {
      const rect = span.getBoundingClientRect();
      if (rect.top < 0 || rect.top > 120 || rect.width < 20) continue;
      const style = window.getComputedStyle(span);
      const fontWeight = parseInt(style.fontWeight) || 400;
      const fontSize = parseFloat(style.fontSize);
      // Chat header names are typically bold (600-700+) and decent size (14px+)
      if (fontWeight >= 600 && fontSize >= 13) {
        const text = span.textContent.trim();
        if (isValidRiderName(text)) {
          dbg('M4 bold span match:', text, 'weight:', fontWeight, 'size:', fontSize);
          return cleanName(text);
        }
      }
    }

    // Strategy M5: Any clickable element in the header with a valid name
    // Facebook wraps the contact name in various interactive elements
    const clickables = document.querySelectorAll(
      'a[role="link"], div[role="button"], a[role="button"], [tabindex="0"]'
    );
    for (const el of clickables) {
      const rect = el.getBoundingClientRect();
      if (rect.top < 0 || rect.top > 120 || rect.width < 20 || rect.width > 400) continue;
      // Check child spans
      const spans = el.querySelectorAll('span');
      for (const span of spans) {
        const text = span.textContent.trim();
        if (isValidRiderName(text) && text.includes(' ')) {
          dbg('M5 clickable>span match:', text);
          return cleanName(text);
        }
      }
    }

    // Strategy M6: The right-side panel (complementary) shows the name
    const complementary = document.querySelector('div[role="complementary"]');
    if (complementary) {
      const compElements = complementary.querySelectorAll('h1, h2, h3, span[dir="auto"], [role="heading"]');
      for (const el of compElements) {
        const text = el.textContent.trim();
        if (isValidRiderName(text)) {
          dbg('M6 complementary match:', text);
          return cleanName(text);
        }
      }
    }

    // Strategy M7: aria-label on the main chat area or thread container
    // Facebook sometimes puts the contact name in aria-label attributes
    const ariaContainers = document.querySelectorAll(
      '[aria-label][role="main"], [aria-label][role="region"], [aria-label][role="complementary"]'
    );
    for (const el of ariaContainers) {
      const label = el.getAttribute('aria-label') || '';
      // aria-label might be "Conversation with FirstName LastName" or just "FirstName LastName"
      const stripped = label
        .replace(/^(conversation with|chat with|messaging|messages?)\s*/i, '')
        .replace(/\(.*?\)/g, '')
        .trim();
      if (isValidRiderName(stripped) && stripped.includes(' ')) {
        dbg('M7 aria-label match:', stripped, 'from:', label);
        return cleanName(stripped);
      }
    }

    dbg('Messenger page: no name found');
    return null;
  }

  function cleanName(raw) {
    if (!raw) return null;
    return raw
      .replace(/[\u{1F000}-\u{1FFFF}]/gu, '')
      .replace(/[\u{2600}-\u{27BF}]/gu, '')
      .replace(/[\u{FE00}-\u{FEFF}]/gu, '')
      .replace(/\(.*?\)/g, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function getFirstName(fullName) {
    if (!fullName) return 'Mate';
    // Normal case: "Trevor Gilreath" -> "Trevor"
    if (fullName.includes(' ')) return fullName.split(' ')[0];
    // CamelCase URL slug: "TrevorGilreath" -> "Trevor"
    var camelSplit = fullName.replace(/([a-z])([A-Z])/g, '$1 $2');
    if (camelSplit.includes(' ')) return camelSplit.split(' ')[0];
    // Dot/underscore separated: "trevor.gilreath" or "trevor_gilreath"
    if (fullName.includes('.')) return fullName.split('.')[0].replace(/^./, function (c) { return c.toUpperCase(); });
    if (fullName.includes('_')) return fullName.split('_')[0].replace(/^./, function (c) { return c.toUpperCase(); });
    // Single word — capitalize and return
    return fullName.charAt(0).toUpperCase() + fullName.slice(1);
  }

  // ── Fix name in AI message: replace username/wrong name with real DB name ──
  // Handles cases where the Streamlit app baked the social username into the message
  let _originalScrapedName = null;
  function fixNameInMessage(msg) {
    const realFirst = getFirstName(currentName);
    // Step 1: replace {name} placeholder (normal case)
    let fixed = msg.replace(/\{name\}/g, realFirst);
    // Step 2: if the original scraped name differs from the DB name,
    // find and replace it in the message text
    if (_originalScrapedName && _originalScrapedName !== currentName) {
      const scraped = _originalScrapedName.trim();
      const scrapedNoDots = scraped.replace(/[_.]/g, '').replace(/^\d+/, '');
      const scrapedParts = scraped.replace(/[_.]/g, ' ').replace(/^\d+/, '').trim();
      const scrapedFirst = scrapedParts.split(' ')[0];
      const scrapedTrunc = scrapedNoDots.length > 7 ? scrapedNoDots.substring(0, 7) : '';
      const variants = [scraped, scrapedNoDots, scrapedParts, scrapedFirst];
      if (scrapedTrunc) variants.push(scrapedTrunc);
      const unique = [...new Set(variants.filter(v => v && v.length >= 3))];
      unique.sort((a, b) => b.length - a.length);
      for (const variant of unique) {
        const re = new RegExp(variant.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
        if (re.test(fixed)) {
          fixed = fixed.replace(re, realFirst);
          break;
        }
      }
    }
    return fixed;
  }

  // ── Paste into Messenger input ──────────────────────────────────────────
  // ONLY target the actual Messenger popup/chat input.
  // Never target comment boxes, wall post boxes, or any other textbox.
  function findMessageInput() {
    const allTextboxes = document.querySelectorAll('div[role="textbox"][contenteditable="true"]');

    // Strategy 1: Direct aria-label match for Messenger chat input
    for (const el of allTextboxes) {
      const label = (el.getAttribute('aria-label') || '').toLowerCase();
      if (label === 'message' || label === 'message…' || label === 'message...' ||
        label === 'type a message' || label === 'type a message…' || label === 'aa') {
        return el;
      }
    }

    // Strategy 2: Textbox inside a FIXED-position container at bottom of viewport.
    // Messenger popups are position:fixed chat windows at the bottom of the screen.
    // Comment boxes and post boxes scroll with the page — they're NOT fixed.
    for (const el of allTextboxes) {
      const rect = el.getBoundingClientRect();
      if (rect.bottom > window.innerHeight - 150 && rect.width > 30 && rect.width < 500) {
        let parent = el.parentElement;
        let depth = 0;
        while (parent && depth < 25) {
          const style = window.getComputedStyle(parent);
          if (style.position === 'fixed') return el;
          parent = parent.parentElement;
          depth++;
        }
      }
    }

    // Strategy 3: On full messenger.com page, any non-search textbox is the chat input
    if (window.location.hostname.includes('messenger.com')) {
      for (const el of allTextboxes) {
        const label = (el.getAttribute('aria-label') || '').toLowerCase();
        if (!label.includes('search')) return el;
      }
    }

    return null;
  }

  // ── Auto-add friend on Facebook profile pages ──────────────────────────
  // Finds and clicks the "Add friend" button. Silently skips if not on a
  // profile page, already friends, or button can't be found.
  function clickAddFriendButton(retryCount = 0) {
    if (!isProfilePage()) {
      console.log('[AG] Not a profile page — skipping Add Friend');
      return false;
    }

    // Strategy 1: aria-label match (most reliable)
    const ariaBtn = document.querySelector(
      '[aria-label="Add friend"], [aria-label="Add Friend"]'
    );
    if (ariaBtn) {
      ariaBtn.click();
      console.log('[AG] ✅ Add Friend clicked (aria-label)');
      showToast('👋 Friend request sent!', '#3b82f6');
      return true;
    }

    // Strategy 2: Span text inside buttons — Facebook wraps button labels in <span>
    const allSpans = document.querySelectorAll(
      'div[role="button"] span, a[role="button"] span, button span'
    );
    for (const span of allSpans) {
      const text = span.textContent.trim();
      if (/^Add [Ff]riend$/i.test(text)) {
        const btn = span.closest('[role="button"], button');
        if (btn) {
          const rect = btn.getBoundingClientRect();
          if (rect.top > 50 && rect.top < 900 && rect.width > 20) {
            btn.click();
            console.log('[AG] ✅ Add Friend clicked (span text)');
            showToast('👋 Friend request sent!', '#3b82f6');
            return true;
          }
        }
      }
    }

    // Strategy 3: Direct button text content match
    const candidates = document.querySelectorAll(
      'div[role="button"], a[role="button"], button'
    );
    for (const el of candidates) {
      const text = el.textContent.trim();
      if (/^Add [Ff]riend$/i.test(text)) {
        const rect = el.getBoundingClientRect();
        if (rect.top > 50 && rect.top < 900 && rect.width > 20) {
          el.click();
          console.log('[AG] ✅ Add Friend clicked (text match)');
          showToast('👋 Friend request sent!', '#3b82f6');
          return true;
        }
      }
    }

    // Strategy 4: SVG icon match (person-plus icon)
    const allButtons = document.querySelectorAll('div[role="button"], a[role="button"]');
    for (const el of allButtons) {
      const svg = el.querySelector('svg');
      if (!svg) continue;
      const paths = svg.querySelectorAll('path');
      const svgText = svg.innerHTML || '';
      if (paths.length >= 2 && (svgText.includes('M18') || svgText.includes('M15'))) {
        const parent = el.closest('[role="button"]') || el;
        const rect = parent.getBoundingClientRect();
        if (rect.top > 50 && rect.top < 900) {
          const nearbyText = parent.textContent.trim().toLowerCase();
          if (nearbyText.includes('add') || nearbyText === '' || nearbyText.includes('friend')) {
            parent.click();
            console.log('[AG] ✅ Add Friend clicked (SVG icon match)');
            showToast('👋 Friend request sent!', '#3b82f6');
            return true;
          }
        }
      }
    }

    // Retry — button may not be loaded yet on slow profiles
    if (retryCount < 2) {
      console.log(`[AG] Add Friend not found — retry ${retryCount + 1} in 2s`);
      setTimeout(() => clickAddFriendButton(retryCount + 1), 2000);
      return false;
    }

    console.log('[AG] Add Friend button not found — may already be friends or a Page');
    return false;
  }

  async function openMessengerPopup() {
    // On a profile page, click the "Message" button to open a chat popup
    if (!isProfilePage()) return false;

    // Strategy 1: Find the "Message" button by aria-label
    let messageBtn = document.querySelector('a[aria-label="Message"], div[aria-label="Message"], a[aria-label="message"]');

    // Strategy 2: Find by text content — look for buttons/links with "Message"
    if (!messageBtn) {
      const candidates = document.querySelectorAll('a[role="button"], div[role="button"], button, a[role="link"]');
      for (const el of candidates) {
        const text = el.textContent.trim();
        if (/^Messages?$/i.test(text)) {
          const rect = el.getBoundingClientRect();
          // Profile action buttons are roughly in the middle vertical area
          if (rect.top > 150 && rect.top < 800 && rect.width > 20) {
            messageBtn = el;
            break;
          }
        }
      }
    }

    // Strategy 3: Look for the blue message button icon near "Friends" button
    if (!messageBtn) {
      const friendsBtn = Array.from(document.querySelectorAll('span')).find(s => s.textContent.trim() === 'Friends');
      if (friendsBtn) {
        // The Message button is usually right next to Friends
        const parent = friendsBtn.closest('div');
        if (parent) {
          const nearby = parent.parentElement?.querySelectorAll('a[role="button"], div[role="button"]') || [];
          for (const el of nearby) {
            if (el !== friendsBtn.closest('[role="button"]')) {
              const rect = el.getBoundingClientRect();
              if (rect.width > 20 && rect.width < 200) {
                messageBtn = el;
                break;
              }
            }
          }
        }
      }
    }

    if (!messageBtn) return false;

    messageBtn.click();

    // Wait for messenger popup input to appear
    return new Promise(resolve => {
      let attempts = 0;
      const check = setInterval(() => {
        attempts++;
        const input = findMessageInput();
        if (input) {
          clearInterval(check);
          resolve(true);
        } else if (attempts > 40) { // 4 seconds max
          clearInterval(check);
          resolve(false);
        }
      }, 100);
    });
  }

  async function pasteIntoMessenger(text) {
    let inputEl = findMessageInput();

    // If no input found, try opening a messenger popup on profile pages
    if (!inputEl && isProfilePage()) {
      const opened = await openMessengerPopup();
      if (opened) {
        inputEl = findMessageInput();
      }
    }

    if (inputEl) {
      inputEl.focus();
      // Small delay to let React register the focus
      await new Promise(r => setTimeout(r, 200));

      // Use clipboard API + paste event — this is the most reliable way
      // to get line breaks into React-controlled contenteditable inputs
      try {
        await navigator.clipboard.writeText(text);

        // Simulate Ctrl+V paste via a paste event with clipboard data
        const pasteEvent = new ClipboardEvent('paste', {
          bubbles: true,
          cancelable: true,
          clipboardData: new DataTransfer()
        });
        pasteEvent.clipboardData.setData('text/plain', text);
        inputEl.dispatchEvent(pasteEvent);

        // If the paste event was cancelled (React handled it), great
        // If not, try execCommand as fallback
        if (!pasteEvent.defaultPrevented) {
          document.execCommand('insertText', false, text);
        }

        return true;
      } catch (e) {
        // Fallback: just copy to clipboard
        return false;
      }
    }

    // Always copy to clipboard as ultimate fallback
    try { await navigator.clipboard.writeText(text); } catch (e) { }
    return false;
  }

  // ── Message Extraction ─────────────────────────────────────────────────
  // Scrapes the last ~10 visible messages from the active Messenger conversation
  // to send to the Streamlit app as a conversation log reference.

  let lastCapturedName = null;   // Track which rider we last captured for
  let captureTimeout = null;     // Debounce capture

  function extractConversationMessages() {
    const messages = [];
    const mainArea = document.querySelector('div[role="main"]');
    if (!mainArea) return messages;

    const mainRect = mainArea.getBoundingClientRect();
    const midX = mainRect.left + mainRect.width / 2;

    // Strategy: find all div[dir="auto"] inside the message area.
    // These contain the actual message text. We determine sender by
    // checking if the message bubble is left-aligned (them) or right-aligned (you).
    const candidates = mainArea.querySelectorAll('div[dir="auto"]');
    const skipTexts = new Set([
      'Reply', 'React', 'More', 'Unsend', 'Forward', 'Pin', 'Copy',
      'Sent', 'Delivered', 'Seen', 'Active now', 'Messenger',
      'Like', 'Love', 'Haha', 'Wow', 'Sad', 'Angry',
      'Replying to', 'Forwarded', 'View more', 'See more',
      'Write a reply…', 'Type a message…', 'Aa', 'GIF',
      'Press enter to send', 'Open camera', 'Attach a file',
      'Search Messenger', 'New message'
    ]);

    for (const el of candidates) {
      const text = el.textContent.trim();
      if (!text || text.length < 1 || text.length > 500) continue;
      if (skipTexts.has(text)) continue;

      // Skip tiny invisible or off-screen elements
      const rect = el.getBoundingClientRect();
      if (rect.width < 30 || rect.height < 12) continue;

      // Skip elements not in the visible chat scroll area
      // (header area is top ~80px, input area is bottom ~80px)
      if (rect.top < mainRect.top + 60 || rect.bottom > mainRect.bottom - 40) continue;

      // Walk up to find the message bubble container (usually has border-radius + padding)
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

      // Additional skip: timestamp/status text (often small, grey)
      const style = window.getComputedStyle(el);
      const fontSize = parseFloat(style.fontSize);
      if (fontSize < 11) continue;  // Skip tiny meta text

      // Avoid duplicate captures (same text from nested elements)
      const lastMsg = messages[messages.length - 1];
      if (lastMsg && lastMsg.text === text.substring(0, 150)) continue;

      messages.push({
        sender: isYou ? 'You' : 'Them',
        text: text.substring(0, 150)
      });
    }

    // Return last 10 messages
    return messages.slice(-10);
  }

  // Format messages for URL transport: compact pipe-separated format
  // "You>Hey mate||Them>Great thanks||You>Check the link"
  function formatMessagesForTransport(messages) {
    if (!messages || messages.length === 0) return '';
    return messages
      .map(m => `${m.sender === 'You' ? 'Y' : 'T'}>${m.text.replace(/\|/g, ' ').replace(/\n/g, ' ')}`)
      .join('||')
      .substring(0, 1800);  // Stay within URL length limits
  }

  // Send captured messages to the Streamlit app via background worker
  function saveConversationToApp(driverName, messages) {
    if (!driverName || !messages || messages.length === 0) return;
    const formatted = formatMessagesForTransport(messages);
    if (!formatted) return;

    // Also store locally for quick reference
    try {
      const key = `ag_msgs_${driverName.toLowerCase().replace(/\s+/g, '_')}`;
      chrome.storage.local.set({ [key]: { messages, timestamp: Date.now() } });
    } catch (e) { /* ignore storage errors */ }

    sendRuntimeMessage({
      type: 'saveConversation',
      driver: driverName,
      messages: formatted,
      platform: window.location.hostname.includes('instagram') ? 'IG' : 'FB'
    });
  }

  // Auto-capture when switching conversations
  function captureCurrentConversation() {
    if (!currentName || !isMessengerPage()) return;
    const messages = extractConversationMessages();
    if (messages.length >= 2) {  // Only save if there's a meaningful thread
      saveConversationToApp(currentName, messages);
    }
  }

  function showToast(message, color) {
    let toast = document.querySelector('.ag-copied-toast');
    if (!toast) {
      toast = document.createElement('div');
      toast.className = 'ag-copied-toast';
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.style.background = color || '#10b981';
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2500);
  }

  // ── Build the panel ─────────────────────────────────────────────────────
  function createPanel() {
    if (document.getElementById(PANEL_ID)) return;

    const panel = document.createElement('div');
    panel.id = PANEL_ID;

    // NOTE: firstName is computed dynamically at click time via getFirstName(currentName)
    // Do NOT use a const here — the DB lookup may update pipelineDriverName/currentName after panel creation
    // Stash the original scraped name so fixNameInMessage can find/replace it later
    _originalScrapedName = currentName;

    // Header
    const header = document.createElement('div');
    header.className = 'ag-panel-header';
    header.innerHTML = `
      <h2>🏁 ${currentName || 'Rider'}</h2>
      <p class="ag-subtitle">Pick a reply template below</p>
      <div class="ag-circuit-row">
        <label>Circuit / Track:</label>
        <input type="text" id="ag-circuit-input" placeholder="e.g. Brands Hatch" autocomplete="off" />
      </div>
    `;

    // Pre-populate circuit from storage, and save on change
    const circuitEl = header.querySelector('#ag-circuit-input');
    if (circuitEl) {
      chrome.storage.local.get('ag_circuit', (data) => {
        if (data.ag_circuit && !circuitEl.value) circuitEl.value = data.ag_circuit;
      });
      circuitEl.addEventListener('input', () => {
        const val = circuitEl.value.trim();
        if (val) chrome.storage.local.set({ ag_circuit: val });
      });
    }

    // ── DATABASE STATUS BANNER (URL lookup) ──
    const dbBanner = document.createElement('div');
    dbBanner.className = 'ag-db-status';
    dbBanner.style.cssText = 'padding:8px 12px;border-radius:8px;font-size:12px;font-weight:600;margin:0 0 8px 0;text-align:center;transition:all 0.3s ease;';
    dbBanner.style.background = '#374151';
    dbBanner.style.color = '#9ca3af';
    dbBanner.textContent = '🔍 Checking database...';
    header.appendChild(dbBanner);

    // Perform fast URL lookup against Airtable
    const detectedFbUrl = getFbProfileUrl();
    console.log('[AG] FB Panel opened — checking URL in database:', detectedFbUrl, 'driver:', currentName);
    if (detectedFbUrl) {
      sendRuntimeMessage({
        type: 'lookupUrl',
        fbUrl: detectedFbUrl,
        driverName: currentName || ''
      }, (response) => {
        if (response && response.found) {
          const stageBadge = response.stage || 'Contact';
          const champText = response.championship ? ` • ${response.championship}` : '';
          const name = (response.fullName || '').trim();
          const totalMatches = response.totalMatches || 1;

          // Smart overlap check — compare the ORIGINAL scraped name (from the page)
          // with the DB name to detect URL clashes (wrong URL on wrong record)
          const scrapedName = currentName || '';  // This is still the original page name
          const currentLower = scrapedName.toLowerCase().replace(/[_.\-]/g, ' ').replace(/^\d+/, '');
          const matchedLower = name.toLowerCase().replace(/[_.\-]/g, ' ');
          const nameTokens = currentLower.split(/\s+/).filter(t => t.length > 1);
          const matchTokens = matchedLower.split(/\s+/).filter(t => t.length > 1);
          const exactOverlap = nameTokens.some(t => matchTokens.includes(t));
          const substringOverlap = nameTokens.some(nt =>
            matchTokens.some(mt => (nt.length >= 4 && mt.length >= 3 && (nt.includes(mt) || mt.includes(nt))))
          );
          const joinedCurrent = currentLower.replace(/\s+/g, '');
          const concatOverlap = matchTokens.length >= 2 && matchTokens.every(mt =>
            joinedCurrent.includes(mt.substring(0, Math.min(mt.length, 4)))
          );
          const hasOverlap = exactOverlap || substringOverlap || concatOverlap;

          if (!hasOverlap && scrapedName && name) {
            // URL CLASH — this URL belongs to a DIFFERENT person's record
            // Do NOT update currentName — keep the original scraped name
            dbBanner.style.background = '#92400e';
            dbBanner.style.color = '#fde68a';
            dbBanner.innerHTML = `⚠️ URL CLASH: This URL is on <strong>${name}</strong>'s record [${stageBadge}]` +
              (totalMatches > 1 ? `<br>📋 ${totalMatches} records share this URL` : '');
            console.log('[AG] URL CLASH: page shows "' + scrapedName + '" but URL belongs to "' + name + '"');
          } else {
            // Same person — update name from DB (only if app didn't set it)
            if (name && name.includes(' ') && !_nameSetByApp) {
              pipelineDriverName = name;
              currentName = name;
              const h2 = header.querySelector('h2');
              if (h2) h2.textContent = `🏁 ${name}`;
              console.log('[AG] Using DB name for messages:', name);
            } else if (_nameSetByApp) {
              console.log('[AG] App name locked: "' + pipelineDriverName + '" — DB confirmed: "' + name + '"');
            }
            dbBanner.style.background = '#065f46';
            dbBanner.style.color = '#d1fae5';
            let html = `✅ IN DATABASE: <strong>${name || currentName}</strong> [${stageBadge}]${champText}`;
            if (totalMatches > 1) {
              html += `<br><span style="font-size:11px;opacity:0.8">⚠️ ${totalMatches} records share this URL</span>`;
            }
            dbBanner.innerHTML = html;
          }

          dbLookupDone = true;
          // Only trigger auto-save if same person (overlap)
          if (hasOverlap) setTimeout(() => autoSaveSocialUrl(), 200);

          // ── CROSS-PLATFORM LINKS ──
          // Show clickable links to message rider on their other platform profiles
          const linksDiv = document.createElement('div');
          linksDiv.className = 'ag-cross-links';
          // Use currentName (updated only if same person) not the raw DB name
          const driverNameForHash = currentName || '';

          // IG link (we're on Facebook, so IG is the "other" platform)
          // Appends #ag_driver=Name so the IG content script picks up the correct driver name instantly
          if (response.igUrl) {
            const igUrl = response.igUrl.split('#')[0] + '#ag_driver=' + encodeURIComponent(driverNameForHash);
            const igLink = document.createElement('a');
            igLink.className = 'ag-cross-link ag-link-ig';
            igLink.href = igUrl;
            igLink.target = '_blank';
            igLink.rel = 'noopener';
            igLink.textContent = '📨 Message on Instagram';
            // Also store driver name in chrome.storage for the other platform
            igLink.addEventListener('click', () => {
              try { chrome.storage.local.set({ ag_current_driver: driverNameForHash }); } catch (e) { }
            });
            linksDiv.appendChild(igLink);
          }

          // FB Messenger link (if they have a different FB URL, e.g. from a profile page)
          if (response.fbUrl && response.fbUrl !== detectedFbUrl) {
            const fbLink = document.createElement('a');
            fbLink.className = 'ag-cross-link ag-link-fb';
            fbLink.href = response.fbUrl.split('#')[0] + '#ag_driver=' + encodeURIComponent(driverNameForHash);
            fbLink.target = '_blank';
            fbLink.rel = 'noopener';
            fbLink.textContent = '💬 Open FB Profile';
            linksDiv.appendChild(fbLink);
          }

          if (linksDiv.children.length > 0) {
            dbBanner.after(linksDiv);
          }

          console.log('[AG] FB URL match found:', response);
        } else {
          dbBanner.style.background = '#1e3a5f';
          dbBanner.style.color = '#93c5fd';
          dbBanner.textContent = '🆕 NEW PROSPECT — not yet in database';
          dbLookupDone = true;
          setTimeout(() => autoSaveSocialUrl(), 200);
          console.log('[AG] No FB URL match:', response);
        }
      });
    } else {
      dbBanner.style.background = '#78350f';
      dbBanner.style.color = '#fde68a';
      dbBanner.textContent = '⚠️ Could not detect FB URL — save will use name match';
      dbLookupDone = true;
    }

    // Body with templates
    const body = document.createElement('div');
    body.className = 'ag-panel-body';

    // --- CAPTURE THREAD BUTTON (prominent, at the top) ---
    const captureBtn = document.createElement('button');
    captureBtn.className = 'ag-template-btn ag-capture-btn';
    captureBtn.innerHTML = `
      <span class="ag-tmpl-name">📤 Send Thread to App</span>
      <span class="ag-tmpl-preview">Captures this conversation and sends it to the Pipeline App for AI follow-up</span>
    `;
    captureBtn.style.cssText = 'background:#1877F2;color:white;border:none;margin-bottom:12px;';
    captureBtn.addEventListener('click', () => {
      if (!currentName || !isMessengerPage()) {
        showToast('Open a conversation first');
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




    // --- AI RACE OUTREACH MESSAGE (synced from webapp) ---
    const aiMsgContainer = document.createElement('div');
    aiMsgContainer.id = 'ag-ai-outreach-container';
    aiMsgContainer.style.display = 'none';

    const aiBtn = document.createElement('button');
    aiBtn.className = 'ag-template-btn ag-outreach';
    aiBtn.style.cssText = 'background:#059669;color:white;border:none;margin-bottom:12px;';
    aiBtn.innerHTML = `
      <span class="ag-tmpl-name">🤖 AI Race Outreach</span>
      <span class="ag-tmpl-preview">Loading from app...</span>
    `;
    aiMsgContainer.appendChild(aiBtn);
    body.appendChild(aiMsgContainer);

    let currentAiTemplate = '';

    function updateAiButton(template) {
      currentAiTemplate = template;
      const preview = fixNameInMessage(template).replace(/\n/g, ' ').substring(0, 80);
      const previewEl = aiBtn.querySelector('.ag-tmpl-preview');
      if (previewEl) previewEl.textContent = preview + (template.length > 80 ? '...' : '');
      aiMsgContainer.style.display = 'block';
    }

    function getFallbackAiMessage() {
      const firstName = getFirstName(pipelineDriverName || currentName);
      const circuitInput = document.getElementById('ag-circuit-input');
      const circuit = circuitInput ? circuitInput.value.trim() : '';
      const templates = [
        `Hey ${firstName}, saw you were out at ${circuit || 'the weekend'}. How's the season going for you?`,
        `Hey ${firstName}, looks like you had a solid weekend${circuit ? ' at ' + circuit : ''}. How's the car feeling?`,
        `Hey ${firstName}, hope the weekend went well${circuit ? ' at ' + circuit : ''}. How's the season shaping up for you?`,
      ];
      return templates[Math.floor(Math.random() * templates.length)];
    }

    function loadAiMessage() {
      try {
        chrome.storage.local.get(['ag_ai_messages', 'ag_ai_outreach_msg', 'ag_current_driver'], (data) => {
          const driverName = pipelineDriverName || (data && data.ag_current_driver) || currentName || '';
          const msgs = (data && data.ag_ai_messages) || {};

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

          if (!template && data && data.ag_ai_outreach_msg) {
            template = data.ag_ai_outreach_msg;
          }

          if (template) {
            updateAiButton(template.replace(/\\n/g, '\n'));
          } else {
            updateAiButton(getFallbackAiMessage());
          }
        });
      } catch (e) {
        console.warn('[AG] Could not load AI message from storage:', e.message);
        updateAiButton(getFallbackAiMessage());
      }
    }
    loadAiMessage();

    try {
      chrome.storage.onChanged.addListener((changes) => {
        if (changes.ag_ai_messages || changes.ag_ai_outreach_msg || changes.ag_current_driver) {
          loadAiMessage();
        }
      });
    } catch (e) { /* ignore */ }

    aiBtn.addEventListener('click', async () => {
      if (!currentAiTemplate) { showToast('No AI message — open app first'); return; }
      const msg = fixNameInMessage(currentAiTemplate);
      const pasted = await pasteIntoMessenger(msg);
      saveOutreachToApp(currentName, msg, 'AI Race Outreach', true);
      if (pasted) {
        showToast('🤖 AI message pasted!');
      } else {
        try {
          await navigator.clipboard.writeText(msg);
          showToast('📋 AI message copied!');
        } catch (e) {
          showToast('Could not paste — copy manually');
        }
      }
      setTimeout(() => clickAddFriendButton(), 1000);
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
        btn.className = CREATE_RIDER_TEMPLATES.includes(key)
          ? 'ag-template-btn ag-outreach'
          : 'ag-template-btn';

        // Generate proper preview for random templates instead of showing placeholder
        const firstName = getFirstName(pipelineDriverName || currentName);
        let previewText = template;
        if (template.includes('__RANDOM_OUTREACH__')) {
          previewText = getRandomOutreach();
        } else if (template.includes('__RANDOM_END_OF_SEASON__')) {
          previewText = getRandomEndOfSeason();
        }
        const preview = previewText.replace(/\{name\}/g, firstName).replace(/\{circuit\}/g, '___').replace(/\{championship\}/g, '(championship)').substring(0, 80);

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
          // Get championship + driver name from chrome.storage (synced from Streamlit app)
          let championship = '';
          let appDriverName = '';
          try {
            const data = await new Promise(r => chrome.storage.local.get(['ag_championship', 'ag_current_driver'], r));
            championship = cleanChampionshipName(data.ag_championship || '');
            appDriverName = (data.ag_current_driver || '').trim();
          } catch (e) { /* ignore */ }
          // Expand random templates FIRST
          let tmpl = template.includes('__RANDOM_OUTREACH__') ? getRandomOutreach()
            : template.includes('__RANDOM_END_OF_SEASON__') ? getRandomEndOfSeason()
              : template;
          // NOW check for championship (after expansion so {championship} is in tmpl)
          if (tmpl.includes('{championship}') && !championship) {
            showToast('⚠️ Set championship in the pipeline app first');
            return;
          }
          const bestName = pipelineDriverName || appDriverName || currentName;
          const msg = tmpl.replace(/\{name\}/g, getFirstName(bestName))
            .replace(/\{circuit\}/g, circuit)
            .replace(/\{championship\}/g, championship);
          const pasted = await pasteIntoMessenger(msg);
          const stage = TEMPLATE_STAGE_MAP[key];
          const shouldCreate = CREATE_RIDER_TEMPLATES.includes(key);
          const isOutreach = RACE_OUTREACH_TEMPLATES.includes(key);

          if (isOutreach) {
            // OUTREACH: save URL + message but do NOT set stage. Panel stays open.
            saveOutreachToApp(currentName, msg, key, shouldCreate);
            if (pasted) {
              showToast('📤 Pasted & saved — URL + message recorded');
            } else {
              await navigator.clipboard.writeText(msg);
              showToast('📤 Copied & saved — URL + message recorded');
            }
            // Auto-send friend request after cold outreach (profile pages only)
            setTimeout(() => clickAddFriendButton(), 1000);
            // Panel stays open so user can send from another platform
          } else {
            // NON-OUTREACH: set stage immediately + close panel
            if (pasted) {
              if (stage) {
                updateRiderStage(currentName, stage, shouldCreate);
                showStageBadge(stage);
              } else {
                showToast('Pasted — review & hit send');
              }
            } else {
              await navigator.clipboard.writeText(msg);
              if (stage) {
                updateRiderStage(currentName, stage, shouldCreate);
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

    // Footer
    const footer = document.createElement('div');
    footer.className = 'ag-panel-footer';
    const riderParam = encodeURIComponent(currentName || '');
    footer.innerHTML = `<a href="${APP_URL}?rider=${riderParam}&tab=dashboard" target="_blank">Open full card in Pipeline App →</a>`;

    panel.appendChild(header);
    panel.appendChild(body);
    panel.appendChild(footer);
    document.body.appendChild(panel);
  }

  // ── Update panel content when rider changes ─────────────────────────────
  function updatePanel() {
    const panel = document.getElementById(PANEL_ID);
    if (!panel) return;

    const firstName = getFirstName(currentName);

    // Update header name
    const h2 = panel.querySelector('.ag-panel-header h2');
    if (h2) h2.innerHTML = `🏁 ${currentName || 'Rider'}`;

    // Update footer link
    const link = panel.querySelector('.ag-panel-footer a');
    if (link) {
      const riderParam = encodeURIComponent(currentName || '');
      link.href = `${APP_URL}?rider=${riderParam}&tab=dashboard`;
    }

    // Get ONLY the template buttons (skip special buttons: capture, save URL, AI outreach)
    const allBtns = Array.from(panel.querySelectorAll('.ag-template-btn'));
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

        // Replace click handler
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
          let championship = '';
          let appDriverName = '';
          try {
            const data = await new Promise(r => chrome.storage.local.get(['ag_championship', 'ag_current_driver'], r));
            championship = cleanChampionshipName(data.ag_championship || '');
            appDriverName = (data.ag_current_driver || '').trim();
          } catch (e) { /* ignore */ }
          let tmpl = template.includes('__RANDOM_OUTREACH__') ? getRandomOutreach()
            : template.includes('__RANDOM_END_OF_SEASON__') ? getRandomEndOfSeason()
              : template;
          if (tmpl.includes('{championship}') && !championship) {
            showToast('⚠️ Set championship in the pipeline app first');
            return;
          }
          const bestName = pipelineDriverName || appDriverName || currentName;
          const msg = tmpl.replace(/\{name\}/g, getFirstName(bestName))
            .replace(/\{circuit\}/g, circuit)
            .replace(/\{championship\}/g, championship);
          const pasted = await pasteIntoMessenger(msg);
          const stage = TEMPLATE_STAGE_MAP[key];
          const shouldCreate = CREATE_RIDER_TEMPLATES.includes(key);
          const isOutreach = RACE_OUTREACH_TEMPLATES.includes(key);

          if (isOutreach) {
            saveOutreachToApp(currentName, msg, key, shouldCreate);
            if (pasted) {
              showToast('📤 Pasted & saved — send then close card when done');
            } else {
              await navigator.clipboard.writeText(msg);
              showToast('📤 Copied & saved — paste with Ctrl+V');
            }
            // Auto-send friend request after outreach
            setTimeout(() => clickAddFriendButton(), 1000);
          } else {
            if (pasted) {
              if (stage) {
                updateRiderStage(currentName, stage, shouldCreate);
                showToast(`Pasted — stage → ${stage.replace(/_/g, ' ')}`);
              } else {
                showToast('Pasted — review & hit send');
              }
            } else {
              await navigator.clipboard.writeText(msg);
              if (stage) updateRiderStage(currentName, stage, shouldCreate);
              showToast('Copied — paste with Ctrl+V then hit send');
            }
            if (panelOpen) togglePanel();
          }
        });
        idx++;
      }
    }
  }

  // ── Toggle panel ────────────────────────────────────────────────────────
  function togglePanel() {
    const panel = document.getElementById(PANEL_ID);
    const btn = document.getElementById(BUTTON_ID);
    if (!panel) return;

    panelOpen = !panelOpen;

    if (panelOpen) {
      panel.classList.add('open');
      btn.classList.add('panel-open');
      btn.title = 'Close rider card';
    } else {
      panel.classList.remove('open');
      btn.classList.remove('panel-open');
      btn.title = `Open ${currentName || 'driver'} card`;
    }
  }

  // ── Button Creation ─────────────────────────────────────────────────────
  function createButton() {
    if (document.getElementById(BUTTON_ID)) return;

    const btn = document.createElement('button');
    btn.id = BUTTON_ID;
    btn.title = 'Open rider contact card (drag to move)';
    btn.innerHTML = `<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
    </svg>`;

    // ── Drag-to-move logic ─────────────────────────────────────
    let isDragging = false;
    let dragStartX, dragStartY, btnStartX, btnStartY;
    const DRAG_THRESHOLD = 5; // px — below this is a click, above is a drag

    btn.addEventListener('mousedown', (e) => {
      if (e.button !== 0) return; // left-click only
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
        // Save position
        try {
          chrome.storage.local.set({ ag_btn_pos_fb: { left: btn.style.left, top: btn.style.top } });
        } catch (e) { }
      }
      dragStartX = null;
    });

    // ── Click handler (only fires if not dragging) ─────────────
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (isDragging) { isDragging = false; return; }

      // Refresh name before opening
      const name = extractConversationName();
      if (name) currentName = name;

      if (!currentName) {
        showToast('Open a conversation first');
        return;
      }

      // Create panel if it doesn't exist yet
      if (!document.getElementById(PANEL_ID)) {
        createPanel();
      } else {
        updatePanel();
      }

      togglePanel();
    });

    // Restore saved position
    try {
      chrome.storage.local.get('ag_btn_pos_fb', (data) => {
        if (data.ag_btn_pos_fb) {
          btn.style.right = 'auto';
          btn.style.left = data.ag_btn_pos_fb.left;
          btn.style.top = data.ag_btn_pos_fb.top;
        }
      });
    } catch (e) { }

    document.body.appendChild(btn);
  }

  // ── Update button state on conversation change ──────────────────────────
  let lastAutoOpenName = null; // Track which name we last auto-opened for

  function updateButton() {
    const name = extractConversationName();
    const btn = document.getElementById(BUTTON_ID);

    if (!btn) return;

    if (name && name !== currentName) {
      // Capture messages from the PREVIOUS conversation before switching
      if (currentName && currentName !== name && isMessengerPage()) {
        const prevMessages = extractConversationMessages();
        if (prevMessages.length >= 2) {
          saveConversationToApp(currentName, prevMessages);
        }
      }

      currentName = name;
      btn.classList.remove('no-rider');
      btn.title = `Open ${name}'s contact card`;

      // Auto-save social URL when rider detected (non-messenger pages)
      // Fires on FB search results / profile pages opened from pipeline
      setTimeout(() => autoSaveSocialUrl(), 1500);

      // Schedule a capture of the NEW conversation after it loads
      if (captureTimeout) clearTimeout(captureTimeout);
      captureTimeout = setTimeout(() => {
        captureCurrentConversation();
      }, 3000);  // Wait 3s for messages to render

      // Auto-open panel when a new conversation is detected
      if (name !== lastAutoOpenName) {
        lastAutoOpenName = name;

        if (!document.getElementById(PANEL_ID)) {
          createPanel();
        } else {
          updatePanel();
        }

        // Open the panel automatically
        if (!panelOpen) {
          togglePanel();
        } else {
          // Panel already open — just update content
          updatePanel();
        }
      } else if (panelOpen) {
        updatePanel();
      }
    } else if (!name) {
      currentName = null;
      btn.classList.add('no-rider');
      btn.title = 'Open a conversation first';
    }
  }

  // ── Throttled MutationObserver ──────────────────────────────────────────
  let updateTimeout = null;
  function startThrottledObserver() {
    if (observer) observer.disconnect();

    observer = new MutationObserver(() => {
      if (updateTimeout) clearTimeout(updateTimeout);
      updateTimeout = setTimeout(updateButton, 500);
    });

    // Always observe document.body — Messenger popup chats are appended outside
    // div[role="main"], so we need body-level observation to detect them.
    observer.observe(document.body, { childList: true, subtree: true });
  }

  // ── Initialize ──────────────────────────────────────────────────────────
  function init() {
    injectStyles();
    createButton();

    // Initial name detection — retry a few times since Facebook SPA loads slowly
    currentName = extractConversationName();
    updateButton();

    // Retry name detection after short delays (Facebook renders asynchronously)
    if (!currentName) {
      setTimeout(() => { if (!currentName) updateButton(); }, 2000);
      setTimeout(() => { if (!currentName) updateButton(); }, 4000);
    }

    startThrottledObserver();

    // SPA navigation detection
    let lastUrl = location.href;
    setInterval(() => {
      if (location.href !== lastUrl) {
        lastUrl = location.href;
        // Reset auto-open tracking on navigation
        lastAutoOpenName = null;
        // Clear stale pipeline name BEFORE checking hash — prevents wrong
        // rider showing when navigating between profiles without #ag_driver=
        pipelineDriverName = null;
        // Check for new #ag_driver= hash on navigation (sets pipelineDriverName if present)
        checkHashForRiderName();
        // Clear pipeline name when entering Messenger (use real chat name)
        if (isMessengerPage()) {
          pipelineDriverName = null;
        } else {
          // Auto-save URL on profile/search page navigation
          setTimeout(() => autoSaveSocialUrl(), 2000);
        }
        setTimeout(updateButton, 800);
        setTimeout(updateButton, 2000);
      }
    }, 1000);

    // Also watch for title changes (reliable signal that chat changed)
    let lastTitle = document.title;
    setInterval(() => {
      if (document.title !== lastTitle) {
        lastTitle = document.title;
        setTimeout(updateButton, 300);
      }
    }, 500);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(init, 1500);
  } else {
    window.addEventListener('load', () => setTimeout(init, 1500));
  }
})();

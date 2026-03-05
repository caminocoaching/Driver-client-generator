// Antigravity Driver Connect — Background Service Worker
// Handles save/stage operations by routing through the EXISTING Streamlit tab
// (preferred — instant, uses warm session) or falling back to a new tab.

const APP_URL = 'https://driver-client-generator.streamlit.app';

// ── Airtable API (for direct saves, no Streamlit reload) ──
// Config loaded from airtable_config.js (gitignored, not committed)
try {
  importScripts('airtable_config.js');
} catch (e) {
  console.warn('[AG] airtable_config.js not found — Save URL will fall back to Streamlit');
}
const AIRTABLE_URL = (typeof AIRTABLE_BASE_ID !== 'undefined' && typeof AIRTABLE_TABLE !== 'undefined')
  ? `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(AIRTABLE_TABLE)}`
  : null;

const ACTIVITY_LOG_URL = (typeof AIRTABLE_BASE_ID !== 'undefined')
  ? `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent('Activity Log')}`
  : null;

// ── Audit Logger: records every stage change ──
function logActivity(rider, oldStage, newStage, source) {
  if (!ACTIVITY_LOG_URL || typeof AIRTABLE_API_KEY === 'undefined') return;
  fetch(ACTIVITY_LOG_URL, {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + AIRTABLE_API_KEY,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      fields: {
        'Driver': driver,
        'Action': oldStage === '(new)' ? 'Created' : 'Stage Change',
        'Old Stage': oldStage,
        'New Stage': newStage,
        'Source': source,
        'Timestamp': new Date().toISOString()
      },
      typecast: true
    })
  }).catch(function (e) {
    console.warn('[AG] Audit log failed:', e.message);
  });
}

// ── Persistent Offline Queue: Airtable saves survive outages ──
// Items stay in queue for up to 7 days, retrying with exponential backoff.
// Uses chrome.storage.local (persistent) + chrome.alarms (survives MV3 suspension).
// NEVER loses data — items only leave the queue when successfully saved.
const QUEUE_EXPIRY_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const BACKOFF_SCHEDULE = [30, 60, 120, 300, 600]; // seconds: 30s, 1m, 2m, 5m, 10m

async function enqueueFailedSave(driverName, fields, attempt) {
  attempt = attempt || 0;
  var queue = (await chrome.storage.local.get('ag_retry_queue'))['ag_retry_queue'] || [];
  // De-dupe: skip if same rider + same stage already queued
  var isDupe = queue.some(function (q) { return q.driver === driverName && q.fields && q.fields.Stage === (fields && fields.Stage); });
  if (isDupe) { console.log('[AG] Skipping dupe queue item for ' + driverName); return; }
  queue.push({ driver: driverName, fields: fields, attempt: attempt, ts: Date.now(), lastTry: 0 });
  await chrome.storage.local.set({ ag_retry_queue: queue });
  console.log('[AG] ⚠️ Queued for retry: ' + driverName + ' (attempt ' + (attempt + 1) + ', ' + queue.length + ' in queue)');
  updateBadge();
  // If this is the first failure, try again quickly
  chrome.alarms.create('ag_retry_queue', { delayInMinutes: 0.5, periodInMinutes: 1 });
}

async function processRetryQueue() {
  var data = await chrome.storage.local.get('ag_retry_queue');
  var queue = data['ag_retry_queue'] || [];
  if (!queue.length || !AIRTABLE_URL) { updateBadge(); return; }
  console.log('[AG] Processing retry queue: ' + queue.length + ' items');

  var now = Date.now();
  var remaining = [];
  var anySuccess = false;

  for (var i = 0; i < queue.length; i++) {
    var item = queue[i];

    // Expire items older than 7 days (data is too stale to be useful)
    if (now - item.ts > QUEUE_EXPIRY_MS) {
      console.warn('[AG] ⏰ Expired queue item for ' + item.driver + ' (queued ' + Math.round((now - item.ts) / 3600000) + 'h ago)');
      continue; // drop from queue
    }

    // Backoff: don't retry too soon
    var backoffIdx = Math.min(item.attempt, BACKOFF_SCHEDULE.length - 1);
    var backoffMs = BACKOFF_SCHEDULE[backoffIdx] * 1000;
    if (item.lastTry && (now - item.lastTry) < backoffMs) {
      remaining.push(item); // keep, not time yet
      continue;
    }

    // Attempt the save
    try {
      await saveDriverToAirtable(item.driver, item.fields);
      console.log('[AG] ✅ Retry succeeded for ' + item.driver + ' (attempt ' + (item.attempt + 1) + ')');
      anySuccess = true;
      // Don't add to remaining — it's done!
    } catch (err) {
      item.attempt = (item.attempt || 0) + 1;
      item.lastTry = now;
      remaining.push(item);
      console.warn('[AG] ⚠️ Retry failed for ' + item.driver + ': ' + err.message + ' (attempt ' + item.attempt + ', next in ' + BACKOFF_SCHEDULE[Math.min(item.attempt, BACKOFF_SCHEDULE.length - 1)] + 's)');
    }
  }

  await chrome.storage.local.set({ ag_retry_queue: remaining });
  updateBadge();

  // Adjust alarm frequency based on queue state
  if (remaining.length > 0) {
    // Oldest item determines pace — fresh items retry fast, old items back off
    var oldestAttempt = Math.max.apply(null, remaining.map(function (q) { return q.attempt || 0; }));
    var nextBackoff = BACKOFF_SCHEDULE[Math.min(oldestAttempt, BACKOFF_SCHEDULE.length - 1)];
    var periodMins = Math.max(0.5, nextBackoff / 60);
    chrome.alarms.create('ag_retry_queue', { delayInMinutes: periodMins, periodInMinutes: periodMins });
  } else {
    // Queue empty — slow poll just in case
    chrome.alarms.create('ag_retry_queue', { delayInMinutes: 5, periodInMinutes: 5 });
  }
}

// Badge shows pending save count on extension icon
async function updateBadge() {
  try {
    var data = await chrome.storage.local.get('ag_retry_queue');
    var count = (data['ag_retry_queue'] || []).length;
    if (count > 0) {
      chrome.action.setBadgeText({ text: String(count) });
      chrome.action.setBadgeBackgroundColor({ color: '#FF4444' });
    } else {
      chrome.action.setBadgeText({ text: '' });
    }
  } catch (e) { /* ignore in contexts without action API */ }
}

// chrome.alarms survives MV3 service worker suspension
chrome.alarms.create('ag_retry_queue', { delayInMinutes: 1, periodInMinutes: 5 });
chrome.alarms.onAlarm.addListener(function (alarm) {
  if (alarm.name === 'ag_retry_queue') processRetryQueue();
});
// Also process on startup
processRetryQueue();

async function saveUrlToAirtable(driverName, fbUrl, igUrl) {
  if (!AIRTABLE_URL || typeof AIRTABLE_API_KEY === 'undefined') {
    throw new Error('Airtable config not loaded');
  }
  var headers = {
    'Authorization': 'Bearer ' + AIRTABLE_API_KEY,
    'Content-Type': 'application/json'
  };

  var fields = {};
  // Clean URLs — strip query strings, hashes, and reject DM/thread URLs
  if (fbUrl) {
    fbUrl = fbUrl.split('?')[0].split('#')[0];
    if (!fbUrl.includes('/messages/') && !fbUrl.includes('/t/') && !fbUrl.includes('messenger.com/t/'))
      fields['FB URL'] = fbUrl;
  }
  if (igUrl) {
    igUrl = igUrl.split('?')[0].split('#')[0];
    if (!igUrl.includes('/direct/') && !igUrl.includes('/direct?'))
      fields['IG URL'] = igUrl;
  }
  fields['Last Activity'] = new Date().toISOString().split('T')[0];

  var recordId = null;

  // ── STEP 1: Search by URL first (most reliable — URL is unique) ──
  var urlToSearch = fields['IG URL'] || fields['FB URL'];
  var urlField = fields['IG URL'] ? 'IG URL' : 'FB URL';
  if (urlToSearch) {
    var urlClean = urlToSearch.split('?')[0].replace(/\/+$/, '');
    var urlFormula = 'FIND("' + urlClean.replace(/"/g, '\\"') + '", {' + urlField + '})';
    try {
      var urlRes = await fetch(AIRTABLE_URL + '?filterByFormula=' + encodeURIComponent(urlFormula) + '&maxRecords=3', { headers: headers });
      if (urlRes.ok) {
        var urlData = await urlRes.json();
        if (urlData.records && urlData.records.length > 0) {
          recordId = urlData.records[0].id;
          console.log('[AG] saveUrl: URL match found for "' + driverName + '" -> "' + (urlData.records[0].fields['Full Name'] || '?') + '"');
        }
      }
    } catch (e) {
      console.warn('[AG] saveUrl: URL search failed:', e.message);
    }
  }

  // ── STEP 2: Fall back to name search ──
  if (!recordId) {
    var searchFormula = 'FIND(LOWER("' + driverName.replace(/"/g, '\\"') + '"), LOWER({Full Name}))';
    var searchUrl = AIRTABLE_URL + '?filterByFormula=' + encodeURIComponent(searchFormula) + '&maxRecords=3';
    var searchRes = await fetch(searchUrl, { headers: headers });
    if (!searchRes.ok) throw new Error('Airtable search failed: ' + searchRes.status);
    var searchData = await searchRes.json();
    if (searchData.records && searchData.records.length > 0) {
      recordId = searchData.records[0].id;
    }
  }

  if (recordId) {
    var updateRes = await fetch(AIRTABLE_URL + '/' + recordId, {
      method: 'PATCH', headers: headers,
      body: JSON.stringify({ fields: fields, typecast: true })
    });
    if (!updateRes.ok) {
      var fb = { 'Last Activity': fields['Last Activity'] };
      var fbRes = await fetch(AIRTABLE_URL + '/' + recordId, {
        method: 'PATCH', headers: headers,
        body: JSON.stringify({ fields: fb, typecast: true })
      });
      if (!fbRes.ok) throw new Error('Airtable update failed: ' + updateRes.status);
    }
    return { success: true, method: 'airtable_direct', recordId: recordId };
  } else {
    // ── GUARD: Don't create records with username-looking names ──
    // If the name has no space AND no social URL, it's likely a social media handle
    // and should NOT create a new record. But if we have a URL, allow creation
    // since the URL is a reliable identifier.
    var hasSocialUrl = !!(fields['IG URL'] || fields['FB URL']);
    if (!driverName.includes(' ') && !hasSocialUrl) {
      console.warn('[AG] saveUrl: Refusing to create record with username-like name: "' + driverName + '" and no URL');
      return { success: false, method: 'blocked_username', reason: 'Name looks like a social handle' };
    }
    var np = driverName.trim().split(' ');
    var cf = { 'First Name': np[0] || driverName, 'Last Name': np.slice(1).join(' ') || '' };
    Object.assign(cf, fields);
    var createRes = await fetch(AIRTABLE_URL, {
      method: 'POST', headers: headers,
      body: JSON.stringify({ fields: cf, typecast: true })
    });
    if (!createRes.ok) {
      var fb2 = { 'First Name': cf['First Name'], 'Last Name': cf['Last Name'], 'Last Activity': fields['Last Activity'] };
      var fbRes2 = await fetch(AIRTABLE_URL, {
        method: 'POST', headers: headers, body: JSON.stringify({ fields: fb2, typecast: true })
      });
      if (!fbRes2.ok) throw new Error('Airtable create failed: ' + createRes.status);
    }
    return { success: true, method: 'airtable_created' };
  }
}

var pendingTabs = new Map();

function findStreamlitTab() {
  return new Promise(function (resolve) {
    chrome.tabs.query({}, function (tabs) {
      var match = tabs.find(function (t) { return t.url && t.url.startsWith(APP_URL); });
      resolve(match || null);
    });
  });
}

async function saveViaExistingTab(params) {
  var tab = await findStreamlitTab();
  if (!tab) return false;
  try {
    await chrome.tabs.sendMessage(tab.id, { type: 'ag_inject_params', params: params.toString() });
    return true;
  } catch (e) {
    console.warn('[AG] Could not send to Streamlit tab:', e.message);
    return false;
  }
}

function saveViaNewTab(url, actionInfo, sendResponse) {
  chrome.tabs.create({ url: url, active: false }, function (tab) {
    if (chrome.runtime.lastError) {
      if (sendResponse) sendResponse({ success: false, error: chrome.runtime.lastError.message });
      return;
    }
    pendingTabs.set(tab.id, Object.assign({}, actionInfo, { created: Date.now() }));
    setTimeout(function () {
      chrome.tabs.remove(tab.id).catch(function () { });
      pendingTabs.delete(tab.id);
    }, 45000);
    if (sendResponse) sendResponse({ success: true, tabId: tab.id });
  });
}

function looksLikeUsername(name) {
  if (!name) return true;
  var n = name.trim();
  if (!n.includes(' ')) return true;
  if (/\d{2,}$/.test(n.replace(/\s/g, ''))) return true;
  if (n.includes('_')) return true;
  // Reject notification/activity text scraped as names
  if (/\b(messaged you|sent you|replied to|liked your|mentioned you|reacted to|is typing|is online|was active|shared a|tagged you|commented on|invited you|accepted your|poked you|search results?)\b/i.test(n)) return true;
  // Reject UI artifacts that get scraped as names
  if (/^(verified account|link icon|photo|video|reel|story|suggested|sponsored|people you may know|add friend|message|follow|following|followers|more|see all|view profile|write a comment|like|share|save|report|block|mute|close|send|cancel|done|next|back|search|home|notifications?|settings?|log ?out|sign ?in|sign ?up|create|explore|direct|reels|new post)$/i.test(n)) return true;
  // Reject own account names
  if (/^(caminocoaching|thecaminocoach|_caminocoaching|camino.?coaching|camino.?coach)$/i.test(n.replace(/\s/g, ''))) return true;
  // Reject names that are mostly single-spaced characters (e.g. "K a l l e")
  // Pattern: at least 3 occurrences of "X " where X is a single char
  var spacedOut = n.match(/^(.\s){3,}.?$/);
  if (spacedOut) return true;
  return false;
}

// ── Clean driver name: strip team/brand suffixes, collapse spaced chars ──
function cleanDriverName(name) {
  if (!name) return name;
  var n = name.trim();

  // Collapse spaced-out characters: "K a l l e R o v a n p e r ä" → "Kalle Rovanperä"
  // Detect pattern: sequences of single-char + space
  if (/^(.\s){3,}/.test(n)) {
    // Try collapsing: split into chars, remove single spaces between single chars
    var collapsed = '';
    var i = 0;
    while (i < n.length) {
      collapsed += n[i];
      // If current is a letter and next is space and char after that is a letter
      if (i + 2 < n.length && n[i + 1] === ' ' && /\S/.test(n[i]) && /\S/.test(n[i + 2])) {
        // Check if this is part of a spaced-out pattern (not a real word boundary)
        // Real word boundary: current word has 2+ chars before the space
        // Spaced-out: single char, space, single char, space...
        if (i === 0 || n[i - 1] === ' ') {
          // Skip the space (collapse)
          i += 2;
          continue;
        }
      }
      i++;
    }
    if (collapsed.length < n.length * 0.7) n = collapsed;
  }

  // Strip trailing team/brand/page suffixes
  n = n.replace(/\s+(racing|motorsport|motorsports|race team|racing team|official|page|team)\s*$/i, '');

  // Strip leading/trailing "Jr", "Jnr", "Sr", "II", "III" only if preceded by real name
  // (keep these — they're part of the name, don't strip)

  return n.trim();
}


// ── Common nicknames map (non-prefix cases) ──
var NICKNAME_MAP = {
  'issy': ['isabelle', 'isabella', 'isabel'],
  'izzy': ['isabelle', 'isabella', 'isabel'],
  'bill': ['william'],
  'will': ['william'],
  'bob': ['robert'],
  'rob': ['robert'],
  'dick': ['richard'],
  'rick': ['richard'],
  'rich': ['richard'],
  'ted': ['edward', 'theodore'],
  'ed': ['edward', 'eduardo'],
  'jim': ['james'],
  'jimmy': ['james'],
  'jack': ['john', 'jackson'],
  'chuck': ['charles'],
  'charlie': ['charles'],
  'hank': ['henry'],
  'harry': ['henry', 'harold', 'harrison'],
  'peggy': ['margaret'],
  'meg': ['megan', 'margaret'],
  'tony': ['anthony', 'antonio'],
  'tom': ['thomas'],
  'tommy': ['thomas'],
  'joe': ['joseph', 'josephine'],
  'joey': ['joseph', 'josephine'],
  'ben': ['benjamin'],
  'benny': ['benjamin'],
  'sam': ['samuel', 'samantha'],
  'al': ['alexander', 'albert', 'alan'],
  'alex': ['alexander', 'alexandra', 'alejandro'],
  'andy': ['andrew', 'andreas'],
  'drew': ['andrew'],
  'matt': ['matthew', 'matthias'],
  'pat': ['patrick', 'patricia'],
  'kat': ['katherine', 'katrina', 'katelyn'],
  'kate': ['katherine', 'katrina', 'katelyn'],
  'katie': ['katherine', 'katrina', 'katelyn'],
  'liz': ['elizabeth'],
  'beth': ['elizabeth', 'bethany'],
  'betty': ['elizabeth'],
  'jen': ['jennifer', 'jenny'],
  'jenny': ['jennifer'],
  'becky': ['rebecca'],
  'bec': ['rebecca'],
  'nick': ['nicholas', 'nicolas'],
  'nicky': ['nicholas', 'nicolas'],
  'steve': ['steven', 'stephen'],
  'steph': ['stephanie', 'stephen'],
  'dave': ['david'],
  'dan': ['daniel', 'daniela'],
  'danny': ['daniel'],
  'mike': ['michael'],
  'mikey': ['michael'],
  'chris': ['christopher', 'christian', 'christina', 'christine'],
  'topher': ['christopher'],
  'greg': ['gregory'],
  'pete': ['peter'],
  'fred': ['frederick', 'alfred'],
  'gus': ['august', 'augustus', 'angus'],
  'seb': ['sebastian', 'sebastien'],
  'nico': ['nicolas', 'nicholas'],
  'remy': ['remington'],
  'walt': ['walter'],
  'phil': ['philip', 'phillip'],
  'bri': ['brianna', 'briana'],
  'jess': ['jessica', 'jesse'],
  'josh': ['joshua'],
  'zach': ['zachary'],
  'zak': ['zachary'],
  'nate': ['nathan', 'nathaniel'],
  'jake': ['jacob'],
  'max': ['maxwell', 'maximilian'],
  'ray': ['raymond'],
  'russ': ['russell'],
  'ty': ['tyler', 'tyrel', 'tyrell'],
  'tee': ['teresa', 'theresa'],
  'manny': ['manuel', 'emanuel'],
  'leo': ['leonard', 'leon'],
  'lenny': ['leonard']
};

// ── Helper: check if two first names are a nickname match ──
function isNicknameMatch(name1, name2) {
  var a = name1.toLowerCase().trim();
  var b = name2.toLowerCase().trim();
  if (!a || !b) return false;
  // Exact or prefix match
  if (a === b || a.startsWith(b) || b.startsWith(a)) return true;
  // Check nicknames map (both directions)
  var aNicks = NICKNAME_MAP[a] || [];
  var bNicks = NICKNAME_MAP[b] || [];
  if (aNicks.indexOf(b) >= 0 || bNicks.indexOf(a) >= 0) return true;
  // Check if both map to the same canonical name
  for (var i = 0; i < aNicks.length; i++) {
    if (bNicks.indexOf(aNicks[i]) >= 0) return true;
    if (b.startsWith(aNicks[i]) || aNicks[i].startsWith(b)) return true;
  }
  for (var j = 0; j < bNicks.length; j++) {
    if (a.startsWith(bNicks[j]) || bNicks[j].startsWith(a)) return true;
  }
  return false;
}

// ── Helper: Airtable fetch with error handling ──
async function airtableFetch(formula, headers, maxRecords) {
  maxRecords = maxRecords || 5;
  var url = AIRTABLE_URL + '?filterByFormula=' + encodeURIComponent(formula) + '&maxRecords=' + maxRecords;
  var res = await fetch(url, { headers: headers });
  if (!res.ok) return { records: [] };
  return res.json();
}

// ══════════════════════════════════════════════════════════
// ██  IDENTITY RESOLUTION — find the right Airtable record
// ══════════════════════════════════════════════════════════
//
//  Match priority:
//  1. Exact Full Name
//  2. Also Known As (AKA) field
//  3. First Name + Last Name + Championship (nickname-aware)
//  4. First Name prefix + Last Name (no championship)
//  5. Social URL match
//
async function findDriverRecord(driverName, fields, headers) {
  var nameParts = driverName.trim().split(' ');
  var searchFirst = nameParts[0] || '';
  var searchLast = nameParts.length >= 2 ? nameParts.slice(1).join(' ') : '';
  var championship = fields['Championship'] || '';
  var safeName = driverName.replace(/'/g, "\\'");

  // ── STEP 1: Exact Full Name ──
  var data1 = await airtableFetch("LOWER({Full Name}) = '" + safeName.toLowerCase() + "'", headers, 3);
  if (data1.records && data1.records.length > 0) {
    // If championship provided and multiple exact matches, prefer same championship
    if (championship && data1.records.length > 1) {
      var champExact = data1.records.find(function (r) {
        return (r.fields['Championship'] || '').toLowerCase() === championship.toLowerCase();
      });
      if (champExact) return { records: [champExact], matchType: 'exact+championship' };
    }
    console.log('[AG] Exact match: "' + driverName + '" -> "' + data1.records[0].fields['Full Name'] + '"');
    return { records: [data1.records[0]], matchType: 'exact' };
  }

  // ── STEP 1.5: Substring match (social page names) ──
  // "Cooper Shipman Racing" → find records where Full Name contains "Cooper Shipman"
  // "Chris White Jnr" → find records where Full Name contains "Chris White"
  if (nameParts.length >= 2) {
    // Try first two words as the real name
    var twoWordName = (nameParts[0] + ' ' + nameParts[1]).toLowerCase().replace(/'/g, "\\'");
    if (twoWordName.length >= 5) {
      var data15 = await airtableFetch("FIND('" + twoWordName + "', LOWER({Full Name}))", headers, 5);
      if (data15.records && data15.records.length > 0) {
        // If championship provided, prefer same championship
        if (championship && data15.records.length > 1) {
          var champSub = data15.records.find(function (r) {
            return (r.fields['Championship'] || '').toLowerCase() === championship.toLowerCase();
          });
          if (champSub) {
            console.log('[AG] Substring match (champ): "' + driverName + '" -> "' + champSub.fields['Full Name'] + '"');
            return { records: [champSub], matchType: 'substring+championship' };
          }
        }
        // Take the first match if only one
        if (data15.records.length === 1) {
          console.log('[AG] Substring match: "' + driverName + '" -> "' + data15.records[0].fields['Full Name'] + '"');
          return { records: [data15.records[0]], matchType: 'substring' };
        }
      }
    }

    // Also try: DB record name is a substring of the social name
    // e.g. social="Zach Blincoe Motorsport", DB="Zach Blincoe"
    var safeNameForFind = safeName.toLowerCase();
    var data15b = await airtableFetch("FIND(LOWER({Full Name}), '" + safeNameForFind + "')", headers, 5);
    if (data15b.records && data15b.records.length > 0) {
      if (data15b.records.length === 1) {
        console.log('[AG] Reverse substring: "' + driverName + '" -> "' + data15b.records[0].fields['Full Name'] + '"');
        return { records: [data15b.records[0]], matchType: 'reverse_substring' };
      }
    }
  }

  // ── STEP 2: Also Known As (AKA) ──
  var safeNameLower = safeName.toLowerCase();
  var data2 = await airtableFetch("FIND('" + safeNameLower + "', LOWER({Also Known As}))", headers, 5);
  if (data2.records && data2.records.length > 0) {
    if (championship && data2.records.length > 1) {
      var champAka = data2.records.find(function (r) {
        return (r.fields['Championship'] || '').toLowerCase() === championship.toLowerCase();
      });
      if (champAka) return { records: [champAka], matchType: 'aka+championship' };
    }
    console.log('[AG] AKA match: "' + driverName + '" -> "' + data2.records[0].fields['Full Name'] + '"');
    return { records: [data2.records[0]], matchType: 'aka' };
  }

  // ── STEP 2.5: Abbreviated last name (e.g. "Jojoe D." → "Jojoe Dabalos") ──
  // When social media shows only a last initial, match by first name + that initial
  var cleanLast = searchLast.replace(/\./g, '').trim();
  if (cleanLast && cleanLast.length <= 2 && searchFirst) {
    var safeFirst25 = searchFirst.replace(/'/g, "\\'").toLowerCase();
    var lastInitial = cleanLast[0].toLowerCase();
    var formula25 = championship
      ? "AND(LOWER({First Name}) = '" + safeFirst25 + "', LOWER({Championship}) = '" + championship.replace(/'/g, "\\'").toLowerCase() + "')"
      : "LOWER({First Name}) = '" + safeFirst25 + "'";
    var data25 = await airtableFetch(formula25, headers, 10);
    if (data25.records && data25.records.length > 0) {
      var initialMatch = data25.records.find(function (r) {
        var rLast = (r.fields['Last Name'] || '').toLowerCase();
        return rLast.startsWith(lastInitial);
      });
      if (initialMatch) {
        console.log('[AG] Initial match: "' + driverName + '" -> "' + initialMatch.fields['Full Name'] + '" (last initial "' + lastInitial + '")');
        return { records: [initialMatch], matchType: 'initial' };
      }
    }
  }

  // ── STEP 3: First Name + Last Name + Championship (nickname-aware) ──
  // "Chris Smith" in CVMA matches "Christopher Smith" in CVMA
  // but NOT "John Smith" in CVMA (different first name)
  if (searchLast && championship) {
    var safeLast = searchLast.replace(/'/g, "\\'").toLowerCase();
    var safeChamp = championship.replace(/'/g, "\\'").toLowerCase();
    var formula3 = "AND(LOWER({Last Name}) = '" + safeLast + "', LOWER({Championship}) = '" + safeChamp + "')";
    var data3 = await airtableFetch(formula3, headers, 10);
    if (data3.records && data3.records.length > 0) {
      var champNick = data3.records.find(function (r) {
        return isNicknameMatch(searchFirst, r.fields['First Name'] || '');
      });
      if (champNick) {
        console.log('[AG] Championship match: "' + driverName + '" -> "' + champNick.fields['Full Name'] + '" (' + championship + ')');
        return { records: [champNick], matchType: 'championship+nickname' };
      }
    }
  }

  // ── STEP 4: First Name prefix + Last Name (no championship) ──
  if (searchLast && searchFirst) {
    var safeLast4 = searchLast.replace(/'/g, "\\'").toLowerCase();
    var formula4 = "LOWER({Last Name}) = '" + safeLast4 + "'";
    var data4 = await airtableFetch(formula4, headers, 10);
    if (data4.records && data4.records.length > 0) {
      // Only match if there's exactly ONE nickname match (avoid ambiguity)
      var nickMatches = data4.records.filter(function (r) {
        return isNicknameMatch(searchFirst, r.fields['First Name'] || '');
      });
      if (nickMatches.length === 1) {
        console.log('[AG] Nickname match: "' + driverName + '" -> "' + nickMatches[0].fields['Full Name'] + '"');
        return { records: [nickMatches[0]], matchType: 'nickname' };
      } else if (nickMatches.length > 1) {
        console.log('[AG] Multiple nickname matches for "' + driverName + '" — skipping to avoid wrong match');
      }
    }
  }

  // ── STEP 5: Social URL match ──
  if (fields['IG URL'] || fields['FB URL']) {
    var urlToSearch = fields['IG URL'] || fields['FB URL'];
    var urlField = fields['IG URL'] ? 'IG URL' : 'FB URL';
    var urlClean = urlToSearch.split('?')[0].replace(/\/+$/, '');
    var formula5 = 'FIND("' + urlClean.replace(/"/g, '\\"') + '", {' + urlField + '})';
    try {
      var data5 = await airtableFetch(formula5, headers, 3);
      if (data5.records && data5.records.length > 0) {
        console.log('[AG] URL match: "' + driverName + '" -> "' + data5.records[0].fields['Full Name'] + '" (via ' + urlField + ')');
        return { records: [data5.records[0]], matchType: 'url' };
      }
    } catch (e) {
      console.warn('[AG] URL search failed:', e.message);
    }
  }

  return { records: [], matchType: 'none' };
}

// ══════════════════════════════════════════════
// ██  MAIN: Save/update a rider in Airtable
// ══════════════════════════════════════════════
async function saveDriverToAirtable(driverName, fields) {
  if (!AIRTABLE_URL || typeof AIRTABLE_API_KEY === 'undefined') {
    throw new Error('Airtable config not loaded');
  }

  // ── PRE-CLEAN: reject junk names, clean brand suffixes ──
  var originalName = driverName;
  driverName = cleanDriverName(driverName);

  // After cleaning, re-check if it looks like junk
  if (looksLikeUsername(driverName)) {
    // If we have a social URL, allow it (URL is a reliable identifier)
    var hasSocialUrl = !!(fields['IG URL'] || fields['FB URL']);
    if (!hasSocialUrl) {
      console.warn('[AG] Rejected junk name after cleaning: "' + originalName + '" → "' + driverName + '"');
      return { success: false, method: 'rejected_junk', error: 'Name looks like junk: ' + originalName };
    }
  }

  var headers = {
    'Authorization': 'Bearer ' + AIRTABLE_API_KEY,
    'Content-Type': 'application/json'
  };

  // Find existing rider using identity resolution
  var result = await findDriverRecord(driverName, fields, headers);

  // If no match and name was cleaned, try the original too
  if ((!result.records || result.records.length === 0) && driverName !== originalName) {
    console.log('[AG] No match for cleaned "' + driverName + '", trying original "' + originalName + '"');
    result = await findDriverRecord(originalName, fields, headers);
  }

  var searchData = result;

  // Always set Last Activity
  fields['Last Activity'] = new Date().toISOString().split('T')[0];

  // Filter out null/undefined/empty fields
  var cleanFields = {};
  for (var key in fields) {
    if (fields[key] !== null && fields[key] !== undefined && fields[key] !== '') {
      cleanFields[key] = fields[key];
    }
  }

  // ── URL OWNERSHIP CHECK: Don't save a URL that belongs to a different rider ──
  // This prevents the Jake Farnsworth / Hayden Nelson bug where visiting DMs
  // picks up the wrong person's profile link and saves it to the current rider.
  for (var urlField of ['IG URL', 'FB URL']) {
    if (!cleanFields[urlField]) continue;
    var checkUrl = cleanFields[urlField].split('?')[0].replace(/\/+$/, '');
    try {
      var checkFormula = 'FIND("' + checkUrl.replace(/"/g, '\\"') + '", {' + urlField + '})';
      var checkData = await airtableFetch(checkFormula, headers, 5);
      if (checkData.records && checkData.records.length > 0) {
        // URL exists — check if it belongs to the SAME rider we're saving to
        var matchedRecord = searchData.records && searchData.records.length > 0 ? searchData.records[0] : null;
        var myRecordId = matchedRecord ? matchedRecord.id : null;
        var wrongOwner = checkData.records.find(function (r) { return r.id !== myRecordId; });
        if (wrongOwner) {
          var ownerName = wrongOwner.fields['Full Name'] || wrongOwner.fields['First Name'] || 'unknown';
          console.warn('[AG] ⚠️ URL CLASH: ' + urlField + ' "' + checkUrl + '" already belongs to "' + ownerName + '" (record ' + wrongOwner.id + ') — NOT saving this URL to "' + driverName + '"');
          delete cleanFields[urlField];
        }
      }
    } catch (e) {
      console.warn('[AG] URL ownership check failed:', e.message);
      // On error, still allow the save (don't block on check failure)
    }
  }

  var tryWrite = async function (url, method, data) {
    var res = await fetch(url, { method: method, headers: headers, body: JSON.stringify(data) });
    if (res.ok) return res;
    var err = {};
    try { err = await res.json(); } catch (e) { }
    if (err.error && err.error.type === 'UNKNOWN_FIELD_NAME') {
      var m = err.error.message ? err.error.message.match(/Unknown field name: "(.+?)"/) : null;
      if (m && cleanFields[m[1]]) {
        console.warn('[AG] ⚠️ Airtable rejected field "' + m[1] + '" — removing and retrying');
        delete cleanFields[m[1]];
        return fetch(url, { method: method, headers: headers, body: JSON.stringify({ fields: cleanFields, typecast: true }) });
      }
    }
    return res;
  };

  if (searchData.records && searchData.records.length > 0) {
    var recordId = searchData.records[0].id;
    var existingFields = searchData.records[0].fields || {};
    var oldStage = existingFields.Stage || 'Contact';

    // ── Auto-detect preferred name from social media ──
    // If the social name differs from Airtable, save it as Also Known As
    var dbFullName = (existingFields['Full Name'] || '').trim();
    var socialName = driverName.trim();
    if (dbFullName && socialName && dbFullName.toLowerCase() !== socialName.toLowerCase()) {
      // Social name is different — save as preferred name
      var existingAKA = existingFields['Also Known As'] || '';
      var socialFirst = socialName.split(' ')[0];
      // Only update AKA if this name isn't already there
      if (existingAKA.toLowerCase().indexOf(socialFirst.toLowerCase()) < 0) {
        var newAKA = existingAKA ? existingAKA + ', ' + socialName : socialName;
        cleanFields['Also Known As'] = newAKA;
        console.log('[AG] Preferred name detected: "' + socialFirst + '" (from social) saved as AKA for "' + dbFullName + '"');
      }
    }

    // Stage advance-only: don't downgrade
    if (cleanFields.Stage) {
      var stageOrder = ['Contact', 'Messaged', 'Outreach', 'Replied', 'Link Sent',
        'Blueprint Link Sent', 'Race Weekend', 'Race Review Complete',
        'Blueprint Started', 'Registered', 'Day 1 Complete',
        'Day 2 Complete', 'Day 3 Complete', 'Strategy Call Booked', 'Client'];
      var currIdx = stageOrder.indexOf(existingFields.Stage || '');
      var newIdx = stageOrder.indexOf(cleanFields.Stage);
      if (currIdx >= 0 && newIdx >= 0 && newIdx <= currIdx) {
        console.log('[AG] Stage not downgraded: ' + existingFields.Stage + ' -> ' + cleanFields.Stage + ' (blocked)');
        delete cleanFields.Stage;
      }
    }

    console.log('[AG] PATCH fields:', JSON.stringify(cleanFields));
    var writeRes = await tryWrite(AIRTABLE_URL + '/' + recordId, 'PATCH', { fields: cleanFields, typecast: true });
    if (!writeRes.ok) {
      var errBody = '';
      try { errBody = await writeRes.text(); } catch (e) { }
      throw new Error('Airtable PATCH failed (' + writeRes.status + '): ' + errBody.substring(0, 200));
    }

    // Audit log
    if (cleanFields.Stage && cleanFields.Stage !== oldStage) {
      logActivity(driverName, oldStage, cleanFields.Stage, 'extension');
    }

    // Return the preferred name for outreach use
    var preferredName = socialName.split(' ')[0]; // Use social first name
    console.log('[AG] ✅ Saved: ' + driverName + ' (' + searchData.matchType + ', record ' + recordId + ')');
    return { success: true, method: 'airtable_direct', recordId: recordId, matchType: searchData.matchType, preferredName: preferredName };
  } else {
    // Create new — refuse social handles UNLESS we have a social URL to identify them.
    // On Instagram, names often lack spaces (e.g. "cameronhill") but the IG profile URL
    // is a reliable identifier and should allow record creation.
    var hasSocialUrl = !!(cleanFields['IG URL'] || cleanFields['FB URL']);
    if (looksLikeUsername(driverName) && !hasSocialUrl) {
      console.warn('[AG] Skipped creating "' + driverName + '" — looks like a social handle and no URL');
      return { success: false, method: 'skipped_username', error: 'Name looks like a username — open their profile first so we can capture the URL' };
    }
    var np = driverName.trim().split(' ');
    cleanFields['First Name'] = cleanFields['First Name'] || np[0] || driverName;
    cleanFields['Last Name'] = cleanFields['Last Name'] || np.slice(1).join(' ') || '';
    var createRes = await tryWrite(AIRTABLE_URL, 'POST', { fields: cleanFields, typecast: true });
    if (!createRes.ok) {
      var errBody2 = '';
      try { errBody2 = await createRes.text(); } catch (e) { }
      throw new Error('Airtable POST failed (' + createRes.status + '): ' + errBody2.substring(0, 200));
    }
    logActivity(driverName, '(new)', cleanFields.Stage || 'Contact', 'extension');
    console.log('[AG] ✅ Created: ' + driverName);
    return { success: true, method: 'airtable_created' };
  }
}

// ══════════════════════════════════════════════
// ██  MESSAGE HANDLERS
// ══════════════════════════════════════════════
chrome.runtime.onMessage.addListener(function (message, sender, sendResponse) {
  if (message.type === 'updateStage') {
    var driver = message.driver, stage = message.stage;
    var createRider = message.createRider, socialUrl = message.socialUrl, circuit = message.circuit;
    if (!driver || !stage) {
      sendResponse({ success: false, error: 'Missing rider or stage' });
      return true;
    }
    if (AIRTABLE_URL) {
      var fields = { Stage: stage };
      var today = new Date().toISOString().split('T')[0];
      var stageDateMap = {
        'Messaged': 'Date Messaged', 'Outreach': 'Date Messaged',
        'Replied': 'Date Replied', 'Link Sent': 'Date Link Sent',
        'Blueprint Link Sent': 'Date Link Sent', 'Race Weekend': 'Date Race Review',
        'Race Review Complete': 'Date Race Review', 'Blueprint Started': 'Date Blueprint Started',
        'Registered': 'Date Blueprint Started', 'Day 1 Complete': 'Date Day 1 Assessment ',
        'Day 2 Complete': 'Date Day 2 Assessment ', 'Strategy Call Booked': 'Date Strategy Call',
        'Client': 'Date Sale Closed'
      };
      if (stageDateMap[stage]) fields[stageDateMap[stage]] = today;
      if (socialUrl && !socialUrl.includes('/direct/') && !socialUrl.includes('/direct?') && !socialUrl.includes('/messages/') && !socialUrl.includes('/t/') && !socialUrl.includes('messenger.com/t/')) {
        if (socialUrl.includes('instagram.com')) fields['IG URL'] = socialUrl.split('?')[0].split('#')[0];
        else fields['FB URL'] = socialUrl.split('?')[0].split('#')[0];
      }
      if (circuit) fields['Championship'] = circuit;
      saveDriverToAirtable(driver, fields).then(function (r) {
        // Track stage changes for analytics
        var stageEventMap = {
          'Replied': 'replied', 'Link Sent': 'link_sent',
          'Blueprint Link Sent': 'link_sent', 'Strategy Call Booked': 'call_booked',
          'Client': 'client', 'Sale Closed': 'client'
        };
        if (stageEventMap[stage]) {
          trackOutreachEvent(stageEventMap[stage], { platform: socialUrl && socialUrl.includes('instagram') ? 'IG' : 'FB' });
        }
        sendResponse(r);
      }).catch(function (err) {
        console.warn('[AG] Direct save failed:', err.message);
        enqueueFailedSave(driver, fields);
        sendResponse({ success: false, error: err.message });
      });
    } else {
      var params = new URLSearchParams();
      params.set('driver', driver); params.set('set_stage', stage);
      if (createRider) params.set('create_rider', 'true');
      if (socialUrl) params.set('social_url', socialUrl);
      if (circuit) params.set('circuit', circuit);
      saveViaExistingTab(params).then(function (ok) { sendResponse({ success: ok }); });
    }
    return true;
  }

  if (message.type === 'saveConversation') {
    var rider2 = message.driver, messages = message.messages;
    if (!rider2 || !messages) {
      sendResponse({ success: false, error: 'Missing rider or messages' });
      return true;
    }
    if (AIRTABLE_URL) {
      saveDriverToAirtable(rider2, {}).then(function (r) { sendResponse(r); }).catch(function (err) {
        sendResponse({ success: false, error: err.message });
      });
    } else {
      var params2 = new URLSearchParams();
      params2.set('driver', rider2); params2.set('save_messages', messages);
      if (message.platform) params2.set('msg_platform', message.platform);
      saveViaExistingTab(params2).then(function (ok) { sendResponse({ success: ok }); });
    }
    return true;
  }

  if (message.type === 'saveOutreach') {
    var rider3 = message.driver, socialUrl3 = message.socialUrl;
    var msgText = message.message, template = message.template;
    var createRider3 = message.createRider, circuit3 = message.circuit, championship3 = message.championship;
    if (!rider3) {
      sendResponse({ success: false, error: 'Missing rider' });
      return true;
    }
    if (AIRTABLE_URL) {
      var today3 = new Date().toISOString().split('T')[0];
      var fields3 = { Stage: 'Messaged', 'Date Messaged': today3 };
      if (socialUrl3 && !socialUrl3.includes('/direct/') && !socialUrl3.includes('/direct?') && !socialUrl3.includes('/messages/') && !socialUrl3.includes('/t/') && !socialUrl3.includes('messenger.com/t/')) {
        if (socialUrl3.includes('instagram.com')) fields3['IG URL'] = socialUrl3.split('?')[0].split('#')[0];
        else fields3['FB URL'] = socialUrl3.split('?')[0].split('#')[0];
      }
      if (championship3) fields3['Championship'] = championship3;
      console.log('[AG] saveOutreach socialUrl:', socialUrl3 || '(empty)', 'fields:', JSON.stringify(fields3));
      saveDriverToAirtable(rider3, fields3).then(function (r) {
        // Track for analytics
        trackOutreachEvent('messaged', {
          template: template || 'unknown',
          championship: championship3 || '',
          platform: message.platform || (socialUrl3 && socialUrl3.includes('instagram') ? 'IG' : 'FB')
        });
        sendResponse(r);
      }).catch(function (err) {
        console.warn('[AG] Direct outreach save failed:', err.message);
        enqueueFailedSave(rider3, fields3);
        sendResponse({ success: false, error: err.message });
      });
    } else {
      var params3 = new URLSearchParams();
      params3.set('driver', rider3); params3.set('save_outreach', template || 'outreach');
      if (socialUrl3) params3.set('social_url', socialUrl3);
      if (message.platform) params3.set('platform', message.platform);
      if (createRider3) params3.set('create_rider', 'true');
      if (circuit3) params3.set('circuit', circuit3);
      if (championship3) params3.set('championship', championship3);
      if (msgText) {
        var formatted = 'Y>' + msgText.replace(/\|/g, ' ').replace(/\n/g, ' ');
        params3.set('save_messages', formatted.substring(0, 1800));
      }
      saveViaExistingTab(params3).then(function (ok) { sendResponse({ success: ok }); });
    }
    return true;
  }

  if (message.type === 'saveUrl') {
    var rider4 = message.driver, fbUrl4 = message.fbUrl, igUrl4 = message.igUrl;
    if (!rider4) {
      sendResponse({ success: false, error: 'Missing driver name' });
      return true;
    }
    if (AIRTABLE_URL) {
      saveUrlToAirtable(rider4, fbUrl4, igUrl4).then(function (result) {
        sendResponse(result);
      }).catch(function (err) {
        console.warn('[AG] Airtable direct save failed, falling back:', err.message);
        var params4 = new URLSearchParams();
        params4.set('driver', rider4); params4.set('save_url', 'true');
        if (fbUrl4) params4.set('fb_url', fbUrl4);
        if (igUrl4) params4.set('ig_url', igUrl4);
        saveViaExistingTab(params4).then(function (ok) {
          sendResponse({ success: ok, method: ok ? 'existing_tab' : 'failed' });
        });
      });
    } else {
      var params5 = new URLSearchParams();
      params5.set('driver', rider4); params5.set('save_url', 'true');
      if (fbUrl4) params5.set('fb_url', fbUrl4);
      if (igUrl4) params5.set('ig_url', igUrl4);
      saveViaExistingTab(params5).then(function (ok) {
        sendResponse({ success: ok, method: ok ? 'existing_tab' : 'failed' });
      });
    }
    return true;
  }

  // ══════════════════════════════════════════════
  // ██  FAST URL LOOKUP — check if rider exists by social URL
  // ══════════════════════════════════════════════
  if (message.type === 'lookupUrl') {
    var lookupIgUrl = message.igUrl || '';
    var lookupFbUrl = message.fbUrl || '';
    if (!lookupIgUrl && !lookupFbUrl) {
      sendResponse({ found: false, reason: 'no_url' });
      return true;
    }
    if (!AIRTABLE_URL || typeof AIRTABLE_API_KEY === 'undefined') {
      sendResponse({ found: false, reason: 'no_config' });
      return true;
    }
    var headers = {
      'Authorization': 'Bearer ' + AIRTABLE_API_KEY,
      'Content-Type': 'application/json'
    };

    // Build search: try IG URL first, then FB URL
    var searchUrl = lookupIgUrl || lookupFbUrl;
    var searchField = lookupIgUrl ? 'IG URL' : 'FB URL';
    // Strip trailing slashes and query params for flexible matching
    var urlClean = searchUrl.split('?')[0].replace(/\/+$/, '');
    var driverNameHint = (message.driverName || '').toLowerCase().trim();

    var formula = 'FIND("' + urlClean.replace(/"/g, '\\"') + '", {' + searchField + '})';
    var apiUrl = AIRTABLE_URL + '?filterByFormula=' + encodeURIComponent(formula) + '&maxRecords=10&fields%5B%5D=Full+Name&fields%5B%5D=First+Name&fields%5B%5D=Last+Name&fields%5B%5D=Stage&fields%5B%5D=Championship&fields%5B%5D=IG+URL&fields%5B%5D=FB+URL&fields%5B%5D=Last+Activity&fields%5B%5D=Date+Messaged';

    fetch(apiUrl, { headers: headers })
      .then(function (res) { return res.json(); })
      .then(function (data) {
        if (data.records && data.records.length > 0) {
          // Build all matches
          var allMatches = data.records.map(function (rec) {
            var f = rec.fields || {};
            return {
              recordId: rec.id,
              fullName: f['Full Name'] || ((f['First Name'] || '') + ' ' + (f['Last Name'] || '')).trim(),
              firstName: f['First Name'] || '',
              lastName: f['Last Name'] || '',
              stage: f['Stage'] || 'Contact',
              championship: f['Championship'] || '',
              igUrl: f['IG URL'] || '',
              fbUrl: f['FB URL'] || '',
              lastActivity: f['Last Activity'] || '',
              dateMessaged: f['Date Messaged'] || ''
            };
          });

          // Pick best match: prefer the record whose name matches the rider hint
          var bestMatch = allMatches[0];
          if (driverNameHint && allMatches.length > 1) {
            for (var i = 0; i < allMatches.length; i++) {
              var mName = allMatches[i].fullName.toLowerCase().trim();
              // Exact or partial name match
              if (mName === driverNameHint ||
                mName.includes(driverNameHint) ||
                driverNameHint.includes(mName)) {
                bestMatch = allMatches[i];
                break;
              }
              // Token overlap: check if first+last name tokens match
              var hintTokens = driverNameHint.replace(/[_.\-]/g, ' ').split(/\s+/);
              var nameTokens = mName.replace(/[_.\-]/g, ' ').split(/\s+/);
              var overlap = hintTokens.filter(function (t) { return t.length > 1 && nameTokens.indexOf(t) >= 0; });
              if (overlap.length >= 2) {
                bestMatch = allMatches[i];
                break;
              }
            }
          }

          sendResponse({
            found: true,
            totalMatches: allMatches.length,
            recordId: bestMatch.recordId,
            fullName: bestMatch.fullName,
            stage: bestMatch.stage,
            championship: bestMatch.championship,
            igUrl: bestMatch.igUrl,
            fbUrl: bestMatch.fbUrl,
            lastActivity: bestMatch.lastActivity,
            dateMessaged: bestMatch.dateMessaged,
            allMatches: allMatches
          });
        } else {
          sendResponse({ found: false, reason: 'not_in_db' });
        }
      })
      .catch(function (err) {
        console.warn('[AG] URL lookup failed:', err.message);
        sendResponse({ found: false, reason: 'error', error: err.message });
      });
    return true;
  }
});

chrome.runtime.onStartup.addListener(function () { pendingTabs.clear(); });

chrome.runtime.onInstalled.addListener(function () {
  var configs = [
    { urlPatterns: ['*://www.facebook.com/*', '*://www.messenger.com/*', '*://messenger.com/*'], script: 'content-facebook.js' },
    { urlPatterns: ['*://www.instagram.com/*', '*://instagram.com/*'], script: 'content-instagram.js' },
    { urlPatterns: ['*://driver-client-generator.streamlit.app/*'], script: 'content-streamlit.js' }
  ];
  chrome.tabs.query({}, function (tabs) {
    for (var i = 0; i < tabs.length; i++) {
      var tab = tabs[i];
      if (!tab.url) continue;
      for (var j = 0; j < configs.length; j++) {
        var config = configs[j];
        var matches = config.urlPatterns.some(function (pattern) {
          var regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
          return regex.test(tab.url);
        });
        if (matches) {
          chrome.scripting.executeScript({
            target: { tabId: tab.id },
            files: [config.script]
          }).catch(function () { });
          break;
        }
      }
    }
  });

  // Set up overdue notification alarm on install/update
  chrome.alarms.create('ag_overdue_check', { delayInMinutes: 2, periodInMinutes: 30 });
});

// ══════════════════════════════════════════════════════════
// ██  OVERDUE DRIVER NOTIFICATIONS
// ══════════════════════════════════════════════════════════
// Checks Airtable every 30 minutes for drivers stuck at a stage
// beyond the expected follow-up window. Shows a Chrome notification
// so you never miss a follow-up opportunity.

// Stage → date field + max hours before overdue
var OVERDUE_CONFIG = {
  'Messaged': { dateField: 'Date Messaged', hours: 72 },
  'Replied': { dateField: 'Date Replied', hours: 48 },
  'Link Sent': { dateField: 'Date Link Sent', hours: 24 },
  'Blueprint Link Sent': { dateField: 'Date Link Sent', hours: 24 },
  'Race Weekend': { dateField: 'Date Race Review', hours: 48 },
  'Race Review Complete': { dateField: 'Date Race Review', hours: 24 },
  'Registered': { dateField: 'Date Blueprint Started', hours: 24 },
  'Blueprint Started': { dateField: 'Date Blueprint Started', hours: 24 },
  'Day 1 Complete': { dateField: 'Date Day 1 Assessment ', hours: 24 },
  'Day 2 Complete': { dateField: 'Date Day 2 Assessment ', hours: 24 }
};

async function checkOverdueDrivers() {
  if (!AIRTABLE_URL || typeof AIRTABLE_API_KEY === 'undefined') return;

  var headers = {
    'Authorization': 'Bearer ' + AIRTABLE_API_KEY,
    'Content-Type': 'application/json'
  };

  var overdueDrivers = [];
  var now = Date.now();

  // Check each stage that has a follow-up window
  for (var stage in OVERDUE_CONFIG) {
    var config = OVERDUE_CONFIG[stage];
    var windowMs = config.hours * 3600 * 1000;

    // Query Airtable for drivers at this stage
    var formula = "{Stage} = '" + stage + "'";
    try {
      var url = AIRTABLE_URL + '?filterByFormula=' + encodeURIComponent(formula)
        + '&fields[]=' + encodeURIComponent('Full Name')
        + '&fields[]=' + encodeURIComponent('First Name')
        + '&fields[]=' + encodeURIComponent('Last Name')
        + '&fields[]=' + encodeURIComponent(config.dateField)
        + '&fields[]=' + encodeURIComponent('Last Activity')
        + '&fields[]=' + encodeURIComponent('Notes')
        + '&maxRecords=50';
      var res = await fetch(url, { headers: headers });
      if (!res.ok) continue;
      var data = await res.json();
      if (!data.records || !data.records.length) continue;

      for (var i = 0; i < data.records.length; i++) {
        var r = data.records[i];
        var fields = r.fields || {};
        var dateStr = fields[config.dateField] || fields['Last Activity'];
        if (!dateStr) continue;
        var stageDate = new Date(dateStr).getTime();
        if (isNaN(stageDate)) continue;

        var elapsed = now - stageDate;
        if (elapsed > windowMs) {
          // Check if a follow-up was recently sent (notes contain ✅ FU_SENT or 📤)
          var notes = fields['Notes'] || '';
          var recentFU = false;
          var fuMatches = notes.match(/\[(\d{2} \w{3} \d{2}:\d{2}) [✅📤]\]/g);
          if (fuMatches && fuMatches.length > 0) {
            try {
              var lastTs = fuMatches[fuMatches.length - 1].match(/(\d{2} \w{3} \d{2}:\d{2})/);
              if (lastTs) {
                var parsed = new Date(lastTs[1] + ' ' + new Date().getFullYear());
                if (now - parsed.getTime() < 48 * 3600 * 1000) recentFU = true;
              }
            } catch (e) { /* ignore */ }
          }

          if (!recentFU) {
            var name = fields['Full Name'] || ((fields['First Name'] || '') + ' ' + (fields['Last Name'] || '')).trim();
            var days = Math.floor(elapsed / (24 * 3600 * 1000));
            overdueDrivers.push({ name: name, stage: stage, days: days });
          }
        }
      }
    } catch (e) {
      console.warn('[AG] Overdue check failed for stage ' + stage + ':', e.message);
    }
  }

  // Store overdue count for badge/popup
  chrome.storage.local.set({
    ag_overdue_count: overdueDrivers.length,
    ag_overdue_drivers: overdueDrivers.slice(0, 20), // Keep top 20
    ag_overdue_checked: new Date().toISOString()
  });

  // Show notification if there are overdue drivers (max once per 2 hours per count)
  if (overdueDrivers.length > 0) {
    var lastNotified = (await chrome.storage.local.get('ag_last_overdue_notify'))['ag_last_overdue_notify'] || 0;
    var lastCount = (await chrome.storage.local.get('ag_last_overdue_count'))['ag_last_overdue_count'] || 0;

    // Only notify if count changed or 2+ hours since last notification
    if (overdueDrivers.length !== lastCount || (now - lastNotified) > 2 * 3600 * 1000) {
      var topNames = overdueDrivers.slice(0, 3).map(function (d) {
        return d.name + ' (' + d.days + 'd at ' + d.stage + ')';
      });
      var body = topNames.join('\n');
      if (overdueDrivers.length > 3) {
        body += '\n+ ' + (overdueDrivers.length - 3) + ' more';
      }

      chrome.notifications.create('ag_overdue_' + now, {
        type: 'basic',
        iconUrl: 'icon128.png',
        title: '⏰ ' + overdueDrivers.length + ' driver' + (overdueDrivers.length > 1 ? 's' : '') + ' need follow-up',
        message: body,
        priority: 2,
        requireInteraction: true
      });

      chrome.storage.local.set({
        ag_last_overdue_notify: now,
        ag_last_overdue_count: overdueDrivers.length
      });

      console.log('[AG] 🔔 Overdue notification: ' + overdueDrivers.length + ' drivers need follow-up');
    }
  }

  // Update badge with overdue count
  if (overdueDrivers.length > 0) {
    var retryData = await chrome.storage.local.get('ag_retry_queue');
    var retryCount = ((retryData || {})['ag_retry_queue'] || []).length;
    if (retryCount === 0) {
      // Only show overdue badge if no pending saves (those take priority)
      chrome.action.setBadgeText({ text: String(overdueDrivers.length) });
      chrome.action.setBadgeBackgroundColor({ color: '#FF8800' }); // Orange for overdue
    }
  }

  console.log('[AG] Overdue check complete: ' + overdueDrivers.length + ' overdue');
}

// Click notification → open Streamlit dashboard
chrome.notifications.onClicked.addListener(function (notificationId) {
  if (notificationId.startsWith('ag_overdue_')) {
    chrome.tabs.create({ url: APP_URL, active: true });
    chrome.notifications.clear(notificationId);
  }
});

// Wire up the alarm
chrome.alarms.onAlarm.addListener(function (alarm) {
  if (alarm.name === 'ag_overdue_check') checkOverdueDrivers();
});

// Also ensure the alarm exists on every startup
chrome.alarms.get('ag_overdue_check', function (existing) {
  if (!existing) {
    chrome.alarms.create('ag_overdue_check', { delayInMinutes: 2, periodInMinutes: 30 });
  }
});

// ══════════════════════════════════════════════════════════
// ██  OUTREACH ANALYTICS TRACKER
// ══════════════════════════════════════════════════════════
// Records every outreach action for conversion tracking.
// Stored in chrome.storage.local and synced to the dashboard.

async function trackOutreachEvent(eventType, data) {
  try {
    var stats = (await chrome.storage.local.get('ag_outreach_stats'))['ag_outreach_stats'] || {
      total_messaged: 0,
      total_replied: 0,
      total_link_sent: 0,
      total_call_booked: 0,
      total_clients: 0,
      by_template: {},
      by_championship: {},
      by_platform: {},
      daily: {}
    };

    var today = new Date().toISOString().split('T')[0];
    if (!stats.daily[today]) {
      stats.daily[today] = { messaged: 0, replied: 0, link_sent: 0 };
    }

    switch (eventType) {
      case 'messaged':
        stats.total_messaged++;
        stats.daily[today].messaged++;
        break;
      case 'replied':
        stats.total_replied++;
        stats.daily[today].replied++;
        break;
      case 'link_sent':
        stats.total_link_sent++;
        stats.daily[today].link_sent++;
        break;
      case 'call_booked':
        stats.total_call_booked++;
        break;
      case 'client':
        stats.total_clients++;
        break;
    }

    // Track by template
    if (data.template) {
      stats.by_template[data.template] = (stats.by_template[data.template] || 0) + 1;
    }

    // Track by championship
    if (data.championship) {
      stats.by_championship[data.championship] = (stats.by_championship[data.championship] || 0) + 1;
    }

    // Track by platform
    if (data.platform) {
      stats.by_platform[data.platform] = (stats.by_platform[data.platform] || 0) + 1;
    }

    // Prune daily data older than 30 days
    var cutoff = new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString().split('T')[0];
    for (var day in stats.daily) {
      if (day < cutoff) delete stats.daily[day];
    }

    await chrome.storage.local.set({ ag_outreach_stats: stats });
  } catch (e) {
    console.warn('[AG] Stats tracking error:', e.message);
  }
}


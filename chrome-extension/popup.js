// Antigravity Social URL Grabber
// Detects Facebook/Instagram profiles and copies clean URLs

// Streamlit app URL
const STREAMLIT_APP = 'https://driver-client-generator.streamlit.app';

document.addEventListener('DOMContentLoaded', async () => {
  const statusEl = document.getElementById('status');
  const platformEl = statusEl.querySelector('.platform');
  const urlEl = statusEl.querySelector('.url');
  const profileSection = document.getElementById('profile-section');
  const profileNameEl = document.getElementById('profile-name');
  const copyBtn = document.getElementById('copy-btn');
  const openAppBtn = document.getElementById('open-app-btn');
  const successEl = document.getElementById('success');
  const successMsg = document.getElementById('success-msg');
  const errorEl = document.getElementById('error');
  const contentEl = document.getElementById('content');

  let cleanUrl = null;
  let platform = null;
  let profileName = null;

  // Get current tab
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    const url = tab.url;

    // Detect platform and extract clean URL
    if (url.includes('facebook.com')) {
      platform = 'facebook';
      cleanUrl = extractFacebookUrl(url);
      statusEl.className = 'status facebook';
      platformEl.textContent = '📘 Facebook Profile';
    } else if (url.includes('instagram.com')) {
      platform = 'instagram';
      cleanUrl = extractInstagramUrl(url);
      statusEl.className = 'status instagram';
      platformEl.textContent = '📷 Instagram Profile';
    } else {
      platformEl.textContent = '⚠️ Not a Social Profile';
      urlEl.textContent = 'Open a Facebook or Instagram profile page';
      copyBtn.disabled = true;
      openAppBtn.disabled = true;
      openAppBtn.textContent = 'Not on Social Media';
      return;
    }

    if (cleanUrl) {
      urlEl.textContent = cleanUrl;
      copyBtn.disabled = false;
      openAppBtn.disabled = false;

      // Try to get profile name from page
      try {
        const results = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          func: getProfileName,
          args: [platform]
        });

        if (results && results[0] && results[0].result) {
          profileName = results[0].result;
          profileNameEl.textContent = profileName;
          profileSection.classList.remove('hidden');
          openAppBtn.textContent = `💾 Save ${profileName}'s URL`;
        }
      } catch (e) {
        console.log('Could not extract name:', e);
      }
    } else {
      urlEl.textContent = 'Could not detect profile URL';
      copyBtn.disabled = true;
      openAppBtn.disabled = true;
    }

  } catch (err) {
    showError('Could not access tab: ' + err.message);
  }

  // "Save to App" button — saves URL via background tab, then focuses the app
  openAppBtn.addEventListener('click', async () => {
    if (!cleanUrl) return;

    // Copy URL to clipboard so user can paste it manually too
    try { await navigator.clipboard.writeText(cleanUrl); } catch(e) {}

    // Save the URL to the driver's record via background tab (no reload)
    if (profileName) {
      chrome.runtime.sendMessage({
        type: 'saveUrl',
        driver: profileName,
        fbUrl: platform === 'facebook' ? cleanUrl : '',
        igUrl: platform === 'instagram' ? cleanUrl : ''
      });
    }

    // Focus the existing Streamlit tab WITHOUT reloading it
    try {
      const tabs = await chrome.tabs.query({});
      const existingTab = tabs.find(t => t.url && t.url.includes('driver-client-generator'));

      if (existingTab) {
        await chrome.tabs.update(existingTab.id, { active: true });
        await chrome.windows.update(existingTab.windowId, { focused: true });
      }
    } catch (e) {
      // Tab focus failed — URL is on clipboard anyway
    }

    // Show success
    contentEl.classList.add('hidden');
    successMsg.textContent = `URL saved & copied!`;
    successEl.classList.remove('hidden');

    setTimeout(() => { window.close(); }, 1200);
  });

  // Copy button click
  copyBtn.addEventListener('click', async () => {
    if (!cleanUrl) return;

    try {
      await navigator.clipboard.writeText(cleanUrl);

      contentEl.classList.add('hidden');
      successMsg.textContent = 'URL Copied!';
      successEl.classList.remove('hidden');

      setTimeout(() => { window.close(); }, 1500);

    } catch (err) {
      showError('Failed to copy: ' + err.message);
    }
  });

  function showError(msg) {
    errorEl.textContent = msg;
    errorEl.classList.remove('hidden');
  }
});

// Extract clean Facebook profile URL
function extractFacebookUrl(url) {
  try {
    const parsed = new URL(url);
    const path = parsed.pathname;

    // Skip non-profile pages
    if (path === '/' ||
        path.startsWith('/watch') ||
        path.startsWith('/groups') ||
        path.startsWith('/events') ||
        path.startsWith('/marketplace') ||
        path.startsWith('/gaming') ||
        path.startsWith('/login') ||
        path.startsWith('/settings')) {
      return null;
    }

    // Handle /profile.php?id=123 format
    if (path === '/profile.php') {
      const id = parsed.searchParams.get('id');
      if (id) {
        return `https://www.facebook.com/profile.php?id=${id}`;
      }
      return null;
    }

    // Handle /username format
    // Remove trailing slashes and query params
    const username = path.split('/')[1];
    if (username && username.length > 0) {
      return `https://www.facebook.com/${username}`;
    }

    return null;
  } catch (e) {
    return null;
  }
}

// Extract clean Instagram profile URL
function extractInstagramUrl(url) {
  try {
    const parsed = new URL(url);
    const path = parsed.pathname;

    // Skip non-profile pages
    if (path === '/' ||
        path.startsWith('/p/') ||      // posts
        path.startsWith('/reel/') ||   // reels
        path.startsWith('/stories/') ||
        path.startsWith('/explore') ||
        path.startsWith('/direct') ||
        path.startsWith('/accounts')) {
      return null;
    }

    // Extract username from path
    const parts = path.split('/').filter(p => p.length > 0);
    if (parts.length >= 1) {
      const username = parts[0];
      return `https://www.instagram.com/${username}/`;
    }

    return null;
  } catch (e) {
    return null;
  }
}

// Function to run in page context to get profile name
function getProfileName(platform) {
  if (platform === 'facebook') {
    // Try various selectors for Facebook profile name
    const selectors = [
      'h1[dir="auto"]',
      '[data-testid="profile_name"]',
      'h1.x1heor9g',
      'span.x1lliihq.x6ikm8r.x10wlt62'
    ];

    for (const sel of selectors) {
      const el = document.querySelector(sel);
      if (el && el.textContent.trim()) {
        return el.textContent.trim();
      }
    }
  } else if (platform === 'instagram') {
    // Try various selectors for Instagram profile name
    const selectors = [
      'header h2',
      'header section h1',
      'h2._aacl._aacs._aact._aacx._aada'
    ];

    for (const sel of selectors) {
      const el = document.querySelector(sel);
      if (el && el.textContent.trim()) {
        return el.textContent.trim();
      }
    }
  }

  return null;
}

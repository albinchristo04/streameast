// index.js
// Minimal service: obtains/stores refresh token, fetches events JSON, creates Blogger posts for new matches.
// Requires: NODE_ENV=production, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, BLOG_ID, JSON_URL, DB_FILE (optional)

const express = require('express');
const fetch = require('node-fetch');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const {
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI,
  BLOG_ID,
  JSON_URL = 'https://raw.githubusercontent.com/albinchristo04/ptv/refs/heads/main/events_with_m3u8.json',
  DB_FILE = './db.json',
  PORT = 3000
} = process.env;

if (!CLIENT_ID || !CLIENT_SECRET || !REDIRECT_URI || !BLOG_ID) {
  console.warn('One or more required env vars missing: CLIENT_ID CLIENT_SECRET REDIRECT_URI BLOG_ID');
}

const app = express();
app.use(express.json());

const oauth2Client = new google.auth.OAuth2(
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI
);

function readDB() {
  try {
    const raw = fs.readFileSync(DB_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    return { postedIds: {}, refresh_token: null };
  }
}
function writeDB(db) {
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
}

// Endpoint: start OAuth consent flow (visit this URL)
app.get('/auth', (req, res) => {
  const scopes = ['https://www.googleapis.com/auth/blogger'];
  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline', // important to get refresh_token
    scope: scopes,
    prompt: 'consent' // force refresh_token every time (useful during setup)
  });
  res.redirect(url);
});

// OAuth2 redirect/callback: exchange code for tokens and store refresh_token
app.get('/oauth2callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('Missing code');
  try {
    const { tokens } = await oauth2Client.getToken(code);
    // tokens may contain refresh_token on first consent
    const db = readDB();
    if (tokens.refresh_token) {
      db.refresh_token = tokens.refresh_token;
      writeDB(db);
      res.send('Refresh token saved. You can close this page.');
    } else {
      // if no refresh token, maybe previously granted. show tokens for debug (do NOT leave in production)
      res.send('No refresh token returned. If you previously granted consent you can reuse the existing refresh token stored on the server.');
    }
  } catch (err) {
    console.error(err);
    res.status(500).send('Token exchange failed: ' + err.message);
  }
});

// Manual trigger to sync now. Protected by a simple secret header (optional).
// Set X-SYNC-SECRET header to the value of SYNC_SECRET env var if provided.
app.post('/sync', async (req, res) => {
  try {
    const SYNC_SECRET = process.env.SYNC_SECRET;
    if (SYNC_SECRET) {
      const sent = req.header('x-sync-secret') || req.query.secret;
      if (sent !== SYNC_SECRET) return res.status(403).send('Forbidden');
    }

    const result = await syncAndCreatePosts();
    res.json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => {
  res.send('Blogger sync service. Use /auth to authorize and POST /sync to sync.');
});

// Core logic: fetch JSON, flatten streams, create posts for new items
async function syncAndCreatePosts() {
  const db = readDB();
  if (!db.postedIds) db.postedIds = {};

  if (!db.refresh_token) {
    throw new Error('No refresh token found in DB. Visit /auth and complete OAuth flow.');
  }

  // configure oauth2 client with refresh token
  oauth2Client.setCredentials({ refresh_token: db.refresh_token });

  const blogger = google.blogger({ version: 'v3', auth: oauth2Client });

  // fetch JSON
  const resp = await fetch(JSON_URL, { timeout: 20000 });
  if (!resp.ok) throw new Error('Failed to fetch events JSON: ' + resp.status);
  const j = await resp.json();

  // Collect streams: structure assumed from template earlier
  const streams = [];
  if (j.events && Array.isArray(j.events.streams)) {
    j.events.streams.forEach(cat => {
      if (Array.isArray(cat.streams)) {
        cat.streams.forEach(s => {
          if (!s.id && s.tag) s.id = s.tag; // fallback
          s._category = cat.category || cat.category_name || '';
          streams.push(s);
        });
      }
    });
  } else if (Array.isArray(j)) {
    // fallback if JSON is just an array
    j.forEach(s => streams.push(s));
  }

  const created = [];
  for (const s of streams) {
    const sid = String(s.id || s.uri_name || s.name || s.title || (s.tag||''));
    if (!sid) continue;
    if (db.postedIds[sid]) continue; // already posted

    // Build post content
    const title = s.name || s.title || `Match ${sid}`;
    const startsAt = s.starts_at ? new Date(Number(s.starts_at) * 1000).toLocaleString() : 'TBA';
    const iframe = s.iframe || s.resolved_m3u8?.[0]?.url || '';
    const poster = s.poster ? `<p><img src="${escapeHtml(s.poster)}" style="max-width:100%;height:auto"></p>` : '';
    const content = `
      <p><strong>Category:</strong> ${escapeHtml(s._category || s.category_name || '')}</p>
      <p><strong>Starts:</strong> ${escapeHtml(startsAt)}</p>
      ${poster}
      <p>${escapeHtml(s.tag || '')}</p>
      ${iframe ? `<p><iframe src="${escapeHtml(iframe)}" width="100%" height="480" frameborder="0" allowfullscreen></iframe></p>` : ''}
      <p>Source: auto-generated from events JSON.</p>
    `;

    // Insert post
    try {
      const respPost = await blogger.posts.insert({
        blogId: BLOG_ID,
        requestBody: {
          title: title,
          content: content,
          labels: [ s._category || 'match' ]
        }
      });
      db.postedIds[sid] = {
        postId: respPost.data.id,
        createdAt: new Date().toISOString(),
        title: title
      };
      created.push({ sid, postId: respPost.data.id, title });
      // small delay for quota safety (optional)
      await new Promise(r => setTimeout(r, 500));
    } catch (err) {
      console.error('Failed creating post for', sid, err.message || err);
      // do not mark as posted; continue.
    }
  }

  writeDB(db);
  return { createdCount: created.length, created };
}

function escapeHtml(s) {
  return String(s || '').replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;', "'":'&#39;' })[c]);
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

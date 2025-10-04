// index.js (updated)
// Your existing setup + FCM push wiring (send-on-write) and userTokens management

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json({ limit: '8mb' }));

// simple request logger
app.use((req, res, next) => {
  console.log(new Date().toISOString(), req.method, req.originalUrl);
  next();
});

const PORT = process.env.PORT || 3000;

// ---------- Firebase admin init ----------
let admin, firestore, rtdb, storageBucket;
let HAS_FIREBASE = false;

try {
  admin = require('firebase-admin');
  const svcPath = process.env.SERVICE_ACCOUNT_PATH || process.env.GOOGLE_APPLICATION_CREDENTIALS || './serviceAccountKey.json';
  if (svcPath && fs.existsSync(path.resolve(svcPath))) {
    const serviceAccount = require(path.resolve(svcPath));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: process.env.FIREBASE_RTDB_URL || serviceAccount.databaseURL || undefined,
      storageBucket: process.env.FIREBASE_STORAGE_BUCKET || undefined
    });
    HAS_FIREBASE = true;
    console.log('✅ Firebase Admin initialized using service account JSON.');
  } else {
    // try ADC
    try {
      admin.initializeApp({
        databaseURL: process.env.FIREBASE_RTDB_URL || undefined,
        storageBucket: process.env.FIREBASE_STORAGE_BUCKET || undefined
      });
      HAS_FIREBASE = true;
      console.log('✅ Firebase Admin initialized using ADC.');
    } catch (e) {
      console.warn('⚠️ Firebase admin not initialized. Running in-memory fallback.');
      HAS_FIREBASE = false;
    }
  }
  if (HAS_FIREBASE) {
    firestore = admin.firestore();
    try { rtdb = admin.database(); } catch (e) { rtdb = null; }
    try { storageBucket = admin.storage().bucket(); } catch (e) { storageBucket = null; }
  }
} catch (err) {
  console.warn('⚠️ firebase-admin failed to init:', err.message);
  HAS_FIREBASE = false;
}

// ---------- In-memory fallback stores ----------
const memory = {
  users: [],   // array of user objects
  posts: [],   // array of post objects
  refreshTokens: {}, // token -> { uid, expiresAt }
  ai: {}, // ai messages in-memory (uid -> [ { role, text, createdAt } ])
  userTokens: {}, // uid -> { tokens: [] } for fallback
  notifications: {} // id -> notification
};

const sseClients = new Set();
function broadcastEvent(eventName, data) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch (e) { /* ignore broken */ }
  }
}

function recordRequest(req) {
  broadcastEvent('request', { method: req.method, path: req.originalUrl, body: req.body || null, time: Date.now() });
}

/* --------------------------
   Normalizers (keeps your shapes)
   -------------------------- */

function normalizeUser(body) {
  return {
    id: body.id || body.uid || (body.email ? `user_${Date.now()}` : uuidv4()),
    name: body.name || '',
    email: body.email || '',
    profilePictureUrl: body.profilePictureUrl || null,
    role: body.role || 'CONSUMER',
    badges: Array.isArray(body.badges) ? body.badges : (body.badges ? body.badges : []),
    settings: body.settings || {},
    joinedAt: Number(body.joinedAt) || Date.now()
  };
}

function normalizeComment(obj) {
  if (!obj) return null;
  return {
    id: obj.id || uuidv4(),
    postId: obj.postId || obj.post_id || '',
    userId: obj.userId || obj.user_id || '',
    userName: obj.userName || obj.userName || obj.userName || '',
    userAvatarUrl: obj.userAvatarUrl || obj.userAvatarUrl || null,
    text: obj.text || '',
    createdAt: Number(obj.createdAt) || Date.now()
  };
}

function normalizeReaction(obj) {
  return {
    userId: obj.userId || obj.user_id || '',
    type: obj.type || obj.type || 'HELPFUL',
    timestamp: Number(obj.timestamp) || Date.now()
  };
}

function normalizeLocation(obj) {
  if (!obj) return null;
  return {
    latitude: Number(obj.latitude || obj.lat || 0),
    longitude: Number(obj.longitude || obj.lng || 0),
    address: obj.address || null,
    mallName: obj.mallName || obj.mall_name || null,
    floorLevel: obj.floorLevel || obj.floor_level || null
  };
}

function normalizePost(body) {
  return {
    id: body.id || body.postId || uuidv4(),
    userId: body.userId || body.authorId || '',
    contentText: body.contentText || body.content || '',
    mediaUrls: Array.isArray(body.mediaUrls) ? body.mediaUrls : (body.mediaUrls ? JSON.parse(body.mediaUrls || '[]') : []),
    category: body.category || (body.category && String(body.category)) || 'OTHER',
    visibility: body.visibility || 'PUBLIC',
    moodEmoji: body.moodEmoji || body.mood || null,
    hashtags: Array.isArray(body.hashtags) ? body.hashtags : (body.hashtags ? body.hashtags : []),
    location: normalizeLocation(body.location) || null,
    reactions: Array.isArray(body.reactions) ? body.reactions.map(normalizeReaction) : (body.reactions ? body.reactions : []),
    comments: Array.isArray(body.comments) ? body.comments.map(normalizeComment) : (body.comments ? body.comments : []),
    createdAt: Number(body.createdAt) || Date.now()
  };
}

/* --------------------------
   Helpers for push notifications (using Firestore userTokens and admin.messaging)
   -------------------------- */

async function getTokensForUid(uid) {
  if (!uid) return [];
  try {
    if (HAS_FIREBASE && firestore) {
      const snap = await firestore.collection('userTokens').doc(uid).get();
      if (!snap.exists) return [];
      const data = snap.data() || {};
      return Array.isArray(data.tokens) ? data.tokens : [];
    } else {
      const rec = memory.userTokens[uid];
      return rec && Array.isArray(rec.tokens) ? rec.tokens.slice() : [];
    }
  } catch (e) {
    console.warn('getTokensForUid error', e.message);
    return [];
  }
}

async function removeInvalidTokensForUid(uid, badTokens) {
  if (!uid || !badTokens || badTokens.length === 0) return;
  try {
    if (HAS_FIREBASE && firestore) {
      // remove each invalid token via arrayRemove
      const doc = firestore.collection('userTokens').doc(uid);
      for (const t of badTokens) {
        await doc.update({ tokens: admin.firestore.FieldValue.arrayRemove(t) }).catch(() => {});
      }
    } else {
      if (memory.userTokens[uid]) {
        memory.userTokens[uid].tokens = (memory.userTokens[uid].tokens || []).filter(t => !badTokens.includes(t));
      }
    }
  } catch (e) {
    console.warn('removeInvalidTokensForUid error', e.message);
  }
}

async function sendPushToTokens(tokens, payloadData) {
  if (!Array.isArray(tokens) || tokens.length === 0) return { success: 0, failure: 0, results: [] };
  if (!HAS_FIREBASE || !admin || !admin.messaging) {
    console.log('No firebase admin - skipping push send (dev fallback). payloadData:', payloadData);
    return { success: 0, failure: 0, results: [] };
  }
  // Use admin.messaging().sendToDevice (data-only message)
  try {
    const message = {
      data: Object.keys(payloadData).reduce((acc, k) => {
        acc[k] = payloadData[k] === undefined || payloadData[k] === null ? '' : String(payloadData[k]);
        return acc;
      }, {}),
      tokens
    };
    const resp = await admin.messaging().sendEachForMulticast ? admin.messaging().sendMulticast(message) : admin.messaging().sendToDevice(tokens, message.data);
    // Normalise response object shape for either sendMulticast or sendToDevice
    if (resp && resp.responses) {
      // sendMulticast response
      return resp;
    } else if (resp && Array.isArray(resp.results)) {
      // older sendToDevice response
      return resp;
    } else {
      return { success: 0, failure: 0, results: [] };
    }
  } catch (err) {
    console.error('sendPushToTokens error', err);
    return { success: 0, failure: 0, results: [] };
  }
}

async function sendNotificationPayloadToUid(uid, payloadData) {
  const tokens = await getTokensForUid(uid);
  if (!tokens || tokens.length === 0) {
    console.log('No tokens for uid', uid);
    return;
  }
  // Send
  const resp = await sendPushToTokens(tokens, payloadData);

  // If resp has `responses` (sendMulticast) => check each for error and collect tokens to remove
  const badTokens = [];
  try {
    if (resp && resp.responses && Array.isArray(resp.responses)) {
      resp.responses.forEach((r, idx) => {
        if (r.error) {
          const token = tokens[idx];
          // common reasons: unregister, invalid-argument, unregistered
          badTokens.push(token);
        }
      });
    } else if (resp && Array.isArray(resp.results)) {
      resp.results.forEach((r, idx) => {
        if (r.error) {
          const token = tokens[idx];
          badTokens.push(token);
        }
      });
    }
  } catch (e) {
    console.warn('parsing push response error', e.message);
  }

  if (badTokens.length > 0) {
    await removeInvalidTokensForUid(uid, [...new Set(badTokens)]);
  }
}

async function sendNotificationPayloadToMultipleUids(uids, payloadData) {
  // collapse tokens per uid and dispatch in parallel (throttle if you have many uids)
  const promises = (uids || []).map(uid => sendNotificationPayloadToUid(uid, payloadData));
  await Promise.all(promises);
}

async function sendNotificationToTopic(topic, payloadData) {
  if (!HAS_FIREBASE || !admin || !admin.messaging) {
    console.log('No firebase admin - skipping topic push send (dev fallback). payloadData:', payloadData);
    return;
  }
  try {
    await admin.messaging().sendToTopic(topic, { data: Object.keys(payloadData).reduce((acc, k) => { acc[k] = payloadData[k] == null ? '' : String(payload[k]); return acc; }, {}) });
  } catch (e) {
    console.error('sendNotificationToTopic error', e);
  }
}

/* --------------------------
   Routes (existing + new token & notifications endpoints)
   -------------------------- */

/* Health + SSE debug */
app.get('/', (req, res) => res.send({ ok: true, message: 'Hydra Hymail backend', firebase: HAS_FIREBASE }));

app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  res.write(': connected\n\n');
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

app.get('/debug', (req, res) => {
  res.setHeader('Content-Type', 'text/html');
  res.send(`<html><body><h3>Hydra Hymail Debug (firebase:${HAS_FIREBASE})</h3><div id="list"></div><script>
    const es = new EventSource('/events');
    es.addEventListener('request', e => {
      const d = JSON.parse(e.data);
      const el = document.createElement('div');
      el.innerHTML = '<b>'+d.method+' '+d.path+'</b> ' + JSON.stringify(d.body).slice(0,200);
      document.getElementById('list').prepend(el);
    });
  </script></body></html>`);
});

/* ---------- USERS CRUD ---------- */

app.post('/users', async (req, res) => {
  try {
    recordRequest(req);
    const u = normalizeUser(req.body || {});
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('users').doc(u.id).set(u);
      if (rtdb) await rtdb.ref(`/users/${u.id}`).set(u);
    } else {
      const i = memory.users.findIndex(x => x.id === u.id);
      if (i === -1) memory.users.unshift(u); else memory.users[i] = u;
    }
    broadcastEvent('user-created', { id: u.id, email: u.email, role: u.role });
    return res.status(201).json({ ok: true, user: u });
  } catch (err) {
    console.error('POST /users error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/users/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('users').doc(id).get();
      if (!doc.exists) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, user: doc.data() });
    } else {
      const user = memory.users.find(u => u.id === id);
      if (!user) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, user });
    }
  } catch (err) {
    console.error('GET /users/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.put('/users/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    const patch = normalizeUser({ ...req.body, id });
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('users').doc(id).set(patch, { merge: true });
      const updated = await firestore.collection('users').doc(id).get();
      return res.json({ ok: true, user: updated.data() });
    } else {
      const idx = memory.users.findIndex(u => u.id === id);
      if (idx === -1) return res.status(404).json({ ok: false, error: 'Not found' });
      memory.users[idx] = { ...memory.users[idx], ...patch };
      return res.json({ ok: true, user: memory.users[idx] });
    }
  } catch (err) {
    console.error('PUT /users/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.delete('/users/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('users').doc(id).delete();
      if (rtdb) await rtdb.ref(`/users/${id}`).remove();
      return res.json({ ok: true });
    } else {
      memory.users = memory.users.filter(u => u.id !== id);
      return res.json({ ok: true });
    }
  } catch (err) {
    console.error('DELETE /users/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/users', async (req, res) => {
  try {
    recordRequest(req);
    if (HAS_FIREBASE && firestore) {
      const snap = await firestore.collection('users').limit(200).get();
      const users = snap.docs.map(d => d.data());
      return res.json({ ok: true, users });
    } else {
      return res.json({ ok: true, users: memory.users.slice(0, 200) });
    }
  } catch (err) {
    console.error('GET /users error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- userTokens: store device tokens for each uid ----------
   POST /userTokens/:uid  { token: "..." }  -> adds token via arrayUnion
   GET /userTokens/:uid   -> list tokens
   DELETE /userTokens/:uid  { token } -> remove token
*/
app.post('/userTokens/:uid', async (req, res) => {
  try {
    recordRequest(req);
    const uid = req.params.uid;
    const token = (req.body && req.body.token) || null;
    if (!uid || !token) return res.status(400).json({ ok: false, error: 'uid and token required' });

    if (HAS_FIREBASE && firestore) {
      await firestore.collection('userTokens').doc(uid).set({ tokens: admin.firestore.FieldValue.arrayUnion(token) }, { merge: true });
      return res.json({ ok: true });
    } else {
      memory.userTokens[uid] = memory.userTokens[uid] || { tokens: [] };
      if (!memory.userTokens[uid].tokens.includes(token)) memory.userTokens[uid].tokens.push(token);
      return res.json({ ok: true });
    }
  } catch (err) {
    console.error('POST /userTokens/:uid error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/userTokens/:uid', async (req, res) => {
  try {
    recordRequest(req);
    const uid = req.params.uid;
    if (!uid) return res.status(400).json({ ok: false, error: 'uid required' });
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('userTokens').doc(uid).get();
      if (!doc.exists) return res.json({ ok: true, tokens: [] });
      return res.json({ ok: true, tokens: doc.data().tokens || [] });
    } else {
      return res.json({ ok: true, tokens: (memory.userTokens[uid] && memory.userTokens[uid].tokens) || [] });
    }
  } catch (err) {
    console.error('GET /userTokens/:uid error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.delete('/userTokens/:uid', async (req, res) => {
  try {
    recordRequest(req);
    const uid = req.params.uid;
    const token = (req.body && req.body.token) || null;
    if (!uid || !token) return res.status(400).json({ ok: false, error: 'uid and token required' });
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('userTokens').doc(uid).update({ tokens: admin.firestore.FieldValue.arrayRemove(token) });
      return res.json({ ok: true });
    } else {
      if (memory.userTokens[uid]) {
        memory.userTokens[uid].tokens = (memory.userTokens[uid].tokens || []).filter(t => t !== token);
      }
      return res.json({ ok: true });
    }
  } catch (err) {
    console.error('DELETE /userTokens/:uid error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- AUTH refresh helpers (unchanged) ---------- */

app.post('/auth/issue_refresh', async (req, res) => {
  try {
    recordRequest(req);
    const { firebaseUid } = req.body || {};
    if (!firebaseUid) return res.status(400).json({ ok: false, error: 'firebaseUid required' });
    const token = uuidv4();
    const expiresAt = Date.now() + (1000 * 60 * 60 * 24 * 30); // 30 days
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('refreshTokens').doc(token).set({ uid: firebaseUid, expiresAt });
    } else {
      memory.refreshTokens[token] = { uid: firebaseUid, expiresAt };
    }
    return res.json({ ok: true, refreshToken: token, expiresAt });
  } catch (err) {
    console.error('POST /auth/issue_refresh error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/auth/exchange_refresh', async (req, res) => {
  try {
    recordRequest(req);
    const { refreshToken } = req.body || {};
    if (!refreshToken) return res.status(400).json({ ok: false, error: 'refreshToken required' });
    let rec = null;
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('refreshTokens').doc(refreshToken).get();
      rec = doc.exists ? doc.data() : null;
    } else {
      rec = memory.refreshTokens[refreshToken] || null;
    }
    if (!rec) return res.status(401).json({ ok: false, error: 'Invalid refresh token' });
    if (rec.expiresAt && Date.now() > rec.expiresAt) return res.status(401).json({ ok: false, error: 'expired' });

    const uid = rec.uid;
    let user = null;
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('users').doc(uid).get();
      user = doc.exists ? doc.data() : null;
    } else {
      user = memory.users.find(u => u.id === uid) || null;
    }
    const session = { uid, email: (user && user.email) || null, role: (user && user.role) || null };
    return res.json({ ok: true, session });
  } catch (err) {
    console.error('POST /auth/exchange_refresh error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- POSTS CRUD (unchanged mostly) ---------- */

app.post('/posts', async (req, res) => {
  try {
    recordRequest(req);
    const p = normalizePost(req.body || {});
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('posts').doc(p.id).set(p);
      if (rtdb) await rtdb.ref(`/posts/${p.id}`).set(p);
    } else {
      const idx = memory.posts.findIndex(x => x.id === p.id);
      if (idx === -1) memory.posts.unshift(p); else memory.posts[idx] = p;
    }

    broadcastEvent('post-created', { id: p.id, category: p.category });

    // Optional: auto-create notification when creating a post if caller asked for it
    try {
      if (req.body && req.body.notify) {
        // default notify to "all" unless targetUid/targetUids provided
        const notifPayload = {
          title: req.body.notificationTitle || `New post from ${p.userId || 'someone'}`,
          body: req.body.notificationBody || (p.contentText ? p.contentText.slice(0, 180) : 'New post'),
          targetUid: req.body.targetUid || req.body.target || 'all',
          targetUids: Array.isArray(req.body.targetUids) ? req.body.targetUids : undefined,
          postId: p.id,
          actorUid: p.userId || null,
          soundKey: req.body.soundKey || 'notification_sound_1'
        };
        // create and send notification (non-blocking)
        createNotificationAndSend(notifPayload).catch(e => console.warn('auto notification send failed', e.message));
      }
    } catch (e) { /* ignore */ }

    return res.status(201).json({ ok: true, post: p });
  } catch (err) {
    console.error('POST /posts error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/posts', async (req, res) => {
  try {
    recordRequest(req);
    if (HAS_FIREBASE && firestore) {
      const snap = await firestore.collection('posts').orderBy('createdAt', 'desc').limit(200).get();
      const posts = snap.docs.map(d => d.data());
      return res.json({ ok: true, posts });
    } else {
      return res.json({ ok: true, posts: memory.posts.slice(0, 200) });
    }
  } catch (err) {
    console.error('GET /posts error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/posts/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('posts').doc(id).get();
      if (!doc.exists) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, post: doc.data() });
    } else {
      const post = memory.posts.find(p => p.id === id);
      if (!post) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, post });
    }
  } catch (err) {
    console.error('GET /posts/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.put('/posts/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    const patch = normalizePost({ ...req.body, id });
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('posts').doc(id).set(patch, { merge: true });
      const updated = await firestore.collection('posts').doc(id).get();
      return res.json({ ok: true, post: updated.data() });
    } else {
      const idx = memory.posts.findIndex(p => p.id === id);
      if (idx === -1) return res.status(404).json({ ok: false, error: 'Not found' });
      memory.posts[idx] = { ...memory.posts[idx], ...patch };
      return res.json({ ok: true, post: memory.posts[idx] });
    }
  } catch (err) {
    console.error('PUT /posts/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.delete('/posts/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    if (HAS_FIREBASE && firestore) {
      await firestore.collection('posts').doc(id).delete();
      if (rtdb) await rtdb.ref(`/posts/${id}`).remove();
      return res.json({ ok: true });
    } else {
      memory.posts = memory.posts.filter(p => p.id !== id);
      return res.json({ ok: true });
    }
  } catch (err) {
    console.error('DELETE /posts/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- Reaction endpoint (unchanged) ---------- */

app.post('/posts/:id/reaction', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    const { userId, type } = req.body || {};
    if (!userId || !type) return res.status(400).json({ ok: false, error: 'userId and type required' });

    const reaction = normalizeReaction({ userId, type, timestamp: Date.now() });

    if (HAS_FIREBASE && firestore) {
      await firestore.runTransaction(async (tx) => {
        const ref = firestore.collection('posts').doc(id);
        const s = await tx.get(ref);
        if (!s.exists) throw new Error('Post not found');
        const data = s.data();
        const reactions = Array.isArray(data.reactions) ? data.reactions : [];
        reactions.push(reaction);
        const likeCount = (data.likeCount || 0) + 1;
        tx.update(ref, { reactions, likeCount });
      });
      const updated = await firestore.collection('posts').doc(id).get();
      broadcastEvent('post-reaction', { id, reaction });
      return res.json({ ok: true, post: updated.data() });
    } else {
      const p = memory.posts.find(p => p.id === id);
      if (!p) return res.status(404).json({ ok: false, error: 'Not found' });
      p.reactions = p.reactions || [];
      p.reactions.push(reaction);
      p.likeCount = (p.likeCount || 0) + 1;
      broadcastEvent('post-reaction', { id, reaction });
      return res.json({ ok: true, post: p });
    }
  } catch (err) {
    console.error('POST /posts/:id/reaction error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- Notifications: create & send directly from server ---------- */

/**
 * createNotificationAndSend(payload)
 * payload = {
 *   title: string,
 *   body: string,
 *   targetUid: (string) optional, // single uid or "all" (topic)
 *   targetUids: (array) optional, // array of uids
 *   postId: (string) optional,
 *   actorUid: (string) optional,
 *   soundKey: (string) optional (e.g. notification_sound_2)
 * }
 */
async function createNotificationAndSend(payload) {
  // Build doc
  const id = uuidv4();
  const doc = {
    id,
    title: payload.title || 'Notification',
    body: payload.body || '',
    targetUid: (typeof payload.targetUid === 'string') ? payload.targetUid : null,
    postId: payload.postId || null,
    actorUid: payload.actorUid || null,
    soundKey: payload.soundKey || 'notification_sound_1',
    createdAt: HAS_FIREBASE && admin ? admin.firestore.FieldValue.serverTimestamp() : Date.now()
  };

  // Save to Firestore (or memory)
  if (HAS_FIREBASE && firestore) {
    await firestore.collection('notifications').doc(id).set(doc);
  } else {
    memory.notifications[id] = doc;
  }
  broadcastEvent('notification-created', { id: doc.id, title: doc.title });

  // Prepare data payload for FCM (data-only)
  const dataPayload = {
    title: doc.title,
    body: doc.body,
    postId: doc.postId || '',
    actorUid: doc.actorUid || '',
    soundKey: doc.soundKey || ''
  };

  // Send: prefer explicit targetUids -> single targetUid -> fallback to topic "all"
  if (Array.isArray(payload.targetUids) && payload.targetUids.length > 0) {
    await sendNotificationPayloadToMultipleUids(payload.targetUids, dataPayload);
  } else if (payload.targetUid === 'all' || payload.targetUid === 'ALL' || payload.targetUid === 'topic_all') {
    if (HAS_FIREBASE && admin && admin.messaging) {
      await admin.messaging().sendToTopic('all', { data: dataPayload }).catch(e => console.warn('sendToTopic failed', e.message));
    } else {
      console.log('Topic send skipped (no admin)');
    }
  } else if (payload.targetUid) {
    await sendNotificationPayloadToUid(payload.targetUid, dataPayload);
  } else {
    // If no target provided, treat as "all"
    if (HAS_FIREBASE && admin && admin.messaging) {
      await admin.messaging().sendToTopic('all', { data: dataPayload }).catch(e => console.warn('sendToTopic failed', e.message));
    } else {
      console.log('No target provided and no admin; skipping push');
    }
  }

  return doc;
}

/* POST /notifications - create a notification and send immediately */
app.post('/notifications', async (req, res) => {
  try {
    recordRequest(req);
    const { title, body, targetUid, targetUids, postId, actorUid, soundKey } = req.body || {};
    if (!title || !body) return res.status(400).json({ ok: false, error: 'title and body required' });

    const payload = { title, body, targetUid, targetUids, postId, actorUid, soundKey };
    const doc = await createNotificationAndSend(payload);
    return res.status(201).json({ ok: true, notification: doc });
  } catch (err) {
    console.error('POST /notifications error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* GET /notifications/:id for debugging */
app.get('/notifications/:id', async (req, res) => {
  try {
    recordRequest(req);
    const id = req.params.id;
    if (HAS_FIREBASE && firestore) {
      const doc = await firestore.collection('notifications').doc(id).get();
      if (!doc.exists) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, notification: doc.data() });
    } else {
      const doc = memory.notifications[id];
      if (!doc) return res.status(404).json({ ok: false, error: 'Not found' });
      return res.json({ ok: true, notification: doc });
    }
  } catch (err) {
    console.error('GET /notifications/:id error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* ---------- AI chat messages, markers, heatmap (unchanged) ---------- */

app.post('/ai/:uid/messages', async (req, res) => {
  try {
    recordRequest(req);
    const uid = req.params.uid;
    const { role, text } = req.body || {};
    if (!role || typeof text !== 'string') return res.status(400).json({ ok: false, error: 'role and text required' });

    if (HAS_FIREBASE && firestore && admin) {
      const col = firestore.collection('ai_chats').doc(uid).collection('messages');
      await col.add({
        role,
        text,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
      broadcastEvent('ai-message', { uid, role, text });
      return res.status(201).json({ ok: true });
    } else {
      memory.ai = memory.ai || {};
      memory.ai[uid] = memory.ai[uid] || [];
      memory.ai[uid].push({ role, text, createdAt: Date.now() });
      broadcastEvent('ai-message', { uid, role, text });
      return res.status(201).json({ ok: true });
    }
  } catch (err) {
    console.error('POST /ai/:uid/messages error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/ai/:uid/messages', async (req, res) => {
  try {
    recordRequest(req);
    const uid = req.params.uid;
    const limit = Math.min(200, Number(req.query.limit || 200));

    if (HAS_FIREBASE && firestore) {
      const snap = await firestore.collection('ai_chats').doc(uid).collection('messages')
        .orderBy('createdAt', 'asc').limit(limit).get();
      const messages = snap.docs.map(d => {
        const data = d.data();
        const createdAt = data.createdAt && data.createdAt.toMillis ? data.createdAt.toMillis() : data.createdAt || null;
        return { id: d.id, role: data.role, text: data.text, createdAt };
      });
      return res.json({ ok: true, messages });
    } else {
      memory.ai = memory.ai || {};
      const messages = (memory.ai[uid] || []).slice(-limit).map(m => ({ role: m.role, text: m.text, createdAt: m.createdAt }));
      return res.json({ ok: true, messages });
    }
  } catch (err) {
    console.error('GET /ai/:uid/messages error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/markers', async (req, res) => {
  try {
    recordRequest(req);
    const mallId = req.query.mallId;
    let posts = [];
    if (HAS_FIREBASE && firestore) {
      let q = firestore.collection('posts');
      if (mallId) q = q.where('mallId', '==', mallId);
      const snap = await q.get();
      posts = snap.docs.map(d => d.data());
    } else {
      posts = memory.posts.slice();
      if (mallId) posts = posts.filter(p => (p.mallId || '').toLowerCase() === (mallId || '').toLowerCase());
    }
    const markers = posts.map(p => ({
      lat: p.location?.latitude || -26.1076,
      lng: p.location?.longitude || 28.0567,
      title: (p.contentText || '').slice(0, 40),
      snippet: p.category || 'other',
      postId: p.id,
      category: p.category || 'other'
    }));
    return res.json({ ok: true, markers });
  } catch (err) {
    console.error('GET /markers error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/heatmap', async (req, res) => {
  try {
    recordRequest(req);
    let posts = [];
    if (HAS_FIREBASE && firestore) {
      const snap = await firestore.collection('posts').get();
      posts = snap.docs.map(d => d.data());
    } else {
      posts = memory.posts.slice();
    }
    const points = posts.map(p => ({ lat: p.location?.latitude || -26.1076, lng: p.location?.longitude || 28.0567 }));
    return res.json({ ok: true, points });
  } catch (err) {
    console.error('GET /heatmap error', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* fallback */
app.use((req, res) => res.status(404).json({ ok: false, error: 'Not found' }));

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  if (!HAS_FIREBASE) console.log('⚠️ Note: Firebase not initialized — running with in-memory fallback.');
});

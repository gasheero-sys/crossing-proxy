const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const PRACTITIONER_PIN = process.env.PRACTITIONER_PIN || '0000';

// Log environment on startup
console.log('DATABASE_URL set:', !!process.env.DATABASE_URL);
console.log('DATABASE_URL prefix:', (process.env.DATABASE_URL || '').slice(0, 30));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Per-group Guide response lock — prevents concurrent Guide calls for same group
const groupGuideLocks = new Set();

app.use(cors());
app.use(express.json({ limit: '4mb' }));

async function initDB() {
  console.log('Attempting DB connection...');
  let client;
  try {
    client = await pool.connect();
    console.log('DB connected successfully');
    await client.query(`
      CREATE TABLE IF NOT EXISTS clients (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        pin_hash TEXT NOT NULL,
        email TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS sessions (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        started_at TIMESTAMPTZ DEFAULT NOW(),
        ended_at TIMESTAMPTZ,
        duration_seconds INTEGER DEFAULT 0,
        word_count INTEGER DEFAULT 0,
        session_number INTEGER DEFAULT 1
      );
      CREATE TABLE IF NOT EXISTS story_arc (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        point_a TEXT, point_b TEXT, obstacle TEXT,
        attempts TEXT, resources TEXT, meaning_made TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS need_scores (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        seen INTEGER, cheered INTEGER, aimed INTEGER, guided INTEGER,
        volition_index INTEGER,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS affect_measurements (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        phase TEXT NOT NULL,
        q1 INTEGER, q2 INTEGER, q3 INTEGER, q4 INTEGER, q5 INTEGER,
        total INTEGER,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS conversations (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS assignments (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        assignment_text TEXT,
        excavation_query TEXT,
        commitment_person TEXT,
        commitment_when TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS ecosystem (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        person_name TEXT,
        person_type TEXT,
        needs_provided TEXT[]
      );
      CREATE TABLE IF NOT EXISTS sva_analysis (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
        bio TEXT, psycho TEXT, social TEXT, behav TEXT, narr TEXT,
        eco TEXT, phenom TEXT, epist TEXT, hist TEXT, synthesis TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(client_id, session_id)
      );
      CREATE TABLE IF NOT EXISTS groups (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        invite_code TEXT NOT NULL UNIQUE,
        created_by TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        active BOOLEAN DEFAULT TRUE
      );
      CREATE TABLE IF NOT EXISTS group_members (
        id SERIAL PRIMARY KEY,
        group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        joined_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(group_id, client_id)
      );
      CREATE TABLE IF NOT EXISTS group_messages (
        id SERIAL PRIMARY KEY,
        group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
        client_id INTEGER,
        client_name TEXT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS group_sessions (
        id SERIAL PRIMARY KEY,
        group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        started_at TIMESTAMPTZ DEFAULT NOW(),
        ended_at TIMESTAMPTZ,
        duration_seconds INTEGER,
        message_count INTEGER DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS client_tokens (
        token TEXT PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        client_name TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    // Ensure group_sessions table exists (migration for existing deployments)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS group_sessions (
        id SERIAL PRIMARY KEY,
        group_id INTEGER REFERENCES groups(id) ON DELETE CASCADE,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        started_at TIMESTAMPTZ DEFAULT NOW(),
        ended_at TIMESTAMPTZ,
        duration_seconds INTEGER,
        message_count INTEGER DEFAULT 0
      );
    `);
    console.log('Database ready');
  } catch(err) {
    console.error('DB init failed:', err.message);
    console.error('DB error code:', err.code);
    console.error('DB error detail:', err.detail);
    throw err;
  } finally {
    if (client) client.release();
  }
}

function hashPin(pin) { return String(pin); }

// DB-backed token functions (survive server restarts)
async function createToken(clientId, name) {
  const token = crypto.randomBytes(32).toString('hex');
  const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days
  await pool.query(
    'INSERT INTO client_tokens (token, client_id, client_name, expires_at) VALUES ($1,$2,$3,$4) ON CONFLICT (token) DO NOTHING',
    [token, clientId, name, expiresAt]
  );
  return token;
}

async function validateToken(token) {
  if (!token) return null;
  try {
    const r = await pool.query(
      'SELECT client_id, client_name FROM client_tokens WHERE token=$1 AND expires_at > NOW()',
      [token]
    );
    if (!r.rows.length) return null;
    return { clientId: r.rows[0].client_id, name: r.rows[0].client_name };
  } catch(e) { return null; }
}

function auth(req, res, next) {
  const token = req.headers['x-auth-token'] || req.query.token;
  validateToken(token).then(t => {
    if (!t) return res.status(401).json({ error: 'Not authenticated' });
    req.clientId = t.clientId;
    req.clientName = t.name;
    next();
  }).catch(() => res.status(401).json({ error: 'Not authenticated' }));
}

function practAuth(req, res, next) {
  const provided = (req.headers['x-practitioner-pin'] || '').trim();
  const expected = (PRACTITIONER_PIN || '').trim();
  console.log('Pract auth — provided:', JSON.stringify(provided), 'expected:', JSON.stringify(expected));
  if (provided !== expected)
    return res.status(401).json({ error: 'Invalid practitioner PIN — provided: ' + provided + ' expected length: ' + expected.length });
  next();
}

app.get('/', (req, res) => {
  res.json({ status: 'Crossing server running', version: '2.0', db: !!process.env.DATABASE_URL });
});

// Login or register by name only — no PIN
app.post('/auth/login-or-register', async (req, res) => {
  const { name } = req.body;
  if (!name || !name.trim())
    return res.status(400).json({ error: 'Please enter your name.' });
  const cleanName = name.trim();
  try {
    // Try to find existing client
    const existing = await pool.query(
      'SELECT id, name FROM clients WHERE LOWER(name)=LOWER($1)', [cleanName]
    );
    if (existing.rows.length) {
      const c = existing.rows[0];
      await pool.query('UPDATE clients SET last_seen=NOW() WHERE id=$1', [c.id]);
      const token1 = await createToken(c.id, c.name);
      return res.json({ token: token1, clientId: c.id, name: c.name, isNew: false });
    }
    // Register new client
    const result = await pool.query(
      'INSERT INTO clients (name, pin_hash) VALUES ($1, $2) RETURNING id, name',
      [cleanName, 'no-pin']
    );
    const c = result.rows[0];
    const token2 = await createToken(c.id, c.name);
    res.json({ token: token2, clientId: c.id, name: c.name, isNew: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/sessions/start', auth, async (req, res) => {
  try {
    const n = await pool.query('SELECT COUNT(*) FROM sessions WHERE client_id=$1', [req.clientId]);
    const num = parseInt(n.rows[0].count) + 1;
    const r = await pool.query(
      'INSERT INTO sessions (client_id, session_number) VALUES ($1,$2) RETURNING id',
      [req.clientId, num]
    );
    res.json({ sessionId: r.rows[0].id, sessionNumber: num });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/sessions/:id/end', auth, async (req, res) => {
  const { duration, wordCount } = req.body;
  try {
    await pool.query(
      'UPDATE sessions SET ended_at=NOW(), duration_seconds=$1, word_count=$2 WHERE id=$3 AND client_id=$4',
      [duration || 0, wordCount || 0, req.params.id, req.clientId]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/story', auth, async (req, res) => {
  const { sessionId, pointA, pointB, obstacle, attempts, resources, meaningMade } = req.body;
  try {
    await pool.query(
      'INSERT INTO story_arc (client_id,session_id,point_a,point_b,obstacle,attempts,resources,meaning_made) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
      [req.clientId, sessionId||null, pointA, pointB, obstacle, attempts, resources, meaningMade]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/needs', auth, async (req, res) => {
  const { sessionId, seen, cheered, aimed, guided, volitionIndex } = req.body;
  try {
    await pool.query(
      'INSERT INTO need_scores (client_id,session_id,seen,cheered,aimed,guided,volition_index) VALUES ($1,$2,$3,$4,$5,$6,$7)',
      [req.clientId, sessionId||null, seen, cheered, aimed, guided, volitionIndex]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/affect', auth, async (req, res) => {
  const { sessionId, phase, scores, total } = req.body;
  try {
    await pool.query(
      'INSERT INTO affect_measurements (client_id,session_id,phase,q1,q2,q3,q4,q5,total) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
      [req.clientId, sessionId||null, phase, scores.q1, scores.q2, scores.q3, scores.q4, scores.q5, total]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/conversation', auth, async (req, res) => {
  const { sessionId, role, content } = req.body;
  try {
    await pool.query(
      'INSERT INTO conversations (client_id,session_id,role,content) VALUES ($1,$2,$3,$4)',
      [req.clientId, sessionId||null, role, content]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/assignment', auth, async (req, res) => {
  const { sessionId, assignmentText, excavationQuery, commitmentPerson, commitmentWhen } = req.body;
  try {
    await pool.query(
      'INSERT INTO assignments (client_id,session_id,assignment_text,excavation_query,commitment_person,commitment_when) VALUES ($1,$2,$3,$4,$5,$6)',
      [req.clientId, sessionId||null, assignmentText, excavationQuery, commitmentPerson, commitmentWhen]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/ecosystem', auth, async (req, res) => {
  const { people } = req.body;
  try {
    await pool.query('DELETE FROM ecosystem WHERE client_id=$1', [req.clientId]);
    for (const p of (people || [])) {
      await pool.query(
        'INSERT INTO ecosystem (client_id,person_name,person_type,needs_provided) VALUES ($1,$2,$3,$4)',
        [req.clientId, p.name, p.type, p.needs || []]
      );
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/data/analysis', auth, async (req, res) => {
  const { sessionId, bio, psycho, social, behav, narr, eco, phenom, epist, hist, synthesis } = req.body;
  try {
    await pool.query(
      `INSERT INTO sva_analysis (client_id,session_id,bio,psycho,social,behav,narr,eco,phenom,epist,hist,synthesis)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       ON CONFLICT (client_id,session_id) DO UPDATE SET
         bio=$3,psycho=$4,social=$5,behav=$6,narr=$7,eco=$8,phenom=$9,epist=$10,hist=$11,synthesis=$12,created_at=NOW()`,
      [req.clientId, sessionId||null, bio, psycho, social, behav, narr, eco, phenom, epist, hist, synthesis]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/data/load', auth, async (req, res) => {
  const id = req.clientId;
  try {
    const story    = await pool.query('SELECT * FROM story_arc WHERE client_id=$1 ORDER BY updated_at DESC LIMIT 1', [id]);
    const needs    = await pool.query('SELECT * FROM need_scores WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 1', [id]);
    const assign   = await pool.query('SELECT * FROM assignments WHERE client_id=$1 ORDER BY created_at DESC LIMIT 1', [id]);
    const affect   = await pool.query('SELECT * FROM affect_measurements WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 10', [id]);
    const eco      = await pool.query('SELECT * FROM ecosystem WHERE client_id=$1', [id]);
    const lastSess = await pool.query('SELECT * FROM sessions WHERE client_id=$1 ORDER BY started_at DESC LIMIT 1', [id]);
    const weekStart = new Date();
    weekStart.setDate(weekStart.getDate() - weekStart.getDay() + 1);
    weekStart.setHours(0,0,0,0);
    const weekCount = await pool.query(
      'SELECT COUNT(*) FROM sessions WHERE client_id=$1 AND started_at>=$2',
      [id, weekStart.toISOString()]
    );
    res.json({
      story: story.rows[0] || null,
      needs: needs.rows[0] || null,
      lastAssignment: assign.rows[0] || null,
      affect: affect.rows,
      ecosystem: eco.rows,
      sessionsThisWeek: parseInt(weekCount.rows[0].count),
      lastSessionTimestamp: lastSess.rows[0]?.started_at || null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/practitioner/clients', practAuth, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT c.id, c.name, c.email, c.created_at, c.last_seen,
        COUNT(DISTINCT s.id) AS session_count,
        MAX(s.started_at) AS last_session,
        (SELECT volition_index FROM need_scores WHERE client_id=c.id ORDER BY recorded_at DESC LIMIT 1) AS latest_volition,
        (SELECT assignment_text FROM assignments WHERE client_id=c.id ORDER BY created_at DESC LIMIT 1) AS latest_assignment
      FROM clients c
      LEFT JOIN sessions s ON s.client_id=c.id
      GROUP BY c.id ORDER BY c.last_seen DESC
    `);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


app.get('/practitioner/client/:id', practAuth, async (req, res) => {
  const id = req.params.id;
  try {
    const client  = await pool.query('SELECT * FROM clients WHERE id=$1', [id]);
    if (!client.rows.length) return res.status(404).json({ error: 'Not found' });
    const sessions = await pool.query('SELECT * FROM sessions WHERE client_id=$1 ORDER BY started_at DESC', [id]);
    const story   = await pool.query('SELECT * FROM story_arc WHERE client_id=$1 ORDER BY updated_at DESC', [id]);
    const needs   = await pool.query('SELECT * FROM need_scores WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    const affect  = await pool.query('SELECT * FROM affect_measurements WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    const assigns = await pool.query('SELECT * FROM assignments WHERE client_id=$1 ORDER BY created_at DESC', [id]);
    const eco     = await pool.query('SELECT * FROM ecosystem WHERE client_id=$1', [id]);
    const convos  = await pool.query('SELECT * FROM conversations WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    const analysis = await pool.query('SELECT * FROM sva_analysis WHERE client_id=$1 ORDER BY created_at DESC LIMIT 1', [id]);
    res.json({ client: client.rows[0], sessions: sessions.rows, story: story.rows,
      needHistory: needs.rows, affectHistory: affect.rows, assignments: assigns.rows,
      ecosystem: eco.rows, conversations: convos.rows, svaAnalysis: analysis.rows[0] || null });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/practitioner/save-analysis/:id', practAuth, async (req, res) => {
  const id = req.params.id;
  const { bio, psycho, social, behav, narr, eco, phenom, epist, hist, synthesis } = req.body;
  try {
    // Delete any existing null-session analysis for this client, then insert fresh
    await pool.query('DELETE FROM sva_analysis WHERE client_id=$1 AND session_id IS NULL', [id]);
    await pool.query(
      'INSERT INTO sva_analysis (client_id,session_id,bio,psycho,social,behav,narr,eco,phenom,epist,hist,synthesis) VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
      [id, bio, psycho, social, behav, narr, eco, phenom, epist, hist, synthesis]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─────────────────────────────────────────────
// GROUP ROOM ENDPOINTS
// ─────────────────────────────────────────────

// Practitioner creates a group
app.post('/practitioner/groups/create', practAuth, async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'Group name required' });
  const code = Math.random().toString(36).slice(2, 8).toUpperCase();
  try {
    const r = await pool.query(
      'INSERT INTO groups (name, invite_code, created_by) VALUES ($1, $2, $3) RETURNING *',
      [name, code, 'practitioner']
    );
    res.json(r.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Practitioner lists all groups
app.get('/practitioner/groups', practAuth, async (req, res) => {
  try {
    const groups = await pool.query('SELECT * FROM groups ORDER BY created_at DESC');
    const result = [];
    for (const g of groups.rows) {
      const members = await pool.query(
        'SELECT c.id, c.name FROM group_members gm JOIN clients c ON c.id=gm.client_id WHERE gm.group_id=$1',
        [g.id]
      );
      const msgCount = await pool.query('SELECT COUNT(*) FROM group_messages WHERE group_id=$1', [g.id]);
      result.push({ ...g, members: members.rows, message_count: parseInt(msgCount.rows[0].count) });
    }
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Practitioner gets full group conversation
app.get('/practitioner/groups/:id/messages', practAuth, async (req, res) => {
  try {
    const msgs = await pool.query(
      'SELECT * FROM group_messages WHERE group_id=$1 ORDER BY recorded_at ASC',
      [req.params.id]
    );
    res.json(msgs.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Practitioner deletes a group
app.delete('/practitioner/groups/:id', practAuth, async (req, res) => {
  try {
    await pool.query('DELETE FROM groups WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Practitioner: delete duplicate consecutive Guide messages from a group
app.delete('/practitioner/groups/:id/duplicate-messages', practAuth, async (req, res) => {
  try {
    // Delete all but the FIRST Guide message in this group
    await pool.query(`
      DELETE FROM group_messages
      WHERE group_id=$1 AND role='assistant'
      AND id NOT IN (
        SELECT id FROM group_messages
        WHERE group_id=$1 AND role='assistant'
        ORDER BY recorded_at ASC
        LIMIT 1
      )
    `, [req.params.id]);
    res.json({ ok: true });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Client joins a group via invite code
app.post('/group/join', auth, async (req, res) => {
  const { code } = req.body;
  if (!code) return res.status(400).json({ error: 'Invite code required' });
  try {
    const g = await pool.query('SELECT * FROM groups WHERE UPPER(invite_code)=UPPER($1) AND active=TRUE', [code]);
    if (!g.rows.length) return res.status(404).json({ error: 'Invalid or expired invite code' });
    const group = g.rows[0];
    await pool.query(
      'INSERT INTO group_members (group_id, client_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [group.id, req.clientId]
    );
    res.json({ ok: true, group: { id: group.id, name: group.name } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client gets their groups
app.get('/group/mine', auth, async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT g.* FROM groups g JOIN group_members gm ON gm.group_id=g.id WHERE gm.client_id=$1 AND g.active=TRUE ORDER BY g.created_at DESC',
      [req.clientId]
    );
    res.json(r.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Start a group session (called when client opens the group room)
app.post('/group/:id/session/start', auth, async (req, res) => {
  try {
    console.log('[session/start] called, client=', req.clientId, 'group=', req.params.id);
    const mem = await pool.query('SELECT 1 FROM group_members WHERE group_id=$1 AND client_id=$2', [req.params.id, req.clientId]);
    if (!mem.rows.length) return res.status(403).json({ error: 'Not a member' });
    // Check for an already-open session (ended_at IS NULL) — don't double-open
    const open = await pool.query(
      'SELECT id, started_at FROM group_sessions WHERE group_id=$1 AND client_id=$2 AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1',
      [req.params.id, req.clientId]
    );
    if (open.rows.length) return res.json({ ok: true, session_id: open.rows[0].id, started_at: open.rows[0].started_at });
    const r = await pool.query(
      'INSERT INTO group_sessions (group_id, client_id) VALUES ($1,$2) RETURNING id, started_at',
      [req.params.id, req.clientId]
    );
    res.json({ ok: true, session_id: r.rows[0].id, started_at: r.rows[0].started_at });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// End a group session (called when client clicks End Session)
app.post('/group/:id/session/end', auth, async (req, res) => {
  try {
    const { session_id } = req.body;
    console.log('[session/end] called, session_id=', session_id, 'client=', req.clientId, 'group=', req.params.id);

    // If no session_id (start failed), create one now retroactively
    let resolvedSessionId = session_id;
    let startedAt;
    if (!session_id) {
      const created = await pool.query(
        'INSERT INTO group_sessions (group_id, client_id) VALUES ($1,$2) RETURNING id, started_at',
        [req.params.id, req.clientId]
      );
      resolvedSessionId = created.rows[0].id;
      startedAt = created.rows[0].started_at;
    } else {
      const sess = await pool.query('SELECT started_at FROM group_sessions WHERE id=$1 AND client_id=$2', [resolvedSessionId, req.clientId]);
      if (!sess.rows.length) return res.status(404).json({ error: 'Session not found' });
      startedAt = sess.rows[0].started_at;
    }
    const msgCount = await pool.query(
      'SELECT COUNT(*) FROM group_messages WHERE group_id=$1 AND client_id=$2 AND recorded_at >= $3',
      [req.params.id, req.clientId, startedAt]
    );
    const now = new Date();
    const durationSeconds = Math.round((now - new Date(startedAt)) / 1000);
    await pool.query(
      'UPDATE group_sessions SET ended_at=$1, duration_seconds=$2, message_count=$3 WHERE id=$4',
      [now, durationSeconds, parseInt(msgCount.rows[0].count), resolvedSessionId]
    );
    console.log('[session/end] saved, duration=', durationSeconds, 'msgs=', msgCount.rows[0].count);
    res.json({ ok: true, duration_seconds: durationSeconds });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Get group session stats for a client (how many sessions, total time, return visits)
app.get('/group/:id/session/stats', auth, async (req, res) => {
  try {
    const mem = await pool.query('SELECT 1 FROM group_members WHERE group_id=$1 AND client_id=$2', [req.params.id, req.clientId]);
    if (!mem.rows.length) return res.status(403).json({ error: 'Not a member' });
    const stats = await pool.query(
      `SELECT
        COUNT(*) as total_sessions,
        COALESCE(SUM(duration_seconds),0) as total_seconds,
        COALESCE(AVG(duration_seconds),0) as avg_seconds,
        MAX(ended_at) as last_session,
        COALESCE(SUM(message_count),0) as total_messages
       FROM group_sessions
       WHERE group_id=$1 AND client_id=$2 AND ended_at IS NOT NULL`,
      [req.params.id, req.clientId]
    );
    res.json(stats.rows[0]);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Practitioner: get group session stats derived from message timestamps (no group_sessions table needed)
app.get('/practitioner/groups/:id/sessions', practAuth, async (req, res) => {
  try {
    // Get all non-Guide messages with timestamps, grouped by client
    const r = await pool.query(
      `SELECT
        client_id, client_name,
        recorded_at,
        TO_CHAR(recorded_at AT TIME ZONE 'Africa/Nairobi', 'YYYY-MM-DD') as day,
        TO_CHAR(recorded_at AT TIME ZONE 'Africa/Nairobi', 'DD Mon YYYY') as day_display,
        TO_CHAR(recorded_at AT TIME ZONE 'Africa/Nairobi', 'HH24:MI') as time_str
       FROM group_messages
       WHERE group_id=$1 AND role='user'
       ORDER BY client_id, recorded_at ASC`,
      [req.params.id]
    );

    // Group messages by client then by day to infer sessions
    const byClient = {};
    r.rows.forEach(row => {
      if (!byClient[row.client_name]) byClient[row.client_name] = {};
      if (!byClient[row.client_name][row.day]) byClient[row.client_name][row.day] = [];
      byClient[row.client_name][row.day].push(row);
    });

    // Build session summaries per client per day
    const sessions = [];
    Object.entries(byClient).forEach(([clientName, days]) => {
      Object.entries(days).forEach(([day, msgs]) => {
        const first = msgs[0];
        const last = msgs[msgs.length - 1];
        const startTime = new Date(first.recorded_at);
        const endTime = new Date(last.recorded_at);
        const durationSeconds = Math.round((endTime - startTime) / 1000);
        sessions.push({
          client_name: clientName,
          session_date: first.day_display,
          start_time: first.time_str,
          end_time: last.time_str,
          duration_seconds: durationSeconds,
          message_count: msgs.length
        });
      });
    });

    // Sort by date desc
    sessions.sort((a, b) => b.session_date.localeCompare(a.session_date));
    res.json(sessions);
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// Client polls for messages since a timestamp
app.get('/group/:id/messages', auth, async (req, res) => {
  const since = req.query.since || '1970-01-01';
  try {
    // Verify membership
    const mem = await pool.query('SELECT 1 FROM group_members WHERE group_id=$1 AND client_id=$2', [req.params.id, req.clientId]);
    if (!mem.rows.length) return res.status(403).json({ error: 'Not a member of this group' });
    const msgs = await pool.query(
      'SELECT * FROM group_messages WHERE group_id=$1 AND recorded_at > $2 ORDER BY recorded_at ASC',
      [req.params.id, since]
    );
    res.json(msgs.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Client sends a message — triggers Guide response
app.post('/group/:id/send', auth, async (req, res) => {
  const groupId = req.params.id;
  const { content } = req.body;
  if (!content) return res.status(400).json({ error: 'Message required' });
  try {
    // Verify membership
    const mem = await pool.query('SELECT 1 FROM group_members WHERE group_id=$1 AND client_id=$2', [groupId, req.clientId]);
    if (!mem.rows.length) return res.status(403).json({ error: 'Not a member' });

    // Prevent concurrent Guide responses for this group
    if (groupGuideLocks.has(groupId)) {
      // Still save the client message, just skip Guide response this time
      await pool.query(
        'INSERT INTO group_messages (group_id, client_id, client_name, role, content) VALUES ($1,$2,$3,$4,$5)',
        [groupId, req.clientId, req.clientName, 'user', content]
      );
      return res.json({ ok: true, queued: true });
    }
    groupGuideLocks.add(groupId);

    // Save client message
    await pool.query(
      'INSERT INTO group_messages (group_id, client_id, client_name, role, content) VALUES ($1,$2,$3,$4,$5)',
      [groupId, req.clientId, req.clientName, 'user', content]
    );

    // Respond to client immediately — Guide runs async so Railway timeout never triggers retry
    res.json({ ok: true });

    // Get group name and recent conversation history (last 30 messages)
    const groupInfo = await pool.query('SELECT name FROM groups WHERE id=$1', [groupId]);
    const history = await pool.query(
      'SELECT client_name, role, content FROM group_messages WHERE group_id=$1 ORDER BY recorded_at DESC LIMIT 30',
      [groupId]
    );
    const msgs = history.rows.reverse();

    // Get member list
    const members = await pool.query(
      'SELECT c.name FROM group_members gm JOIN clients c ON c.id=gm.client_id WHERE gm.group_id=$1',
      [groupId]
    );
    const memberNames = members.rows.map(m => m.name).join(', ');

    // Build Guide system prompt for group
    const systemPrompt = `ABSOLUTE RULE: Never use asterisks. Never write stage directions or embodied actions like *pausing*, *nodding*, *leaning in*, *turning back*, *warmth spreading*, *smiling*, or any similar physical description. You are text only. Your presence is in your words, not your body. If you include any asterisk-based action, you have failed this instruction.

You are the Guide in a therapeutic group conversation grounded in the Scaffolded Volition Approach (SVA), VEMIS framework, and Ubuntu philosophy.

GROUP: "${groupInfo.rows[0]?.name}"
MEMBERS PRESENT: ${memberNames}

YOUR ROLE IN THE GROUP:
- You are facilitating — not counselling individuals — in this shared space
- Notice patterns across what different members share
- Ask questions that invite the group to think together, not just respond to one person
- Never reveal one member's private session data to others
- Use the four needs framework (Seen, Cheered, Aimed, Guided) to sense what the group collectively needs
- Name what you notice in the group — the silences, the themes, the echoes between members
- Ubuntu principle: a person is a person through persons — hold the communal dimension always
- Keep responses warm, unhurried, and focused. 2-4 sentences is usually enough.
- Address the person who just spoke AND occasionally invite others to respond
- Do not try to fix or solve. Hold and witness.
- ONE question at a time maximum. Short responses. The silence you leave matters as much as what you say.`;

    // Build messages array
    const apiMessages = msgs.map(m => ({
      role: m.role === 'user' ? 'user' : 'assistant',
      content: m.role === 'user' ? `${m.client_name}: ${m.content}` : m.content
    }));

    // Call Anthropic API
    if (!ANTHROPIC_API_KEY) {
      const fallback = 'I am here with all of you.';
      await pool.query(
        'INSERT INTO group_messages (group_id, client_id, client_name, role, content) VALUES ($1,NULL,$2,$3,$4)',
        [groupId, 'The Guide', 'assistant', fallback]
      );
      groupGuideLocks.delete(groupId);
      return;
    }

    const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-opus-4-5',
        max_tokens: 400,
        system: systemPrompt,
        messages: apiMessages
      })
    });

    const aiData = await aiRes.json();
    const guideText = aiData.content?.[0]?.text || 'I am here with all of you.';

    // Save Guide response
    await pool.query(
      'INSERT INTO group_messages (group_id, client_id, client_name, role, content) VALUES ($1,NULL,$2,$3,$4)',
      [groupId, 'The Guide', 'assistant', guideText]
    );

    groupGuideLocks.delete(groupId);
  } catch (err) {
    groupGuideLocks.delete(groupId);
    console.error('Group send error:', err.message);
  }
});

app.post('/api/messages', async (req, res) => {
  if (!ANTHROPIC_API_KEY)
    return res.status(500).json({ error: { message: 'ANTHROPIC_API_KEY not set on server.' } });
  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify(req.body)
    });
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (err) {
    res.status(500).json({ error: { message: 'Proxy error: ' + err.message } });
  }
});

initDB().then(() => {
  app.listen(PORT, () => console.log(`Crossing server v2 on port ${PORT}`));
}).catch(err => {
  console.error('DB init failed:', err.message);
  app.listen(PORT, () => console.log(`Crossing server (no DB) on port ${PORT}`));
});

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const PRACTITIONER_PIN = process.env.PRACTITIONER_PIN || '0000';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

app.use(cors());
app.use(express.json({ limit: '4mb' }));

// =====================================================================
// DATABASE SETUP
// =====================================================================
async function initDB() {
  const client = await pool.connect();
  try {
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
    `);
    console.log('Database ready');
  } finally {
    client.release();
  }
}

// =====================================================================
// HELPERS
// =====================================================================
function hashPin(pin) {
  return crypto.createHash('sha256').update(pin + 'crossing_sva_2025').digest('hex');
}

const tokens = new Map();

function createToken(clientId, name) {
  const token = crypto.randomBytes(32).toString('hex');
  tokens.set(token, { clientId, name, expires: Date.now() + 7 * 24 * 60 * 60 * 1000 });
  return token;
}

function validateToken(token) {
  if (!token) return null;
  const t = tokens.get(token);
  if (!t || Date.now() > t.expires) { tokens.delete(token); return null; }
  return t;
}

function auth(req, res, next) {
  const t = validateToken(req.headers['x-auth-token']);
  if (!t) return res.status(401).json({ error: 'Not authenticated' });
  req.clientId = t.clientId;
  req.clientName = t.name;
  next();
}

function practAuth(req, res, next) {
  if (req.headers['x-practitioner-pin'] !== PRACTITIONER_PIN)
    return res.status(401).json({ error: 'Invalid practitioner PIN' });
  next();
}

// =====================================================================
// HEALTH
// =====================================================================
app.get('/', (req, res) => {
  res.json({ status: 'Crossing server running', version: '2.0' });
});

// =====================================================================
// AUTH
// =====================================================================
app.post('/auth/register', async (req, res) => {
  const { name, pin } = req.body;
  if (!name || !pin || String(pin).length < 4)
    return res.status(400).json({ error: 'Name and a 4-digit PIN are required.' });
  try {
    const result = await pool.query(
      'INSERT INTO clients (name, pin_hash) VALUES ($1, $2) RETURNING id, name',
      [name.trim(), hashPin(String(pin))]
    );
    const c = result.rows[0];
    res.json({ token: createToken(c.id, c.name), clientId: c.id, name: c.name });
  } catch (err) {
    if (err.code === '23505')
      return res.status(409).json({ error: 'That name is already registered. Try logging in instead.' });
    res.status(500).json({ error: err.message });
  }
});

app.post('/auth/login', async (req, res) => {
  const { name, pin } = req.body;
  if (!name || !pin) return res.status(400).json({ error: 'Name and PIN required.' });
  try {
    const result = await pool.query(
      'SELECT id, name FROM clients WHERE LOWER(name)=LOWER($1) AND pin_hash=$2',
      [name.trim(), hashPin(String(pin))]
    );
    if (!result.rows.length)
      return res.status(401).json({ error: 'Name or PIN not recognised.' });
    const c = result.rows[0];
    await pool.query('UPDATE clients SET last_seen=NOW() WHERE id=$1', [c.id]);
    res.json({ token: createToken(c.id, c.name), clientId: c.id, name: c.name });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =====================================================================
// SESSIONS
// =====================================================================
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

// =====================================================================
// DATA SAVE
// =====================================================================
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

// =====================================================================
// DATA LOAD (restore on login)
// =====================================================================
app.get('/data/load', auth, async (req, res) => {
  const id = req.clientId;
  try {
    const story     = await pool.query('SELECT * FROM story_arc WHERE client_id=$1 ORDER BY updated_at DESC LIMIT 1', [id]);
    const needs     = await pool.query('SELECT * FROM need_scores WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 1', [id]);
    const assign    = await pool.query('SELECT * FROM assignments WHERE client_id=$1 ORDER BY created_at DESC LIMIT 1', [id]);
    const affect    = await pool.query('SELECT * FROM affect_measurements WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 10', [id]);
    const eco       = await pool.query('SELECT * FROM ecosystem WHERE client_id=$1', [id]);
    const lastSess  = await pool.query('SELECT * FROM sessions WHERE client_id=$1 ORDER BY started_at DESC LIMIT 1', [id]);

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

// =====================================================================
// PRACTITIONER DASHBOARD
// =====================================================================
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
    const client   = await pool.query('SELECT * FROM clients WHERE id=$1', [id]);
    if (!client.rows.length) return res.status(404).json({ error: 'Not found' });
    const sessions = await pool.query('SELECT * FROM sessions WHERE client_id=$1 ORDER BY started_at DESC', [id]);
    const story    = await pool.query('SELECT * FROM story_arc WHERE client_id=$1 ORDER BY updated_at DESC', [id]);
    const needs    = await pool.query('SELECT * FROM need_scores WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    const affect   = await pool.query('SELECT * FROM affect_measurements WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    const assigns  = await pool.query('SELECT * FROM assignments WHERE client_id=$1 ORDER BY created_at DESC', [id]);
    const eco      = await pool.query('SELECT * FROM ecosystem WHERE client_id=$1', [id]);
    const convos   = await pool.query('SELECT * FROM conversations WHERE client_id=$1 ORDER BY recorded_at ASC', [id]);
    res.json({ client: client.rows[0], sessions: sessions.rows, story: story.rows,
      needHistory: needs.rows, affectHistory: affect.rows, assignments: assigns.rows,
      ecosystem: eco.rows, conversations: convos.rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// =====================================================================
// ANTHROPIC PROXY
// =====================================================================
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

// =====================================================================
// START
// =====================================================================
initDB().then(() => {
  app.listen(PORT, () => console.log(`Crossing server v2 on port ${PORT}`));
}).catch(err => {
  console.error('DB init failed:', err.message);
  app.listen(PORT, () => console.log(`Crossing server (no DB) on port ${PORT}`));
});

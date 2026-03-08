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
      CREATE TABLE IF NOT EXISTS arc_readings (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        result JSONB NOT NULL,
        generated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS eco_reflections (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        person_name TEXT,
        emoji_response TEXT,
        emoji_label TEXT,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS masking_scores (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        masking_load INTEGER,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
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
      CREATE TABLE IF NOT EXISTS persistent_profiles (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE UNIQUE,
        volition_index INTEGER,
        seen_score INTEGER, cheered_score INTEGER, aimed_score INTEGER, guided_score INTEGER,
        masking_trend JSONB DEFAULT '[]',
        active_patterns JSONB DEFAULT '[]',
        risk_flags JSONB DEFAULT '[]',
        next_priorities JSONB DEFAULT '[]',
        last_assignment TEXT,
        last_assignment_status TEXT DEFAULT 'pending',
        session_count INTEGER DEFAULT 0,
        last_session_summary TEXT,
        profile_staleness BOOLEAN DEFAULT FALSE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS session_archives (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        session_number INTEGER,
        compressed_summary TEXT,
        raw_transcript_length INTEGER,
        assignment_given TEXT,
        affect_before INTEGER,
        affect_after INTEGER,
        masking_load INTEGER,
        volition_index INTEGER,
        archived_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS session_architectures (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        movement_priorities JSONB,
        risk_flags JSONB DEFAULT '[]',
        opening_question TEXT,
        hypothesis_label TEXT,
        override_conditions TEXT,
        generated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS sovereign_moments (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        moment_text TEXT NOT NULL,
        detection_tier INTEGER DEFAULT 2,
        confirmed BOOLEAN DEFAULT FALSE,
        dismissed BOOLEAN DEFAULT FALSE,
        practitioner_note TEXT,
        detected_at TIMESTAMPTZ DEFAULT NOW()
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
    // Migrations for new tables on existing deployments
    await pool.query(`
      CREATE TABLE IF NOT EXISTS arc_readings (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        result JSONB NOT NULL,
        generated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS eco_reflections (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        person_name TEXT,
        emoji_response TEXT,
        emoji_label TEXT,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS masking_scores (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        masking_load INTEGER,
        recorded_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    // Phase 1 migrations
    await pool.query(`
      CREATE TABLE IF NOT EXISTS persistent_profiles (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE UNIQUE,
        volition_index INTEGER,
        seen_score INTEGER, cheered_score INTEGER, aimed_score INTEGER, guided_score INTEGER,
        masking_trend JSONB DEFAULT '[]',
        active_patterns JSONB DEFAULT '[]',
        risk_flags JSONB DEFAULT '[]',
        next_priorities JSONB DEFAULT '[]',
        last_assignment TEXT,
        last_assignment_status TEXT DEFAULT 'pending',
        session_count INTEGER DEFAULT 0,
        last_session_summary TEXT,
        profile_staleness BOOLEAN DEFAULT FALSE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS session_archives (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        session_number INTEGER,
        compressed_summary TEXT,
        raw_transcript_length INTEGER,
        assignment_given TEXT,
        affect_before INTEGER,
        affect_after INTEGER,
        masking_load INTEGER,
        volition_index INTEGER,
        archived_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS session_architectures (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        movement_priorities JSONB,
        risk_flags JSONB DEFAULT '[]',
        opening_question TEXT,
        hypothesis_label TEXT,
        override_conditions TEXT,
        generated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS sovereign_moments (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        moment_text TEXT NOT NULL,
        detection_tier INTEGER DEFAULT 2,
        confirmed BOOLEAN DEFAULT FALSE,
        dismissed BOOLEAN DEFAULT FALSE,
        practitioner_note TEXT,
        detected_at TIMESTAMPTZ DEFAULT NOW()
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

// Bulk save all conversation messages at once (used at session end for reliability)
app.post('/data/conversation/bulk', auth, async (req, res) => {
  const { sessionId, messages } = req.body;
  if (!Array.isArray(messages) || !messages.length) return res.json({ ok: true, saved: 0 });
  try {
    // Check how many messages already saved for this session to avoid duplicates
    const existing = sessionId ? await pool.query(
      'SELECT COUNT(*) FROM conversations WHERE client_id=$1 AND session_id=$2',
      [req.clientId, sessionId]
    ) : { rows: [{ count: '0' }] };
    const alreadySaved = parseInt(existing.rows[0].count);
    // Only save messages we haven't saved yet
    const toSave = messages.slice(alreadySaved);
    for (const m of toSave) {
      await pool.query(
        'INSERT INTO conversations (client_id,session_id,role,content) VALUES ($1,$2,$3,$4)',
        [req.clientId, sessionId||null, m.role, m.content]
      );
    }
    res.json({ ok: true, saved: toSave.length });
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

// Life Arc reading — save result JSON from Movement Six
app.post('/data/arc-reading', auth, async (req, res) => {
  const { sessionId, result } = req.body;
  if (!result) return res.status(400).json({ error: 'No result provided' });
  try {
    // Upsert: one arc reading per client (keep most recent)
    await pool.query('DELETE FROM arc_readings WHERE client_id=$1', [req.clientId]);
    await pool.query(
      'INSERT INTO arc_readings (client_id,session_id,result) VALUES ($1,$2,$3)',
      [req.clientId, sessionId || null, JSON.stringify(result)]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Ecosystem micro-reflection — emoji responses after adding a person
app.post('/data/eco-reflection', auth, async (req, res) => {
  const { sessionId, person, response, emoji } = req.body;
  try {
    await pool.query(
      'INSERT INTO eco_reflections (client_id,session_id,person_name,emoji_response,emoji_label) VALUES ($1,$2,$3,$4,$5)',
      [req.clientId, sessionId || null, person, emoji, response]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Masking load score
app.post('/data/masking', auth, async (req, res) => {
  const { sessionId, maskingLoad } = req.body;
  try {
    await pool.query(
      'INSERT INTO masking_scores (client_id,session_id,masking_load) VALUES ($1,$2,$3)',
      [req.clientId, sessionId || null, maskingLoad]
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
    const arcReading = await pool.query('SELECT * FROM arc_readings WHERE client_id=$1 ORDER BY generated_at DESC LIMIT 1', [id]);
    const arcResult = arcReading.rows[0] ? { ...arcReading.rows[0].result, generated_at: arcReading.rows[0].generated_at } : null;
    // Phase 1 data
    const persistentProfile = await pool.query('SELECT * FROM persistent_profiles WHERE client_id=$1', [id]);
    const sovereignMoments  = await pool.query('SELECT * FROM sovereign_moments WHERE client_id=$1 ORDER BY detected_at DESC', [id]);
    const sessionArchives   = await pool.query('SELECT * FROM session_archives WHERE client_id=$1 ORDER BY archived_at DESC', [id]);
    res.json({ client: client.rows[0], sessions: sessions.rows, story: story.rows,
      needHistory: needs.rows, affectHistory: affect.rows, assignments: assigns.rows,
      ecosystem: eco.rows, conversations: convos.rows, svaAnalysis: analysis.rows[0] || null,
      arcReading: arcResult,
      persistentProfile: persistentProfile.rows[0] || null,
      sovereignMoments: sovereignMoments.rows,
      sessionArchives: sessionArchives.rows });
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

// ─────────────────────────────────────────────
// PHASE 1: PERSISTENT MEMORY ENDPOINTS
// ─────────────────────────────────────────────

// GET persistent profile for authenticated client — used by Guide at session start
app.get('/data/profile', auth, async (req, res) => {
  try {
    const profile = await pool.query(
      'SELECT * FROM persistent_profiles WHERE client_id=$1', [req.clientId]
    );
    const sessionCount = await pool.query(
      'SELECT COUNT(*) FROM sessions WHERE client_id=$1', [req.clientId]
    );
    const architecture = await pool.query(
      `SELECT sa.* FROM session_architectures sa
       JOIN sessions s ON s.id = sa.session_id
       WHERE sa.client_id=$1
       ORDER BY sa.generated_at DESC LIMIT 1`,
      [req.clientId]
    );
    res.json({
      profile: profile.rows[0] || null,
      sessionCount: parseInt(sessionCount.rows[0].count),
      architecture: architecture.rows[0] || null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// POST /sessions/:id/post-session-jobs — runs archive builder, profile updater, sovereign moment detector
// Called by client after session end. Runs async — responds immediately, jobs run in background.
app.post('/sessions/:id/post-session-jobs', auth, async (req, res) => {
  const sessionId = parseInt(req.params.id);
  const clientId = req.clientId;
  res.json({ ok: true, queued: true });

  // Run jobs async — failures are logged but never propagate to client
  runPostSessionJobs(clientId, sessionId).catch(err => {
    console.error('[post-session-jobs] fatal error for client', clientId, 'session', sessionId, ':', err.message);
  });
});

async function runPostSessionJobs(clientId, sessionId) {
  console.log('[post-session-jobs] starting for client', clientId, 'session', sessionId);

  if (!ANTHROPIC_API_KEY) {
    console.warn('[post-session-jobs] no API key — skipping AI jobs, writing raw archive');
    await writeRawArchive(clientId, sessionId);
    return;
  }

  // Job 1: Archive Builder
  let summary = null;
  try {
    summary = await buildSessionArchive(clientId, sessionId);
    console.log('[post-session-jobs] Job 1 (archive) complete');
  } catch (err) {
    console.error('[post-session-jobs] Job 1 (archive) failed:', err.message);
    await writeRawArchive(clientId, sessionId);
  }

  // Job 2: Profile Updater
  try {
    await updatePersistentProfile(clientId, sessionId, summary);
    console.log('[post-session-jobs] Job 2 (profile) complete');
  } catch (err) {
    console.error('[post-session-jobs] Job 2 (profile) failed:', err.message);
    // Mark profile stale so practitioner is aware
    try {
      await pool.query(
        `INSERT INTO persistent_profiles (client_id, profile_staleness) VALUES ($1, TRUE)
         ON CONFLICT (client_id) DO UPDATE SET profile_staleness=TRUE`,
        [clientId]
      );
    } catch (e) { console.error('[post-session-jobs] could not mark profile stale:', e.message); }
  }

  // Job 3: Sovereign Moment Detector
  try {
    await detectSovereignMoments(clientId, sessionId);
    console.log('[post-session-jobs] Job 3 (sovereign moments) complete');
  } catch (err) {
    console.error('[post-session-jobs] Job 3 (sovereign moments) failed:', err.message);
    // Mark session for manual review — log only, no DB write needed
    console.warn('[post-session-jobs] session', sessionId, 'flagged for manual sovereign moment review');
  }
}

async function writeRawArchive(clientId, sessionId) {
  try {
    const sess = await pool.query('SELECT * FROM sessions WHERE id=$1 AND client_id=$2', [sessionId, clientId]);
    if (!sess.rows.length) return;
    const s = sess.rows[0];
    const convos = await pool.query(
      'SELECT COUNT(*) FROM conversations WHERE session_id=$1 AND client_id=$2', [sessionId, clientId]
    );
    await pool.query(
      `INSERT INTO session_archives (client_id, session_id, session_number, raw_transcript_length, archived_at)
       VALUES ($1,$2,$3,$4,NOW())
       ON CONFLICT DO NOTHING`,
      [clientId, sessionId, s.session_number, parseInt(convos.rows[0].count)]
    );
  } catch (err) { console.error('[writeRawArchive] failed:', err.message); }
}

async function buildSessionArchive(clientId, sessionId) {
  const sess = await pool.query('SELECT * FROM sessions WHERE id=$1 AND client_id=$2', [sessionId, clientId]);
  if (!sess.rows.length) throw new Error('Session not found');
  const s = sess.rows[0];

  const convos = await pool.query(
    'SELECT role, content FROM conversations WHERE session_id=$1 AND client_id=$2 ORDER BY recorded_at ASC',
    [sessionId, clientId]
  );

  // Assignment: try session_id match first, then fall back to most recent within 2 hours of session start
  let assign = await pool.query(
    'SELECT assignment_text FROM assignments WHERE session_id=$1 AND client_id=$2 ORDER BY created_at DESC LIMIT 1',
    [sessionId, clientId]
  );
  if (!assign.rows.length) {
    assign = await pool.query(
      `SELECT assignment_text FROM assignments
       WHERE client_id=$1
         AND created_at >= $2::timestamptz - interval '2 hours'
       ORDER BY created_at DESC LIMIT 1`,
      [clientId, s.started_at]
    );
  }
  // Final fallback: most recent assignment for this client at all
  if (!assign.rows.length) {
    assign = await pool.query(
      'SELECT assignment_text FROM assignments WHERE client_id=$1 ORDER BY created_at DESC LIMIT 1',
      [clientId]
    );
  }

  // Affect: try session_id first, then fall back to time window
  let affects = await pool.query(
    'SELECT phase, total, q1, q2, q3, q4, q5 FROM affect_measurements WHERE session_id=$1 AND client_id=$2 ORDER BY recorded_at ASC',
    [sessionId, clientId]
  );
  if (!affects.rows.length) {
    affects = await pool.query(
      `SELECT phase, total, q1, q2, q3, q4, q5 FROM affect_measurements
       WHERE client_id=$1
         AND recorded_at >= $2::timestamptz - interval '2 hours'
       ORDER BY recorded_at ASC`,
      [clientId, s.started_at]
    );
  }

  const masking = await pool.query(
    'SELECT masking_load FROM masking_scores WHERE session_id=$1 AND client_id=$2 ORDER BY recorded_at DESC LIMIT 1',
    [sessionId, clientId]
  );
  const needs = await pool.query(
    'SELECT volition_index FROM need_scores WHERE session_id=$1 AND client_id=$2 ORDER BY recorded_at DESC LIMIT 1',
    [sessionId, clientId]
  );

  const transcriptText = convos.rows.map(r => (r.role === 'user' ? 'PERSON: ' : 'GUIDE: ') + r.content).join('\n');
  const assignmentGiven = assign.rows[0]?.assignment_text || null;
  const befRow = affects.rows.find(a => a.phase === 'before') || null;
  const aftRow = affects.rows.find(a => a.phase === 'after') || null;
  // Store raw totals for legacy compatibility
  const affectBefore = befRow?.total ?? null;
  const affectAfter  = aftRow?.total ?? null;
  const maskingLoad  = masking.rows[0]?.masking_load ?? null;
  const volitionIndex = needs.rows[0]?.volition_index || null;

  // Correct volitional shift formula:
  // q1 (heaviness) and q2 (aloneness): improvement = before - after (lower is better)
  // q3 (clarity), q4 (hope), q5 (seen): improvement = after - before (higher is better)
  // Shift % = sum of gains / max possible gains * 100
  // Max possible = 5 points per question * 5 questions = 25
  let affectShiftPct = null;
  if (befRow && aftRow) {
    const q1gain = Math.max(0, (befRow.q1 || 0) - (aftRow.q1 || 0));
    const q2gain = Math.max(0, (befRow.q2 || 0) - (aftRow.q2 || 0));
    const q3gain = Math.max(0, (aftRow.q3 || 0) - (befRow.q3 || 0));
    const q4gain = Math.max(0, (aftRow.q4 || 0) - (befRow.q4 || 0));
    const q5gain = Math.max(0, (aftRow.q5 || 0) - (befRow.q5 || 0));
    const totalGain = q1gain + q2gain + q3gain + q4gain + q5gain;
    // Max possible from this starting point (not from zero — from where they were)
    const maxFromBefore =
      (befRow.q1 || 0) +    // q1: max gain = before value (reduce to 0)
      (befRow.q2 || 0) +    // q2: max gain = before value
      (5 - (befRow.q3 || 0)) + // q3: max gain = 5 minus before
      (5 - (befRow.q4 || 0)) + // q4: max gain = 5 minus before
      (5 - (befRow.q5 || 0));  // q5: max gain = 5 minus before
    // Fall back to absolute max (25) if maxFromBefore is 0
    const divisor = maxFromBefore > 0 ? maxFromBefore : 25;
    affectShiftPct = Math.round((totalGain / divisor) * 1000) / 10; // one decimal place
  } else if (affectBefore !== null && affectAfter !== null) {
    // Legacy fallback: only totals available — use simple formula
    affectShiftPct = Math.round(((affectAfter - affectBefore) / 25) * 100 * 10) / 10;
  }

  // Generate compressed summary — include actual assignment text so AI doesn't say "none"
  const summaryPrompt = `You are summarising a therapeutic session for a persistent memory system.
The summary must be under 200 tokens and capture only structural essentials.
Format exactly as:
THEME: [one sentence — the emotional/psychological core of what the person was crossing today]
MOVEMENT: [one sentence — what actually shifted in this session, with honest weight. Do not default to "slight." A self-renaming, a lifted head, a new frame for one's life — these are significant movements even if quiet. Name what moved and how much.]
ASSIGNMENT: [the exact assignment given below, compressed to one sentence — do not write "none" if an assignment is provided]
FLAGS: [clinical risk signals only — see rules below]
PATTERNS: [any recurring patterns visible — or "none"]

Calibration note on MOVEMENT: "Slight" should only be used if genuinely nothing shifted. If the person named themselves, reached toward something, or showed any change in volitional capacity, that is moderate-to-significant movement. Match the weight of the movement to the evidence.

CRITICAL FLAG CLASSIFICATION RULES:
Before generating FLAGS, classify each potential flag as one of:
- INTERNAL STATE: Person describing their own psychological/emotional/bodily experience → may be clinically significant
- EXTERNAL EVENT REPORT: Person describing something that happened outside them (e.g. "you have frozen", "the screen went blank", "my phone rang") → DO NOT flag clinically; note as session event only if it interrupted a significant moment
- TECHNICAL REPORT: Person commenting on the session technology or connection → exclude entirely from clinical flags

Only INTERNAL STATE language can generate clinical flags. External event reports must never generate dissociation, overwhelm, or psychological risk flags.

CONNECTIVITY EVENTS: If the transcript shows the person saying something like "you have frozen", "you disappeared", "are you there", "the connection dropped" — this is a connectivity rupture, not a clinical signal. Note it as: "Connectivity rupture occurred mid-session — learner named it and remained present. Review transcript at reconnection point to assess whether repair was made."

ASSIGNMENT GIVEN THIS SESSION:
${assignmentGiven || 'No assignment was given this session.'}

SESSION TRANSCRIPT:
${transcriptText.slice(0, 4000)}`;

  const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 250, messages: [{ role: 'user', content: summaryPrompt }] })
  });
  const aiData = await aiRes.json();
  if (aiData.error) throw new Error('AI error: ' + aiData.error.message);
  const summary = aiData.content?.[0]?.text?.trim() || null;

  await pool.query(
    `INSERT INTO session_archives
       (client_id, session_id, session_number, compressed_summary, raw_transcript_length,
        assignment_given, affect_before, affect_after, masking_load, volition_index)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [clientId, sessionId, s.session_number, summary, convos.rows.length,
     assignmentGiven, affectBefore, affectAfter, maskingLoad, volitionIndex]
  );

  return summary;
}

async function updatePersistentProfile(clientId, sessionId, latestSummary) {
  const archives = await pool.query(
    `SELECT * FROM session_archives WHERE client_id=$1 ORDER BY archived_at DESC LIMIT 10`,
    [clientId]
  );
  const latestNeeds = await pool.query(
    `SELECT seen, cheered, aimed, guided, volition_index FROM need_scores
     WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 1`, [clientId]
  );
  const latestAssign = await pool.query(
    `SELECT assignment_text FROM assignments WHERE client_id=$1 ORDER BY created_at DESC LIMIT 1`, [clientId]
  );
  const maskingHistory = await pool.query(
    `SELECT masking_load, recorded_at FROM masking_scores WHERE client_id=$1 ORDER BY recorded_at DESC LIMIT 5`, [clientId]
  );
  const sessionCount = await pool.query(
    `SELECT COUNT(*) FROM sessions WHERE client_id=$1`, [clientId]
  );
  // Phase 1: include ecosystem data in profile generation
  const ecosystem = await pool.query(
    `SELECT person_name, person_type, needs_provided FROM ecosystem WHERE client_id=$1`, [clientId]
  );

  const archiveSummaries = archives.rows.map((a, i) =>
    `Session ${a.session_number || (archives.rows.length - i)}: ${a.compressed_summary || '(no summary)'}`
  ).join('\n');

  const needs = latestNeeds.rows[0] || {};
  const maskingTrend = maskingHistory.rows.map(m => m.masking_load);

  // Build ecosystem context — single node is a critical structural signal
  const ecoNodes = ecosystem.rows;
  const ecoText = ecoNodes.length === 0
    ? 'ECOSYSTEM: Empty — no support relationships mapped.'
    : ecoNodes.length === 1
      ? `ECOSYSTEM: Single node — "${ecoNodes[0].person_name}" (${ecoNodes[0].person_type}) carries all mapped needs. This is structurally precarious. Expanding this network is a clinical priority.`
      : `ECOSYSTEM: ${ecoNodes.map(e => `${e.person_name} (${e.person_type}, needs: ${(e.needs_provided||[]).join(', ')||'none'})`).join('; ')}`;

  const profilePrompt = `You are updating a persistent client profile for a therapeutic memory system.
Based on the session history and ecosystem data below, extract a structured profile.
Respond ONLY in this exact JSON format with no preamble or markdown:
{
  "active_patterns": ["pattern 1", "pattern 2", "pattern 3"],
  "risk_flags": ["flag 1"] or [],
  "next_priorities": ["priority 1", "priority 2", "priority 3"],
  "hypothesis_label": "one brief phrase describing what this person is crossing"
}

Rules for active_patterns:
- Maximum 3, each under 20 words
- Must be grounded in specific evidence from the session transcripts
- Describe what the person DOES or the structure of their experience — not an interpretation of why
- Do NOT characterise resistance or avoidance unless the transcript directly evidences it
- If a person pushes back on a frame, this may be epistemological precision, not avoidance — describe it as such

Rules for risk_flags:
- Only genuine clinical concern: isolation, despair, harm ideation, crisis
- Leave empty if none present
- A single-node ecosystem is a structural concern, not a crisis risk — note it in priorities, not flags

Rules for next_priorities:
- Maximum 4 priorities (not 3 — allow space for structural interventions)
- Priority 1 must address the most structurally urgent item, not the most emotionally available one
- IF the ecosystem has only one node: the FIRST or SECOND priority must name the person in that node and explore expanding the relationship or the network
- Each priority must be specific and actionable — name what to do, not just what to explore
- Later priorities (3, 4) may address deeper psychological terrain

Rules for hypothesis_label:
- A single poetic phrase: "learning to exist without apology", "crossing from performed strength to felt safety"

${ecoText}

FOUR NEEDS (latest):
Seen=${needs.seen||0}, Cheered=${needs.cheered||0}, Aimed=${needs.aimed||0}, Guided=${needs.guided||0}
${needs.aimed < 30 ? 'NOTE: Aimed is critically low (' + needs.aimed + '/100) — the person has no authored direction. Building aim is a structural priority.' : ''}
${needs.seen < 30 ? 'NOTE: Seen is critically low (' + needs.seen + '/100) — the person is not feeling witnessed. Being seen is the most basic need.' : ''}

SESSION HISTORY (most recent first):
${archiveSummaries}`;

  const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 600, messages: [{ role: 'user', content: profilePrompt }] })
  });
  const aiData = await aiRes.json();
  if (aiData.error) throw new Error('AI error: ' + aiData.error.message);

  let profileData = { active_patterns: [], risk_flags: [], next_priorities: [], hypothesis_label: null };
  try {
    const raw = aiData.content?.[0]?.text?.trim() || '{}';
    profileData = JSON.parse(raw.replace(/```json|```/g, '').trim());
  } catch (e) {
    console.error('[updatePersistentProfile] JSON parse failed, using defaults:', e.message);
  }

  await pool.query(
    `INSERT INTO persistent_profiles
       (client_id, volition_index, seen_score, cheered_score, aimed_score, guided_score,
        masking_trend, active_patterns, risk_flags, next_priorities,
        last_assignment, last_assignment_status, session_count, last_session_summary,
        profile_staleness, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',$12,$13,FALSE,NOW())
     ON CONFLICT (client_id) DO UPDATE SET
       volition_index=$2, seen_score=$3, cheered_score=$4, aimed_score=$5, guided_score=$6,
       masking_trend=$7, active_patterns=$8, risk_flags=$9, next_priorities=$10,
       last_assignment=$11, session_count=$12, last_session_summary=$13,
       profile_staleness=FALSE, updated_at=NOW()`,
    [
      clientId,
      needs.volition_index || null, needs.seen || null, needs.cheered || null,
      needs.aimed || null, needs.guided || null,
      JSON.stringify(maskingTrend),
      JSON.stringify(profileData.active_patterns || []),
      JSON.stringify(profileData.risk_flags || []),
      JSON.stringify(profileData.next_priorities || []),
      latestAssign.rows[0]?.assignment_text || null,
      parseInt(sessionCount.rows[0].count),
      latestSummary || null
    ]
  );
}

async function detectSovereignMoments(clientId, sessionId) {
  const convos = await pool.query(
    'SELECT role, content FROM conversations WHERE session_id=$1 AND client_id=$2 ORDER BY recorded_at ASC',
    [sessionId, clientId]
  );
  if (convos.rows.length < 4) return;

  const transcriptText = convos.rows.map((r, i) =>
    `[${i+1}] ${r.role === 'user' ? 'PERSON' : 'GUIDE'}: ${r.content}`
  ).join('\n');

  const detectionPrompt = `You are detecting sovereign moments in a therapeutic conversation.

A SOVEREIGN MOMENT is any instance where the person asserts their own voice, framing, or self-understanding — particularly in response to or contrast with what the Guide has offered. There are four types:

TYPE 1 — EXPLICIT CORRECTION: Person directly refuses or corrects the Guide's framing.
Examples:
- Guide says "It sounds like grief" / Person says "No — it's not grief, it's fury"
- Guide offers a metaphor / Person says "That's not quite it — what it actually is..."
- Guide reflects something back / Person says "Actually, I think..."

TYPE 2 — SELF-NAMING: Person authors their own description, identity, or self-understanding — especially when it is precise, unexpected, or goes beyond what the Guide offered.
Examples:
- "I am the seer whose task is coming to an end well" (self-named, not assigned)
- "I am not someone who gives up — I am someone who goes quiet"
- Any moment of self-description using language the person generated, not the Guide

TYPE 3 — CHOOSING / REACHING: Person makes an explicit volitional choice or reaches toward something — not passive reception, but active authorship of direction.
Examples:
- "We find together, I like that" (choosing collaboration, not just accepting it)
- "Yes — I want to try that"
- "My head is lifted up now and I will see it when it comes" (authored orientation)

TYPE 4 — OWNERSHIP: Person explicitly claims or confirms their own act, naming, or insight.
Examples:
- "Yes. I did." (in response to being told they named themselves)
- "That is mine — I said that"
- Any explicit claiming of an act or insight as their own

TIER ASSIGNMENT:
- Tier 1 (auto-confirm): Types 1, 2, 3, 4 when the sovereign act is clear and unambiguous from the text
- Tier 2 (flag for review): Moments that may be sovereign but are ambiguous or brief

NOT sovereign moments:
- Simple agreement ("yes", "that's right", "exactly") with no added authorship
- New information added without any self-assertion
- Emotional responses (crying, sighing) without a voiced claim

BE GENEROUS. It is better to flag too many than to miss a genuine moment of self-authorship. A person's sovereign moments are the markers of their crossing — missing them means their trail goes blank.

Respond ONLY in valid JSON, no preamble, no markdown:
{
  "tier1": [{"text": "exact quote from person", "type": "TYPE 1|2|3|4", "note": "brief reason"}],
  "tier2": [{"text": "exact quote from person", "type": "TYPE 1|2|3|4", "note": "brief reason"}]
}

TRANSCRIPT:
${transcriptText.slice(0, 6000)}`;

  const aiRes = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 1000, messages: [{ role: 'user', content: detectionPrompt }] })
  });
  const aiData = await aiRes.json();
  if (aiData.error) throw new Error('AI error: ' + aiData.error.message);

  let detected = { tier1: [], tier2: [] };
  try {
    const raw = aiData.content?.[0]?.text?.trim() || '{}';
    detected = JSON.parse(raw.replace(/```json|```/g, '').trim());
  } catch (e) {
    console.error('[detectSovereignMoments] JSON parse failed:', e.message);
    return;
  }

  // Auto-save Tier 1 (confirmed)
  for (const moment of (detected.tier1 || [])) {
    const text = typeof moment === 'string' ? moment : moment.text;
    const note = typeof moment === 'object' ? (moment.type + (moment.note ? ' — ' + moment.note : '')) : null;
    if (text && text.length > 3) {
      await pool.query(
        `INSERT INTO sovereign_moments (client_id, session_id, moment_text, detection_tier, confirmed, practitioner_note)
         VALUES ($1,$2,$3,1,TRUE,$4)`,
        [clientId, sessionId, text.slice(0, 500), note]
      );
    }
  }

  // Save Tier 2 (flagged for practitioner review)
  for (const moment of (detected.tier2 || [])) {
    const text = typeof moment === 'string' ? moment : moment.text;
    const note = typeof moment === 'object' ? (moment.type + (moment.note ? ' — ' + moment.note : '')) : null;
    if (text && text.length > 3) {
      await pool.query(
        `INSERT INTO sovereign_moments (client_id, session_id, moment_text, detection_tier, confirmed, practitioner_note)
         VALUES ($1,$2,$3,2,FALSE,$4)`,
        [clientId, sessionId, text.slice(0, 500), note]
      );
    }
  }

  console.log('[detectSovereignMoments] tier1:', detected.tier1?.length || 0, 'tier2:', detected.tier2?.length || 0);
}

// Practitioner: view sovereign moments for a client
app.get('/practitioner/client/:id/sovereign-moments', practAuth, async (req, res) => {
  try {
    const moments = await pool.query(
      'SELECT * FROM sovereign_moments WHERE client_id=$1 ORDER BY detected_at DESC',
      [req.params.id]
    );
    res.json(moments.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Practitioner: confirm or dismiss a sovereign moment
app.patch('/practitioner/sovereign-moments/:id', practAuth, async (req, res) => {
  const { action, note } = req.body; // action: 'confirm' | 'dismiss'
  try {
    if (action === 'confirm') {
      await pool.query(
        'UPDATE sovereign_moments SET confirmed=TRUE, dismissed=FALSE, practitioner_note=$1 WHERE id=$2',
        [note || null, req.params.id]
      );
    } else if (action === 'dismiss') {
      await pool.query(
        'UPDATE sovereign_moments SET dismissed=TRUE, confirmed=FALSE, practitioner_note=$1 WHERE id=$2',
        [note || null, req.params.id]
      );
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Explicit Phase 1 table creation — call once if initDB migration didn't run
app.post('/practitioner/create-phase1-tables', practAuth, async (req, res) => {
  const results = [];
  const tables = [
    {
      name: 'persistent_profiles',
      sql: `CREATE TABLE IF NOT EXISTS persistent_profiles (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE UNIQUE,
        volition_index INTEGER,
        seen_score INTEGER, cheered_score INTEGER, aimed_score INTEGER, guided_score INTEGER,
        masking_trend JSONB DEFAULT '[]',
        active_patterns JSONB DEFAULT '[]',
        risk_flags JSONB DEFAULT '[]',
        next_priorities JSONB DEFAULT '[]',
        last_assignment TEXT,
        last_assignment_status TEXT DEFAULT 'pending',
        session_count INTEGER DEFAULT 0,
        last_session_summary TEXT,
        profile_staleness BOOLEAN DEFAULT FALSE,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )`
    },
    {
      name: 'session_archives',
      sql: `CREATE TABLE IF NOT EXISTS session_archives (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        session_number INTEGER,
        compressed_summary TEXT,
        raw_transcript_length INTEGER,
        assignment_given TEXT,
        affect_before INTEGER,
        affect_after INTEGER,
        masking_load INTEGER,
        volition_index INTEGER,
        archived_at TIMESTAMPTZ DEFAULT NOW()
      )`
    },
    {
      name: 'session_architectures',
      sql: `CREATE TABLE IF NOT EXISTS session_architectures (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        movement_priorities JSONB,
        risk_flags JSONB DEFAULT '[]',
        opening_question TEXT,
        hypothesis_label TEXT,
        override_conditions TEXT,
        generated_at TIMESTAMPTZ DEFAULT NOW()
      )`
    },
    {
      name: 'sovereign_moments',
      sql: `CREATE TABLE IF NOT EXISTS sovereign_moments (
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
        session_id INTEGER,
        moment_text TEXT NOT NULL,
        detection_tier INTEGER DEFAULT 2,
        confirmed BOOLEAN DEFAULT FALSE,
        dismissed BOOLEAN DEFAULT FALSE,
        practitioner_note TEXT,
        detected_at TIMESTAMPTZ DEFAULT NOW()
      )`
    }
  ];

  for (const t of tables) {
    try {
      await pool.query(t.sql);
      results.push({ table: t.name, status: 'ok' });
    } catch (err) {
      results.push({ table: t.name, status: 'error', error: err.message });
    }
  }

  const allOk = results.every(r => r.status === 'ok');
  res.json({ ok: allOk, results });
});

// One-time migration: generate persistent profiles from existing session data
app.post('/practitioner/migrate-profiles', practAuth, async (req, res) => {
  const force = req.body && req.body.force === true; // force=true regenerates existing archives
  try {
    const clients = await pool.query('SELECT id FROM clients');
    res.json({ ok: true, queued: clients.rows.length, force });

    for (const c of clients.rows) {
      try {
        // Get most recent completed session
        const sess = await pool.query(
          'SELECT id FROM sessions WHERE client_id=$1 AND ended_at IS NOT NULL ORDER BY started_at DESC LIMIT 1',
          [c.id]
        );
        if (!sess.rows.length) continue;
        const sessionId = sess.rows[0].id;

        // Check if archive already exists — skip unless force=true
        const existing = await pool.query(
          'SELECT id FROM session_archives WHERE client_id=$1 AND session_id=$2', [c.id, sessionId]
        );
        if (existing.rows.length && !force) {
          // Archive exists and not forcing — still update profile in case it changed
          await updatePersistentProfile(c.id, sessionId, null).catch(err =>
            console.error('[migrate] profile failed for client', c.id, ':', err.message)
          );
        } else {
          // Delete existing archive if force, then rebuild
          if (existing.rows.length && force) {
            await pool.query('DELETE FROM session_archives WHERE client_id=$1 AND session_id=$2', [c.id, sessionId]);
          }
          const summary = await buildSessionArchive(c.id, sessionId).catch(err => {
            console.error('[migrate] archive failed for client', c.id, ':', err.message);
            return null;
          });
          await updatePersistentProfile(c.id, sessionId, summary).catch(err =>
            console.error('[migrate] profile failed for client', c.id, ':', err.message)
          );
        }

        console.log('[migrate] completed client', c.id);
        await new Promise(r => setTimeout(r, 2000));
      } catch (err) {
        console.error('[migrate] error for client', c.id, ':', err.message);
      }
    }
    console.log('[migrate] all clients processed');
  } catch (err) { console.error('[migrate] fatal:', err.message); }
});

initDB().then(() => {
  app.listen(PORT, () => console.log(`Crossing server v2 on port ${PORT}`));
}).catch(err => {
  console.error('DB init failed:', err.message);
  app.listen(PORT, () => console.log(`Crossing server (no DB) on port ${PORT}`));
});

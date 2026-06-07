const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const crypto = require('crypto');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const DB_PATH = process.env.DB_PATH || './chat.db';

const db = new sqlite3.Database(DB_PATH);
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, pubkey TEXT UNIQUE, handle TEXT UNIQUE, created_at INTEGER DEFAULT (strftime('%s', 'now')))`);
  db.run(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      from_handle TEXT,
      to_handle TEXT,
      encrypted_body TEXT,
      nonce TEXT,
      tag TEXT,
      timestamp INTEGER DEFAULT (strftime('%s', 'now'))
    )
  `);
});

async function getRecentHistory(handle, limit = 50) {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT from_handle, to_handle, encrypted_body, nonce, tag, timestamp 
       FROM messages 
       WHERE to_handle = ? OR from_handle = ?
       ORDER BY timestamp DESC LIMIT ?`,
      [handle, handle, limit],
      (err, rows) => {
        if (err) reject(err);
        else resolve(rows.reverse());
      }
    );
  });
}

function storeMessage(from, to, body, nonce, tag) {
  db.run(
    `INSERT INTO messages (from_handle, to_handle, encrypted_body, nonce, tag) VALUES (?, ?, ?, ?, ?)`,
    [from, to, body, nonce, tag]
  );
}

const staticDir = path.join(__dirname, 'public');
const server = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(staticDir, filePath);
  const ext = path.extname(filePath);
  const contentType = { '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css' }[ext] || 'text/plain';
  fs.readFile(filePath, (err, content) => {
    if (err) { res.writeHead(404); res.end('File not found'); }
    else { res.writeHead(200, { 'Content-Type': contentType }); res.end(content); }
  });
});

const wss = new WebSocket.Server({ server });
const clients = new Map();

function broadcastPresence() {
  const online = Array.from(clients.values()).map(c => c.handle);
  for (let [client, info] of clients.entries()) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'presence', online }));
    }
  }
}

wss.on('connection', (ws) => {
  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    
    if (msg.type === 'auth') {
      db.get(`SELECT id, handle FROM users WHERE handle = ?`, [msg.handle], async (err, row) => {
        if (row) { ws.send(JSON.stringify({ type: 'error', message: 'Handle taken' })); return; }
        const id = crypto.randomUUID();
        db.run(`INSERT INTO users (id, pubkey, handle) VALUES (?, ?, ?)`, [id, msg.pubkey, msg.handle], async (err2) => {
          clients.set(ws, { id, handle: msg.handle, pubkey: msg.pubkey });
          ws.send(JSON.stringify({ type: 'auth_ok', handle: msg.handle }));
          
          const recentMessages = await getRecentHistory(msg.handle);
          ws.send(JSON.stringify({ type: 'history', messages: recentMessages }));
          broadcastPresence();
        });
      });
    }

    if (msg.type === 'msg') {
      const sender = clients.get(ws);
      if (!sender) return;
      storeMessage(sender.handle, msg.to, msg.body, msg.nonce, msg.tag || '');
      for (let [client, info] of clients.entries()) {
        if (info.handle === msg.to && client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify({ type: 'msg', from: sender.handle, body: msg.body, nonce: msg.nonce }));
        }
      }
    }
  });
  ws.on('close', () => {
    clients.delete(ws);
    broadcastPresence();
  });
});

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

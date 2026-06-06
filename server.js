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
  db.run(`CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, from_handle TEXT, to_handle TEXT, body TEXT, nonce TEXT, tag TEXT, timestamp INTEGER DEFAULT (strftime('%s', 'now')))`);
});

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
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    
    if (msg.type === 'auth') {
      db.get(`SELECT id, handle FROM users WHERE handle = ?`, [msg.handle], (err, row) => {
        if (row) { ws.send(JSON.stringify({ type: 'error', message: 'Handle taken' })); return; }
        const id = crypto.randomUUID();
        db.run(`INSERT INTO users (id, pubkey, handle) VALUES (?, ?, ?)`, [id, msg.pubkey, msg.handle], (err2) => {
          clients.set(ws, { id, handle: msg.handle, pubkey: msg.pubkey });
          ws.send(JSON.stringify({ type: 'auth_ok', handle: msg.handle }));
          broadcastPresence();
        });
      });
    }

    if (msg.type === 'msg') {
      const sender = clients.get(ws);
      if (!sender) return;
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

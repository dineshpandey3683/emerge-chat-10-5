const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const crypto = require('crypto');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const DB_PATH = './chat.db';

const db = new sqlite3.Database(DB_PATH);

// Create tables
db.run(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    pubkey TEXT UNIQUE,
    handle TEXT UNIQUE,
    created_at INTEGER DEFAULT (strftime('%s', 'now'))
  )
`);

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

// Serve static files
const staticDir = path.join(__dirname, 'public');
const server = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(staticDir, filePath);
  
  const ext = path.extname(filePath);
  const contentType = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.ico': 'image/x-icon'
  }[ext] || 'text/plain';
  
  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404);
      res.end('File not found');
    } else {
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    }
  });
});

const wss = new WebSocket.Server({ server });
const clients = new Map();

// Get recent messages for a user
function getRecentHistory(handle, limit = 50) {
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

// Store message in database
function storeMessage(from, to, body, nonce, tag) {
  db.run(
    `INSERT INTO messages (from_handle, to_handle, encrypted_body, nonce, tag) 
     VALUES (?, ?, ?, ?, ?)`,
    [from, to, body, nonce, tag]
  );
}

// Broadcast online users
function broadcastPresence() {
  const online = Array.from(clients.values()).map(c => c.handle);
  for (let [client, info] of clients.entries()) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'presence', online }));
    }
  }
}

function validateMessage(data) {
  if (!data || typeof data !== 'object') return false;
  const allowed = ['type', 'to', 'body', 'nonce', 'tag'];
  for (let key in data) {
    if (!allowed.includes(key)) return false;
  }
  if (data.type === 'msg') {
    if (!data.to || !data.body || !data.nonce) return false;
    if (typeof data.to !== 'string' || typeof data.body !== 'string') return false;
    if (data.to.length > 50 || data.body.length > 5000) return false;
  }
  if (data.type === 'auth') {
    if (!data.handle || !data.pubkey) return false;
    if (typeof data.handle !== 'string' || typeof data.pubkey !== 'string') return false;
    if (data.handle.length < 2 || data.handle.length > 30) return false;
    if (!/^[a-zA-Z0-9_]+$/.test(data.handle)) return false;
  }
  return true;
}

wss.on('connection', (ws, req) => {
  const clientIP = req.socket.remoteAddress;
  console.log(`[+] Connection from ${clientIP}`);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { ws.close(); return; }
    if (!validateMessage(msg)) { 
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
      ws.close(); 
      return; 
    }

    // AUTHENTICATION
    if (msg.type === 'auth') {
      db.get(`SELECT id, handle FROM users WHERE handle = ?`, [msg.handle], async (err, row) => {
        if (err) {
          ws.send(JSON.stringify({ type: 'error', message: 'Database error' }));
          return;
        }
        if (row) {
          ws.send(JSON.stringify({ type: 'error', message: 'Handle already taken' }));
          ws.close();
          return;
        }
        const id = crypto.randomUUID();
        db.run(`INSERT INTO users (id, pubkey, handle) VALUES (?, ?, ?)`,
          [id, msg.pubkey, msg.handle], async (err2) => {
            if (err2) {
              ws.send(JSON.stringify({ type: 'error', message: 'Registration failed' }));
              return;
            }
            clients.set(ws, { id, handle: msg.handle, pubkey: msg.pubkey });
            ws.send(JSON.stringify({ type: 'auth_ok', id, handle: msg.handle }));
            
            // Send message history
            const history = await getRecentHistory(msg.handle);
            ws.send(JSON.stringify({ type: 'history', messages: history }));
            
            console.log(`[+] ${msg.handle} joined (${clients.size} online)`);
            broadcastPresence();
          });
      });
      return;
    }

    // MESSAGE RELAY
    if (msg.type === 'msg') {
      const sender = clients.get(ws);
      if (!sender) { 
        ws.send(JSON.stringify({ type: 'error', message: 'Not authenticated' }));
        return; 
      }
      
      // Store message
      storeMessage(sender.handle, msg.to, msg.body, msg.nonce, msg.tag || '');
      
      // Find recipient
      let recipientWs = null;
      for (let [client, info] of clients.entries()) {
        if (info.handle === msg.to) {
          recipientWs = client;
          break;
        }
      }
      
      if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
        recipientWs.send(JSON.stringify({
          type: 'msg',
          from: sender.handle,
          body: msg.body,
          nonce: msg.nonce,
          tag: msg.tag || ''
        }));
      } else {
        ws.send(JSON.stringify({ type: 'error', message: `User "${msg.to}" not online` }));
      }
      return;
    }
  });

  ws.on('close', () => {
    const user = clients.get(ws);
    if (user) {
      console.log(`[-] ${user.handle} left (${clients.size - 1} online)`);
      clients.delete(ws);
      broadcastPresence();
    }
  });
});

server.listen(PORT, () => {
  console.log(`
╔═══════════════════════════════════════╗
║  ⟁ EMERGE CHAT — 10/5 READY         ║
║  Server: ws://localhost:${PORT}        ║
║  Web:    http://localhost:${PORT}        ║
║  History: ✓ | Online List: ✓         ║
╚═══════════════════════════════════════╝
  `);
});

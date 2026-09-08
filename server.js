// 加帕里公园 · 联机中继服务器（部署到 Render）
// 职责：大厅/房间管理（4位房间码，最多8人）+ WebSocket 消息转发（房主权威，服务器不跑游戏逻辑）
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 9090;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map(); // code -> room

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, service: 'kapari-relay', rooms: rooms.size, ts: Date.now() }));
});

const wss = new WebSocketServer({ server });

function mkCode() {
  let c;
  do { c = Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join(''); } while (rooms.has(c));
  return c;
}
function mkToken() { return Math.random().toString(16).slice(2, 10); }
function memberList(room) { return [...room.members.values()].map(m => ({ pid: m.pid, name: m.name, offline: !m.ws })); }
function lobbyMsg(room) { return { t: 'lobby', room: room.code, host: room.hostPid, members: memberList(room) }; }
function send(ws, data) { if (ws && ws.readyState === 1) ws.send(typeof data === 'string' ? data : JSON.stringify(data)); }
function broadcast(room, data, exceptWs) {
  const s = JSON.stringify(data);
  for (const m of room.members.values()) { if (m.ws && m.ws !== exceptWs && m.ws.readyState === 1) m.ws.send(s); }
}

wss.on('connection', ws => {
  let joined = null; // { room, member }

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (joined) joined.room.lastActive = Date.now();

    if (m.t === 'ping') { send(ws, { t: 'pong' }); return; }

    // ---- 未入房：create / join ----
    if (!joined) {
      if (m.t === 'create') {
        const name = String(m.name || '朋友').slice(0, 10);
        const room = { code: mkCode(), hostPid: 'p0', nextPid: 1, started: false, members: new Map(), created: Date.now(), lastActive: Date.now() };
        const member = { pid: 'p0', name, token: mkToken(), ws };
        room.members.set('p0', member);
        rooms.set(room.code, room);
        joined = { room, member };
        send(ws, { t: 'joined', room: room.code, pid: 'p0', token: member.token, host: 'p0', members: memberList(room), started: false });
        return;
      }
      if (m.t === 'join') {
        const code = String(m.room || '').toUpperCase();
        const room = rooms.get(code);
        if (!room) { send(ws, { t: 'err', msg: '房间不存在，请核对房间码' }); return; }
        const token = String(m.token || '');
        // 凭 token 重连（同一身份回到对局）
        for (const mm of room.members.values()) {
          if (token && mm.token === token && !mm.ws) {
            mm.ws = ws;
            joined = { room, member: mm };
            send(ws, { t: 'joined', room: room.code, pid: mm.pid, token: mm.token, host: room.hostPid, members: memberList(room), started: room.started });
            broadcast(room, lobbyMsg(room));
            return;
          }
        }
        if (room.started) { send(ws, { t: 'err', msg: '对局已开始，只能凭原身份重连（换浏览器/清缓存后无法重入）' }); return; }
        if (room.members.size >= 8) { send(ws, { t: 'err', msg: '房间已满（最多8人）' }); return; }
        const pid = 'p' + (room.nextPid++);
        const member = { pid, name: String(m.name || '朋友').slice(0, 10), token: mkToken(), ws };
        room.members.set(pid, member);
        joined = { room, member };
        send(ws, { t: 'joined', room: room.code, pid, token: member.token, host: room.hostPid, members: memberList(room), started: false });
        broadcast(room, lobbyMsg(room));
        return;
      }
      return;
    }

    // ---- 已入房 ----
    const room = joined.room;
    if (m.t === 'start') {
      if (joined.member.pid !== room.hostPid) return;
      const online = [...room.members.values()].filter(x => x.ws);
      if (online.length < 2) { send(ws, { t: 'err', msg: '至少需要2名玩家才能开始' }); return; }
      room.started = true;
      broadcast(room, { t: 'starting' });
      return;
    }
    if (m.t === 'act') { // 玩家操作 → 转发给房主裁决
      if (joined.member.pid === room.hostPid) return;
      const host = room.members.get(room.hostPid);
      if (host && host.ws) send(host.ws, { t: 'act', from: joined.member.pid, a: m.a, v: m.v });
      return;
    }
    if (m.t === 'g') { // 房主广播（特效/提示事件）
      if (joined.member.pid !== room.hostPid) return;
      broadcast(room, m.d, ws);
      return;
    }
    if (m.t === 'gto') { // 房主定向消息（个性化快照 / 效果询问 / 亮牌）
      if (joined.member.pid !== room.hostPid) return;
      const to = room.members.get(m.to);
      if (to && to.ws) send(to.ws, { t: 'gto', to: m.to, d: m.d });
      return;
    }
  });

  ws.on('close', () => {
    if (!joined) return;
    const room = joined.room;
    const pid = joined.member.pid;
    joined.member.ws = null;
    joined = null;

    if (!room.started && pid !== room.hostPid) {
      // 开局前离开：直接移出房间
      room.members.delete(pid);
      broadcast(room, lobbyMsg(room));
      return;
    }
    if (pid === room.hostPid && !room.started) {
      rooms.delete(room.code);
      broadcast(room, { t: 'err', msg: '房主解散了房间' });
      return;
    }
    if (room.started) {
      broadcast(room, { t: 'bye', pid });
      if (pid === room.hostPid) broadcast(room, { t: 'hostlost' });
    }
  });
});

// 心跳保活 + 过期房间清理
setInterval(() => { for (const ws of wss.clients) { try { ws.ping(); } catch (_) {} } }, 30000);
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const anyOnline = [...room.members.values()].some(m => m.ws);
    if (!anyOnline && now - room.lastActive > 30 * 60 * 1000) rooms.delete(code);
    else if (now - room.created > 12 * 60 * 60 * 1000) rooms.delete(code);
  }
}, 5 * 60 * 1000);

server.listen(PORT, () => console.log('Kapari relay listening on ' + PORT));

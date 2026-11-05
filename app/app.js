'use strict';
/* Local Chat (HTML/CSS/JS) — protocol-compatible with the Flutter app.
   Needs Node APIs in the renderer (Capacitor Electron: nodeIntegration:true, contextIsolation:false). */
const R = window.require || window.nodeRequire;
const net = R('net'), dgram = R('dgram'), os = R('os'), fs = R('fs'), path = R('path'), { pathToFileURL } = R('url');
const PT = 4567, PF = 4568, PD = 4569;
// Save all received files into Downloads/Local Chat/
const DIR = path.join(os.homedir(), 'Downloads', 'Local Chat');
fs.mkdirSync(DIR, { recursive: true });
// Electron shell for opening files with system default app
let _shell = null;
try { const { shell } = R('electron'); _shell = shell; } catch { try { _shell = R('@electron/remote').shell; } catch {} }
function openFile(fp) {
  if (_shell && fp && fs.existsSync(fp)) { _shell.openPath(fp); }
  else if (fp) { try { R('child_process').exec(`"${fp}"`); } catch {} }
}

/* ---------- helpers & storage ---------- */
const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const furl = p => p ? pathToFileURL(p).href : '';
const clean = ip => (ip || '').replace('::ffff:', '');
const chatIdFor = (a, b) => [a, b].sort().join('_');
let db = JSON.parse(localStorage.getItem('lc_db') || '{"users":{},"chats":{},"msgs":{},"pend":[]}');
let saveT; const save = () => { clearTimeout(saveT); saveT = setTimeout(() => localStorage.setItem('lc_db', JSON.stringify(db)), 300); };
const myId = localStorage.getItem('device_id') || (localStorage.setItem('device_id', crypto.randomUUID()), localStorage.getItem('device_id'));
let myName = localStorage.getItem('display_name') || '';
let openId = null, replyTo = null, cur = 'onboard', fresh = false, lastTyped = 0;
const typing = {}, tt = {}, prog = {};
if (localStorage.getItem('theme')) document.documentElement.dataset.theme = localStorage.getItem('theme');

const findMsg = id => { for (const l of Object.values(db.msgs)) { const m = l.find(x => x.id === id); if (m) return m; } };
const online = u => u && u.online && u.ip;
const recips = c => db.chats[c].ids.filter(i => i !== myId);
const cname = c => c.group ? c.name : (db.users[c.ids.find(i => i !== myId)]?.name || 'Unknown');
const other = c => c.group ? null : db.users[c.ids.find(i => i !== myId)];
function ensureChat(id, group, name, ids) { if (!db.chats[id]) db.chats[id] = { id, group: !!group, name, ids, unread: 0, last: 0 }; }
function addMsg(m) {
  (db.msgs[m.chatId] ??= []).push(m);
  db.chats[m.chatId].last = m.ts;
  if (m.senderId !== myId && m.status !== 'receiving') {
    if (m.chatId !== openId) db.chats[m.chatId].unread = (db.chats[m.chatId].unread || 0) + 1;
    playNotif();
  }
  save();
}

/* ---------- low-level TCP ---------- */
const sendPacket = (ip, obj) => new Promise(res => {
  const s = net.connect({ host: ip, port: PT }); let ok = false;
  s.setTimeout(2500, () => s.destroy());
  s.on('error', () => {}); s.on('close', () => res(ok));
  s.on('connect', () => s.end(JSON.stringify(obj), () => { ok = true; }));
});
const sendFileTo = (ip, hdr, fp) => new Promise(res => {
  const s = net.connect({ host: ip, port: PF }); let ok = false;
  s.on('error', () => {}); s.on('close', () => res(ok)); s.on('finish', () => { ok = true; });
  s.on('connect', () => {
    s.write(JSON.stringify(hdr) + '\n');
    const rs = fs.createReadStream(fp); rs.on('error', () => s.destroy()); rs.pipe(s);
  });
});

/* ---------- users / discovery ---------- */
function seen(id, name, ip) {
  if (id === myId) return;
  let u = db.users[id]; const wasOff = !u || !u.online;
  if (!u) u = db.users[id] = { id, blocked: false };
  Object.assign(u, { name: name || u.name || 'Unknown', ip, online: true, seen: Date.now() });
  if (wasOff) { flush(id); pushPhoto(u); }
  save(); ui();
}
function pushPhoto(u) {
  const p = localStorage.getItem('my_photo');
  if (!p || u.photoSent || !fs.existsSync(p)) return;
  u.photoSent = 1;
  sendFileTo(u.ip, { msgId: String(Date.now()), chatId: '', senderId: myId, filename: path.basename(p), filesize: fs.statSync(p).size, isProfilePhoto: true }, p);
}
setInterval(() => { // pruner
  let ch = false;
  for (const u of Object.values(db.users)) if (u.online && Date.now() - u.seen > 12000) { u.online = false; ch = true; }
  if (ch) { save(); ui(); }
}, 5000);

const ifaces = () => Object.values(os.networkInterfaces()).flat().filter(a => a && a.family === 'IPv4' && !a.internal);
const bcast = a => { const ip = a.address.split('.').map(Number), m = a.netmask.split('.').map(Number); return ip.map((x, i) => x | (~m[i] & 255)).join('.'); };

const rx = dgram.createSocket({ type: 'udp4', reuseAddr: true });
rx.on('error', () => {});
rx.on('message', (m, r) => { try { const p = JSON.parse(m); if (p.id) seen(p.id, p.name, r.address); } catch {} });
rx.bind(PD, () => { try { rx.setBroadcast(true); } catch {} ifaces().forEach(a => { try { rx.addMembership('224.0.0.1', a.address); } catch {} }); });

setInterval(() => { // UDP beacons, one socket per interface (hotspot-friendly)
  if (!myName) return;
  const b = Buffer.from(JSON.stringify({ id: myId, name: myName }));
  ifaces().forEach(a => {
    const s = dgram.createSocket('udp4');
    s.on('error', () => { try { s.close(); } catch {} });
    s.bind(0, a.address, () => {
      try { s.setBroadcast(true); s.setMulticastInterface(a.address); } catch {}
      ['255.255.255.255', '224.0.0.1', bcast(a)].forEach(t => s.send(b, PD, t, () => {}));
      setTimeout(() => { try { s.close(); } catch {} }, 400);
    });
  });
}, 3000);

setInterval(() => { // TCP ping sweep fallback
  if (!myName) return;
  ifaces().forEach(a => {
    const base = a.address.split('.').slice(0, 3).join('.');
    for (let i = 1; i < 255; i++) {
      const h = base + '.' + i; if (h === a.address) continue;
      const s = net.connect({ host: h, port: PT });
      s.setTimeout(700, () => s.destroy());
      s.on('error', () => {});
      s.on('connect', () => s.write(JSON.stringify({ type: 'ping', senderId: myId, content: myName })));
      s.on('data', d => { try { const p = JSON.parse(d); if (p.senderId) seen(p.senderId, p.content, h); } catch {} s.destroy(); });
    }
  });
}, 9000);

/* ---------- servers ---------- */
net.createServer({ allowHalfOpen: true }, s => {
  let buf = Buffer.alloc(0); const ip = clean(s.remoteAddress);
  s.on('error', () => {}); s.on('end', () => s.end());
  s.on('data', d => { buf = Buffer.concat([buf, d]); try { const p = JSON.parse(buf.toString()); buf = Buffer.alloc(0); onPacket(p, ip, s); } catch {} });
}).listen(PT);

function onPacket(p, ip, s) {
  if (p.type === 'ping') { seen(p.senderId, p.content, ip); s.end(JSON.stringify({ type: 'pong', senderId: myId, content: myName })); return; }
  if (p.type === 'pong') { seen(p.senderId, p.content, ip); return; }
  if (p.type === 'ack') { const m = findMsg(p.msgId); if (m && !(m.status === 'read' && p.status !== 'read')) { m.status = p.status; save(); ui(); } return; }
  if (p.type === 'delete') { const m = findMsg(p.id); if (m) { m.deleted = 1; save(); ui(); } return; }
  if (p.type === 'typing') {
    const k = p.chatId + p.senderId; (typing[p.chatId] ??= {})[p.senderId] = 1;
    clearTimeout(tt[k]); tt[k] = setTimeout(() => { delete typing[p.chatId]?.[p.senderId]; ui(); }, 4000); ui(); return;
  }
  if (p.type === 'text') {
    if (db.users[p.senderId]?.blocked) { sendPacket(ip, { type: 'ack', msgId: p.id, status: 'delivered' }); return; } // fake ack
    if (!db.users[p.senderId]) seen(p.senderId, null, ip);
    ensureChat(p.chatId, !!p.groupName, p.groupName, p.participantIds || [p.senderId, myId]);
    if (findMsg(p.id)) return;
    const isOpen = openId === p.chatId && !document.hidden;
    const m = { id: p.id, chatId: p.chatId, senderId: p.senderId, content: p.content, type: 'text', ts: p.timestamp, status: isOpen ? 'read' : 'delivered', reply: p.replyToMessageId, deleted: p.isDeleted ? 1 : 0 };
    addMsg(m); if (!isOpen) db.chats[p.chatId].unread++;
    sendPacket(ip, { type: 'ack', msgId: p.id, status: m.status });
    if (document.hidden && 'Notification' in window) { try { new Notification(db.users[p.senderId]?.name || 'Local Chat', { body: p.content.slice(0, 100) }); } catch {} }
    ui();
  }
}

net.createServer(s => { // files, voice, profile photos
  let buf = Buffer.alloc(0), h = null, ws = null, rec = 0, m = null, skip = false; const ip = clean(s.remoteAddress);
  s.on('error', () => {});
  const body = d => { if (skip || !d.length) return; rec += d.length; ws.write(d); prog[h.msgId] = rec / (h.filesize || 1); if (m) ui(); };
  s.on('data', d => {
    if (!h) {
      buf = Buffer.concat([buf, d]); const i = buf.indexOf(10); if (i < 0) return;
      try { h = JSON.parse(buf.slice(0, i).toString()); } catch { s.destroy(); return; }
      d = buf.slice(i + 1);
      skip = !!db.users[h.senderId]?.blocked;
      if (!skip) {
        const fn = path.basename(h.filename || 'file');
        h.out = path.join(DIR, h.isProfilePhoto ? `profile_${h.senderId}_${Date.now()}_${fn}` : `${Date.now()}_${fn}`);
        ws = fs.createWriteStream(h.out);
        if (!h.isProfilePhoto) {
          ensureChat(h.chatId, false, null, [h.senderId, myId]);
          m = { id: h.msgId, chatId: h.chatId, senderId: h.senderId, content: fn, path: h.out, type: /^voice_|\.(m4a|aac|ogg|wav|webm)$/i.test(fn) ? 'voice' : 'file', ts: Date.now(), status: 'receiving', filesize: h.filesize };
          addMsg(m);
        }
      }
    }
    body(d);
  });
  s.on('close', () => {
    if (!h || skip || !ws) return;
    ws.end(() => {
      if (h.isProfilePhoto) { const u = db.users[h.senderId]; if (u) { if (u.photo) fs.unlink(u.photo, () => {}); u.photo = h.out; } }
      else if (m) {
        const ok = rec >= h.filesize, isOpen = openId === h.chatId;
        m.status = ok ? (isOpen ? 'read' : 'delivered') : 'failed';
        if (ok && !isOpen) db.chats[h.chatId].unread++;
        if (ok) sendPacket(ip, { type: 'ack', msgId: h.msgId, status: m.status });
        delete prog[h.msgId];
      }
      save(); ui();
    });
  });
}).listen(PF);

/* ---------- sending ---------- */
const pkt = m => { const c = db.chats[m.chatId]; return { id: m.id, type: 'text', senderId: myId, chatId: m.chatId, content: m.content, timestamp: m.ts, replyToMessageId: m.reply || null, isDeleted: false, groupName: c.group ? c.name : null, participantIds: c.group ? c.ids : null }; };
async function dispatch(m, rid, kind = 'msg') {
  const u = db.users[rid]; let ok = false;
  if (online(u)) {
    if (kind === 'del') ok = await sendPacket(u.ip, { type: 'delete', id: m.id });
    else if (m.type === 'text') ok = await sendPacket(u.ip, pkt(m));
    else if (fs.existsSync(m.path)) ok = await sendFileTo(u.ip, { msgId: m.id, chatId: m.chatId, senderId: myId, filename: m.content, filesize: fs.statSync(m.path).size, isProfilePhoto: false }, m.path);
  }
  if (!ok && !db.pend.some(x => x.id === m.id && x.rid === rid && x.kind === kind)) db.pend.push({ id: m.id, rid, kind });
  save();
}
async function flush(uid) { // offline retry queue
  const list = db.pend.filter(x => x.rid === uid); db.pend = db.pend.filter(x => x.rid !== uid);
  for (const x of list) { const m = findMsg(x.id); if (m && !(x.kind === 'msg' && m.deleted)) await dispatch(m, uid, x.kind); }
}
function send(chatId, content, reply) {
  if (content.trim().split(/\s+/).length > 300) { const f = path.join(DIR, `message_${Date.now()}.txt`); fs.writeFileSync(f, content); return sendFile(chatId, f); }
  const m = { id: String(Date.now()), chatId, senderId: myId, content, type: 'text', ts: Date.now(), status: 'sent', reply };
  addMsg(m); recips(chatId).forEach(r => dispatch(m, r)); ui();
}
function sendFile(chatId, fp, type = 'file') {
  const m = { id: String(Date.now()), chatId, senderId: myId, content: path.basename(fp), path: fp, type, ts: Date.now(), status: 'sent' };
  addMsg(m); recips(chatId).forEach(r => dispatch(m, r)); ui();
}
function delMsg(id) { const m = findMsg(id); if (!m || m.senderId !== myId) return; m.deleted = 1; save(); recips(m.chatId).forEach(r => dispatch(m, r, 'del')); ui(); }
function sendTyping() {
  if (Date.now() - lastTyped < 3000) return; lastTyped = Date.now();
  recips(openId).forEach(r => { const u = db.users[r]; if (online(u)) sendPacket(u.ip, { type: 'typing', senderId: myId, chatId: openId }); });
}

/* ---------- navigation ---------- */
/* Switch left panel only — never hides right panel */
function show(v) {
  if (v === 'onboard') {
    document.getElementById('onboard').hidden = false;
    document.getElementById('main-layout').hidden = true;
    cur = v; return;
  }
  document.getElementById('onboard').hidden = true;
  document.getElementById('main-layout').hidden = false;
  cur = v;
  document.querySelectorAll('.left-view').forEach(el => el.classList.toggle('active', el.id === v));
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('nav-active', btn.dataset.go === v));
  if (v === 'set') { $('#sName').value = myName; updateMyProfile(); }
  if (v === 'group') rGroup();
  ui();
}
/* Render left panel + chat if open */
const ui = () => {
  if (cur === 'home') rHome();
  else if (cur === 'nearby') rNear();
  else if (cur === 'set') rSet();
  if (openId) { const chatEl = document.getElementById('chat'); if (chatEl && !chatEl.hidden) rChat(); }
};
/* Open chat in right panel */
function openChat(id) {
  openId = id; fresh = true; db.chats[id].unread = 0;
  (db.msgs[id] || []).forEach(m => {
    if (m.senderId !== myId && m.status !== 'read' && m.status !== 'receiving' && m.status !== 'failed') {
      m.status = 'read'; const u = db.users[m.senderId];
      if (online(u)) sendPacket(u.ip, { type: 'ack', msgId: m.id, status: 'read' });
    }
  });
  save();
  document.getElementById('welcome-screen').hidden = true;
  document.getElementById('chat').hidden = false;
  document.getElementById('sidebar').classList.add('slide-out'); // mobile only
  rHome(); rChat(); // update list highlight + render chat
}
function startChat(uid) { const id = chatIdFor(myId, uid); ensureChat(id, false, null, [myId, uid].sort()); openChat(id); }
document.addEventListener('click', e => { const g = e.target.closest('[data-go]'); if (g) { show(g.dataset.go); return; } });

/* ---------- rendering ---------- */
const initials = n => (n || '?').trim().slice(0, 1).toUpperCase();
/* Always show initials as base; photo absolute-overlays if available (onerror = show initials) */
const av = (u, name) => {
  const hasPhoto = u?.photo && fs.existsSync(u.photo);
  const img = hasPhoto ? `<img src="${furl(u.photo)}" onerror="this.style.display='none'">` : '';
  return `<div class="av">${esc(initials(name))}${img}${u ? `<i class="${u.online ? 'on' : ''}"></i>` : ''}</div>`;
};
const fmt = t => { const d = new Date(t), n = new Date(); return d.toDateString() === n.toDateString() ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : d.toLocaleDateString([], { day: 'numeric', month: 'short' }); };
const preview = m => !m ? 'No messages yet' : m.deleted ? 'This message was deleted' : m.type === 'voice' ? 'Voice message' : m.type === 'file' ? 'File: ' + m.content : m.content;

let searchQuery = '', lastHomeHash = '';
function rHome() {
  const q = searchQuery.toLowerCase();
  const cs = Object.values(db.chats)
    .filter(c => !q || cname(c).toLowerCase().includes(q))
    .sort((a, b) => (b.last || 0) - (a.last || 0));
  // Skip full rebuild if nothing changed (prevents flicker during file transfer)
  const hash = cs.map(c => `${c.id}:${c.unread}:${c.last||0}:${c.id===openId?1:0}`).join('|') + q;
  if (hash === lastHomeHash) return;
  lastHomeHash = hash;
  $('#list').innerHTML = cs.length ? cs.map(c => {
    const ms = db.msgs[c.id] || [], l = ms[ms.length - 1];
    const active = c.id === openId ? ' active-chat' : '';
    return `<div class="row${active}" data-open="${c.id}">${av(other(c), cname(c))}<div class="mid"><b>${esc(cname(c))}</b><small>${esc(preview(l))}</small></div><div class="right"><span class="time">${l ? fmt(l.ts) : ''}</span>${c.unread ? `<br><span class="badge">${c.unread > 9 ? '9+' : c.unread}</span>` : ''}</div></div>`;
  }).join('') : `<div class="empty">${q ? 'No results for \u201c' + esc(q) + '\u201d.' : 'No chats yet.\nOpen Nearby to find people.'}</div>`;
}
function rNear() {
  const us = Object.values(db.users).sort((a, b) => (b.online ? 1 : 0) - (a.online ? 1 : 0));
  $('#nearList').innerHTML = us.length ? us.map(u => `<div class="row" data-user="${u.id}">${av(u, u.name)}<div class="mid"><b>${esc(u.name)}</b><small>${u.online ? 'Online · ' + esc(u.ip) : 'Offline'}</small></div></div>`).join('') : '<div class="empty">Looking for devices on this network…</div>';
}
function rGroup() {
  $('#gName').value = '';
  const us = Object.values(db.users).filter(u => u.online && !u.blocked);
  $('#gList').innerHTML = us.length ? us.map(u => `<label class="chk">${av(u, u.name)}<span class="grow">${esc(u.name)}</span><input type="checkbox" value="${u.id}"></label>`).join('') : '<div class="empty">No one is online right now.</div>';
}
function rSet() {
  const b = Object.values(db.users).filter(u => u.blocked);
  $('#blk').innerHTML = b.length ? b.map(u => `<div class="chk">${av(u, u.name)}<span class="grow">${esc(u.name)}</span><button data-unblock="${u.id}">Unblock</button></div>`).join('') : '<small>No blocked users.</small>';
}
const prg = m => {
  const pct = Math.round((prog[m.id] || 0) * 100);
  const total = m.filesize || 0;
  const done = Math.round((prog[m.id] || 0) * total);
  const sizeStr = total > 0 ? `${fmtSize(done)} / ${fmtSize(total)}` : 'Receiving\u2026';
  return `<div class="prg-wrap"><div class="prg-hdr"><span style="font-size:18px">${fileIcon(m.content)}</span><span class="prg-name">${esc(m.content)}</span><span class="prg-pct">${pct}%</span></div><div class="bar"><i style="width:${pct}%"></i></div><small class="prg-sub">${sizeStr}</small></div>`;
};

// Returns a human-readable file size
const fmtSize = bytes => {
  if (!bytes) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
};

// Returns an emoji icon for a file based on extension
const fileIcon = name => {
  const ext = (name || '').split('.').pop().toLowerCase();
  const map = { pdf: '📄', doc: '📝', docx: '📝', xls: '📊', xlsx: '📊', ppt: '📑', pptx: '📑',
    zip: '🗜', rar: '🗜', '7z': '🗜', tar: '🗜', gz: '🗜',
    mp3: '🎵', wav: '🎵', flac: '🎵', aac: '🎵',
    mp4: '🎬', mkv: '🎬', avi: '🎬', mov: '🎬',
    png: '🖼', jpg: '🖼', jpeg: '🖼', gif: '🖼', webp: '🖼',
    txt: '📃', csv: '📋', json: '🔧', xml: '🔧', html: '🌐',
    exe: '⚙️', apk: '📦', dmg: '💿' };
  return map[ext] || '📎';
};

function bubble(m) {
  const me = m.senderId === myId, c = db.chats[m.chatId], q = m.reply && findMsg(m.reply); let body;
  if (m.deleted) body = '<i class="del">This message was deleted</i>';
  else if (m.status === 'receiving') body = prg(m);
  else if (m.status === 'failed') body = `<i class="del">Transfer failed: ${esc(m.content)}</i>`;
  else if (m.type === 'voice') body = `<audio controls src="${furl(m.path)}"></audio>`;
  else if (m.type === 'file') {
    if (/\.(png|jpe?g|gif|webp)$/i.test(m.content)) {
      // Images: show inline, clickable to open
      body = `<img class="pic" src="${furl(m.path)}" data-openfile="${esc(m.path)}" title="Click to open" style="cursor:pointer">`;
    } else {
      // All other files: show a professional file card
      const icon = fileIcon(m.content);
      let sizeStr = '';
      try { if (m.path && fs.existsSync(m.path)) sizeStr = fmtSize(fs.statSync(m.path).size); } catch {}
      body = `<div class="file-card" data-openfile="${esc(m.path)}" title="Click to open">
        <div class="file-icon">${icon}</div>
        <div class="file-info">
          <span class="fname">${esc(m.content)}</span>
          ${sizeStr ? `<span class="fsize">${sizeStr}</span>` : ''}
          <span class="file-open-hint">Click to open</span>
        </div>
      </div>`;
    }
  }
  else body = esc(m.content).replace(/\n/g, '<br>');
  const tick = me ? `<span class="tk ${m.status}">${{ sent: '✓', delivered: '✓✓', read: '✓✓' }[m.status] || ''}</span>` : '';
  return `<div class="bub ${me ? 'me' : 'them'}" data-mid="${m.id}">
    ${!me && c.group ? `<span class="who">${esc(db.users[m.senderId]?.name || 'Unknown')}</span>` : ''}
    ${q ? `<div class="quote">${esc(q.deleted ? 'Deleted message' : preview(q)).slice(0, 80)}</div>` : ''}
    ${body}<div class="meta"><span>${fmt(m.ts)}</span>${tick}</div>
    <span class="acts"><a data-reply="${m.id}">Reply</a>${m.type === 'text' && !m.deleted ? `<a data-copy="${m.id}">Copy</a>` : ''}${me && !m.deleted ? `<a data-del="${m.id}">Delete</a>` : ''}</span></div>`;
}
function rChat() {
  const c = db.chats[openId]; if (!c) return;
  const o = other(c), ty = Object.keys(typing[openId] || {});

  // Update chat header (avatar + name + status)
  const chatAvWrap = document.getElementById('chat-av-wrap');
  if (chatAvWrap) chatAvWrap.innerHTML = av(o || null, cname(c));
  $('#chatHead').innerHTML = `<b>${esc(cname(c))}</b><br><small class="${ty.length ? 'it' : ''}">${ty.length ? 'typing\u2026' : c.group ? c.ids.length + ' members' : o?.online ? '\u25cf\u00a0online' : '\u25cb\u00a0offline'}</small>`;
  $('#bBlock').hidden = c.group; if (o) $('#bBlock').textContent = o.blocked ? 'Unblock' : 'Block';

  const box = $('#msgs');
  const msgs = db.msgs[openId] || [];
  const atEnd = box.scrollHeight - box.scrollTop - box.clientHeight < 90;

  const dateSep = (ts) => {
    const ds = new Date(ts).toDateString();
    const now = new Date(), today = now.toDateString(), yest = new Date(now - 86400000).toDateString();
    const label = ds === today ? 'Today' : ds === yest ? 'Yesterday' : new Date(ts).toLocaleDateString([], { day: 'numeric', month: 'long', year: 'numeric' });
    return `<div class="date-sep" data-date="${esc(ds)}"><span>${label}</span></div>`;
  };

  if (fresh) {
    // Full rebuild with date separators — only runs when chat first opens
    let html = '', lastDate = null;
    for (const m of msgs) {
      const ds = new Date(m.ts).toDateString();
      if (ds !== lastDate) { lastDate = ds; html += dateSep(m.ts); }
      html += bubble(m);
    }
    box.innerHTML = html;
  } else {
    // Incremental update — zero flicker, only touch what changed
    for (const m of msgs) {
      const existing = box.querySelector(`.bub[data-mid="${CSS.escape(m.id)}"]`);
      if (!existing) {
        // Brand-new message — insert date separator if needed, then append
        const ds = new Date(m.ts).toDateString();
        const allSeps = box.querySelectorAll('.date-sep');
        const lastSep = allSeps[allSeps.length - 1];
        if (!lastSep || lastSep.dataset.date !== ds) box.insertAdjacentHTML('beforeend', dateSep(m.ts));
        box.insertAdjacentHTML('beforeend', bubble(m));
      } else {
        // File transfer just completed — swap progress bar out for real content
        if (m.status !== 'receiving' && m.status !== 'failed' && !m.deleted && existing.querySelector('.bar')) {
          const tmp = document.createElement('div'); tmp.innerHTML = bubble(m);
          existing.replaceWith(tmp.firstElementChild); continue;
        }
        // Tick (sent → delivered → read)
        const tk = existing.querySelector('.tk');
        if (tk) { tk.className = `tk ${m.status}`; tk.textContent = { sent: '\u2713', delivered: '\u2713\u2713', read: '\u2713\u2713' }[m.status] || ''; }
        // Live progress bar + pct label + size label
        if (m.status === 'receiving') {
          const pct = Math.round((prog[m.id] || 0) * 100);
          const barEl = existing.querySelector('.bar i'); if (barEl) barEl.style.width = pct + '%';
          const pctEl = existing.querySelector('.prg-pct'); if (pctEl) pctEl.textContent = pct + '%';
          if (m.filesize) {
            const subEl = existing.querySelector('.prg-sub');
            if (subEl) subEl.textContent = `${fmtSize(Math.round((prog[m.id] || 0) * m.filesize))} / ${fmtSize(m.filesize)}`;
          }
        }
        // Deleted message — replace in-place
        if (m.deleted && !existing.querySelector('.del')) {
          const tmp = document.createElement('div'); tmp.innerHTML = bubble(m);
          existing.replaceWith(tmp.firstElementChild);
        }
      }
    }
  }

  if (fresh || atEnd) box.scrollTop = box.scrollHeight;
  fresh = false;

  // Keep scroll-to-bottom button in sync
  const sd = $('#scrollDown');
  if (sd) sd.hidden = box.scrollHeight - box.scrollTop - box.clientHeight < 80;
}

/* ---------- events ---------- */
document.addEventListener('click', e => {
  // Handle file open clicks (file cards and inline images)
  const fo = e.target.closest('[data-openfile]');
  if (fo) { e.preventDefault(); openFile(fo.dataset.openfile); return; }

  const t = e.target.closest('[data-open],[data-user],[data-reply],[data-del],[data-unblock],[data-copy]'); if (!t) return;
  const d = t.dataset;
  if (d.open) openChat(d.open);
  else if (d.user) startChat(d.user);
  else if (d.reply) { replyTo = d.reply; const q = findMsg(replyTo); $('#replyBar').innerHTML = `<span>${esc(preview(q)).slice(0, 90)}</span><a data-cancelreply style="cursor:pointer">✕</a>`; $('#replyBar').hidden = false; $('#txt').focus(); }
  else if (d.del) delMsg(d.del);
  else if (d.copy) { const cm = findMsg(d.copy); if (cm) navigator.clipboard.writeText(cm.content).catch(() => {}); }
  else if (d.unblock) { db.users[d.unblock].blocked = false; save(); rSet(); }
});
$('#replyBar').onclick = e => { if (e.target.closest('[data-cancelreply]')) { replyTo = null; $('#replyBar').hidden = true; } };

$('#nameGo').onclick = () => { const n = $('#nameIn').value.trim(); if (!n) return $('#nameIn').focus(); myName = n; localStorage.setItem('display_name', n); updateMyProfile(); show('home'); };
$('#nameIn').onkeydown = e => { if (e.key === 'Enter') $('#nameGo').click(); };
$('#sSave').onclick = () => { const n = $('#sName').value.trim(); if (n) { myName = n; localStorage.setItem('display_name', n); updateMyProfile(); } };
$('#thm').onclick = () => { const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'; document.documentElement.dataset.theme = t; localStorage.setItem('theme', t); };
$('#sPhoto').onchange = async e => {
  const f = e.target.files[0]; if (!f) return;
  const old = localStorage.getItem('my_photo'); if (old) fs.unlink(old, () => {});
  const p = path.join(DIR, `me_${Date.now()}_${f.name}`); fs.writeFileSync(p, Buffer.from(await f.arrayBuffer()));
  localStorage.setItem('my_photo', p);
  Object.values(db.users).forEach(u => { u.photoSent = 0; if (online(u)) pushPhoto(u); }); save(); updateMyProfile();
};
$('#bBlock').onclick = () => { const o = other(db.chats[openId]); if (o) { o.blocked = !o.blocked; save(); ui(); } };
$('#gGo').onclick = () => {
  const name = $('#gName').value.trim(), ids = [...document.querySelectorAll('#gList input:checked')].map(i => i.value);
  if (!name || !ids.length) return alert('Enter a group name and pick at least one person.');
  const id = String(Date.now()); ensureChat(id, true, name, [myId, ...ids]); db.chats[id].last = Date.now(); save(); openChat(id);
};

const txt = $('#txt');
function toggleBtns() { const has = txt.value.trim().length > 0; $('#sendBtn').hidden = !has; $('#mic').hidden = has || !!mr; }
function doSend() {
  const v = txt.value.trim(); if (!v) return;
  send(openId, v, replyTo); txt.value = ''; txt.style.height = 'auto'; replyTo = null; $('#replyBar').hidden = true; toggleBtns();
}
txt.oninput = () => { txt.style.height = 'auto'; txt.style.height = Math.min(txt.scrollHeight, 100) + 'px'; toggleBtns(); sendTyping(); };
txt.onkeydown = e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doSend(); } };
$('#sendBtn').onclick = doSend;
$('#attach').onclick = () => $('#fileIn').click();
$('#fileIn').onchange = async e => {
  const f = e.target.files[0]; if (!f) return;
  const p = path.join(DIR, `${Date.now()}_${f.name}`); fs.writeFileSync(p, Buffer.from(await f.arrayBuffer()));
  sendFile(openId, p); e.target.value = '';
};

/* voice: click mic to start, click again to send */
let mr = null, chunks = [], recTimer, recSec = 0;
async function recStart() {
  try { const s = await navigator.mediaDevices.getUserMedia({ audio: true }); mr = new MediaRecorder(s); }
  catch { return alert('Microphone access is blocked.'); }
  chunks = []; mr.ondataavailable = e => chunks.push(e.data); mr.start();
  recSec = 0; $('#mic').classList.add('rec'); $('#mic').textContent = 'Send 0s'; $('#cancel').hidden = false;
  recTimer = setInterval(() => { $('#mic').textContent = `Send ${++recSec}s`; }, 1000);
}
function recStop(sendIt) {
  if (!mr) return; const r = mr, chat = openId; mr = null; clearInterval(recTimer);
  r.onstop = async () => {
    r.stream.getTracks().forEach(t => t.stop());
    if (sendIt && chunks.length) { const f = path.join(DIR, `voice_${Date.now()}.webm`); fs.writeFileSync(f, Buffer.from(await new Blob(chunks).arrayBuffer())); sendFile(chat, f, 'voice'); }
  };
  r.stop(); $('#mic').classList.remove('rec'); $('#mic').textContent = 'Mic'; $('#cancel').hidden = true; toggleBtns();
}
$('#mic').onclick = () => mr ? recStop(true) : recStart();
$('#cancel').onclick = () => recStop(false);

/* ---------- my profile (sidebar + settings) ---------- */
function updateMyProfile() {
  const myPhoto = localStorage.getItem('my_photo');
  const hasPhoto = myPhoto && fs.existsSync(myPhoto);
  const imgEl = hasPhoto ? `<img src="${furl(myPhoto)}" alt="me" onerror="this.style.display='none'">` : '';
  const content = `${initials(myName)}${imgEl}`;
  const myAv = document.getElementById('my-av'); if (myAv) myAv.innerHTML = content;
  const profileAv = document.getElementById('my-profile-av'); if (profileAv) profileAv.innerHTML = content;
  const nameLabel = document.getElementById('my-name-label'); if (nameLabel) nameLabel.textContent = myName || 'Local Chat';
  const profileName = document.getElementById('my-profile-name'); if (profileName) profileName.textContent = myName || '\u2014';
}

/* ---------- notification sound (lazy AudioContext) ---------- */
let _audioCtx = null;
function getAudioCtx() {
  if (!_audioCtx) try { _audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch(e) {}
  return _audioCtx;
}
function playNotif() {
  const ctx = getAudioCtx(); if (!ctx) return;
  try {
    const g = ctx.createGain(); g.connect(ctx.destination);
    [880, 1100].forEach((freq, i) => {
      const o = ctx.createOscillator(); o.connect(g);
      o.type = 'sine'; o.frequency.value = freq;
      const t = ctx.currentTime + i * 0.18;
      g.gain.setValueAtTime(0, t);
      g.gain.linearRampToValueAtTime(0.22, t + 0.02);
      g.gain.exponentialRampToValueAtTime(0.001, t + 0.38);
      o.start(t); o.stop(t + 0.4);
    });
  } catch(e) {}
}
// Unlock AudioContext on first user interaction
document.addEventListener('click', () => getAudioCtx(), { once: true });

/* ---------- search ---------- */
$('#searchIn').addEventListener('input', () => { searchQuery = $('#searchIn').value.trim(); rHome(); });

/* ---------- chat back button (mobile) ---------- */
document.getElementById('chat-back').addEventListener('click', () => {
  openId = null; replyTo = null; fresh = false;
  document.getElementById('welcome-screen').hidden = false;
  document.getElementById('chat').hidden = true;
  document.getElementById('sidebar').classList.remove('slide-out');
  cur = 'home';
  document.querySelectorAll('.left-view').forEach(el => el.classList.toggle('active', el.id === 'home'));
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('nav-active', btn.dataset.go === 'home'));
  rHome();
});

/* ---------- scroll-to-bottom button ---------- */
$('#msgs').addEventListener('scroll', () => {
  const b = $('#msgs'), sd = $('#scrollDown');
  if (sd) sd.hidden = b.scrollHeight - b.scrollTop - b.clientHeight < 80;
});
$('#scrollDown').onclick = () => { const b = $('#msgs'); b.scrollTop = b.scrollHeight; };

/* ---------- drag-and-drop files into chat ---------- */
const chatEl = document.getElementById('chat');
chatEl.addEventListener('dragover', e => { e.preventDefault(); if (openId) chatEl.classList.add('drag-over'); });
chatEl.addEventListener('dragleave', e => { if (!chatEl.contains(e.relatedTarget)) chatEl.classList.remove('drag-over'); });
chatEl.addEventListener('drop', async e => {
  e.preventDefault(); chatEl.classList.remove('drag-over');
  if (!openId) return;
  for (const f of [...e.dataTransfer.files]) {
    const p = path.join(DIR, `${Date.now()}_${f.name}`);
    fs.writeFileSync(p, Buffer.from(await f.arrayBuffer()));
    sendFile(openId, p);
  }
});

/* ---------- image lightbox ---------- */
document.addEventListener('click', e => {
  const pic = e.target.closest('.pic');
  if (!pic) return;
  const lb = document.getElementById('lightbox');
  const lbImg = document.getElementById('lightbox-img');
  const lbOpen = document.getElementById('lightbox-open');
  lbImg.src = pic.src;
  if (lbOpen) { lbOpen.href = '#'; lbOpen.onclick = ev => { ev.preventDefault(); openFile(pic.dataset.openfile || ''); }; }
  lb.hidden = false;
});
document.getElementById('lightbox-bg').addEventListener('click', () => { document.getElementById('lightbox').hidden = true; });
document.getElementById('lightbox-close').addEventListener('click', () => { document.getElementById('lightbox').hidden = true; });
document.addEventListener('keydown', e => { if (e.key === 'Escape') { document.getElementById('lightbox').hidden = true; document.getElementById('ctx-menu').hidden = true; } });

/* ---------- right-click context menu ---------- */
let ctxTarget = null;
document.addEventListener('contextmenu', e => {
  const bub = e.target.closest('.bub');
  document.getElementById('ctx-menu').hidden = true;
  if (!bub) return;
  e.preventDefault();
  ctxTarget = bub.dataset.mid;
  const m = findMsg(ctxTarget); if (!m) return;
  document.getElementById('ctx-del').hidden = m.senderId !== myId || !!m.deleted;
  document.getElementById('ctx-copy').hidden = m.type !== 'text' || !!m.deleted;
  const menu = document.getElementById('ctx-menu');
  menu.style.left = Math.min(e.clientX, window.innerWidth - 180) + 'px';
  menu.style.top = Math.min(e.clientY, window.innerHeight - 140) + 'px';
  menu.hidden = false;
});
document.addEventListener('click', e => { if (!e.target.closest('#ctx-menu')) document.getElementById('ctx-menu').hidden = true; });
document.getElementById('ctx-reply').addEventListener('click', () => {
  document.getElementById('ctx-menu').hidden = true;
  if (!ctxTarget) return;
  replyTo = ctxTarget; const q = findMsg(replyTo);
  $('#replyBar').innerHTML = `<span>${esc(preview(q)).slice(0, 90)}</span><a data-cancelreply style="cursor:pointer">✕</a>`;
  $('#replyBar').hidden = false; $('#txt').focus();
});
document.getElementById('ctx-copy').addEventListener('click', () => {
  document.getElementById('ctx-menu').hidden = true;
  const m = findMsg(ctxTarget); if (m) navigator.clipboard.writeText(m.content).catch(() => {});
});
document.getElementById('ctx-del').addEventListener('click', () => {
  document.getElementById('ctx-menu').hidden = true;
  if (ctxTarget) delMsg(ctxTarget);
});

/* ---------- boot ---------- */
setInterval(() => { if (cur === 'home') rHome(); }, 30000);
updateMyProfile();
show(myName ? 'home' : 'onboard');

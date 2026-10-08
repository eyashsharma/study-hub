import http from 'node:http';
import telegram from 'telegram';
import 'dotenv/config';

const { TelegramClient, Api, sessions } = telegram;
const { StringSession } = sessions;

const {
  TELEGRAM_API_ID,
  TELEGRAM_API_HASH,
  SESSION_STRING,
  TELEGRAM_BOT_TOKEN,
  STREAM_TOKEN,
  PORT = '8080',
} = process.env;

if (!TELEGRAM_API_ID || !TELEGRAM_API_HASH || !STREAM_TOKEN) {
  console.error('Missing required env: TELEGRAM_API_ID, TELEGRAM_API_HASH, STREAM_TOKEN');
  process.exit(1);
}

// Application-level streaming/cache window: each HTTP response serves at most 64 MB,
// so the browser keeps issuing Range requests for the rest. Bounds memory + runtime per request.
const CHUNK_WINDOW = 64 * 1024 * 1024;
// Per-request MTProto chunk (upload.getFile hard limit is 1 MB).
const MTPROTO_LIMIT = 1024 * 1024;

let client;

async function initClient() {
  const apiId = parseInt(TELEGRAM_API_ID, 10);
  const opts = { connectionRetries: 5 };

  if (SESSION_STRING && SESSION_STRING.trim().length > 20) {
    client = new TelegramClient(new StringSession(SESSION_STRING), apiId, TELEGRAM_API_HASH, opts);
    await client.connect();
    console.log('MTProto client ready (user session).');
  } else if (TELEGRAM_BOT_TOKEN) {
    client = new TelegramClient(new StringSession(''), apiId, TELEGRAM_API_HASH, opts);
    await client.start({ botAuthToken: TELEGRAM_BOT_TOKEN });
    console.log('MTProto client ready (bot session).');
  } else {
    console.error('Provide either SESSION_STRING (recommended) or TELEGRAM_BOT_TOKEN.');
    process.exit(1);
  }
}

// Cache resolved Telegram entities so a numeric chat id (e.g. -100…) can be
// looked up later without re-resolving its username.
const entityCache = new Map();

function cacheEntity(key, entity) {
  if (key) entityCache.set(String(key), entity);
  const id = entity && entity.id != null ? String(entity.id) : null;
  if (id) {
    entityCache.set(id, entity);
    entityCache.set(`-100${id}`, entity);
    entityCache.set(`-${id}`, entity);
  }
}

// Accepts a username ('name' / '@name') or a numeric chat id; returns the GramJS entity.
async function resolveEntity(ref) {
  const key = String(ref ?? '').trim();
  if (!key) throw new Error('Missing channel reference');
  if (entityCache.has(key)) return entityCache.get(key);
  const entity = await client.getEntity(key.startsWith('@') ? key.slice(1) : key);
  cacheEntity(key, entity);
  return entity;
}

// Re-resolve the message by id to get a fresh document + fileReference (fileReference expires).
async function getMessageDocument(channel, messageId) {
  const entity = await resolveEntity(channel);
  const messages = await client.getMessages(entity, { ids: [messageId] });
  const msg = messages && messages[0];
  if (!msg || !msg.media || !msg.media.document) {
    throw new Error('Message has no downloadable video document');
  }
  return msg.media.document;
}

// Walk the chat history and collect every video attachment (metadata only).
async function listVideos(ref, limit) {
  const entity = await resolveEntity(ref);
  const messages = await client.getMessages(entity, { limit });
  const videos = [];
  for (const m of messages) {
    const doc = m && m.media && m.media.document;
    if (!doc || !(doc.mimeType || '').startsWith('video/')) continue;
    const attrs = doc.attributes || [];
    const videoAttr = attrs.find((a) => a.className === 'DocumentAttributeVideo');
    const fileAttr = attrs.find((a) => a.className === 'DocumentAttributeFilename');
    videos.push({
      message_id: m.id,
      size: Number(doc.size || 0),
      mime: doc.mimeType || 'video/mp4',
      file_name: (fileAttr && fileAttr.fileName) || null,
      duration: (videoAttr && videoAttr.duration) || 0,
      caption: m.message || '',
      date: m.date ? new Date(m.date * 1000).toISOString() : null,
    });
  }
  return {
    videos,
    channel: {
      id: entity && entity.id != null ? String(entity.id) : null,
      username: (entity && entity.username) || null,
      title: (entity && entity.title) || null,
    },
  };
}

// GET /videos?channel=<chatId|@username>&limit=<n>&token=<token>
// Returns every video message already posted in the chat — used to backfill
// the StudyHub catalog, since the Bot API cannot return past messages.
async function handleList(res, query) {
  if (!query.token || query.token !== STREAM_TOKEN) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'Unauthorized' }));
    return;
  }
  const ref = query.channel || process.env.TELEGRAM_CHANNEL_ID;
  if (!ref) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'Missing channel' }));
    return;
  }
  const limit = Math.min(parseInt(query.limit, 10) || 200, 500);
  try {
    const { channel, videos } = await listVideos(ref, limit);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, channel, count: videos.length, videos }));
  } catch (e) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: e.message }));
  }
}

function buildLocation(doc) {
  return new Api.InputDocumentFileLocation({
    id: doc.id,
    accessHash: doc.accessHash,
    fileReference: doc.fileReference,
    thumbSize: '',
  });
}

// Stream [start, start+length) bytes of doc to res, using MTProto chunked download.
async function pump(res, doc, channel, messageId, start, length, total) {
  let written = 0;
  let currentOffset = start;
  let curDoc = doc;
  let refreshAttempts = 0;

  while (written < length) {
    const iter = client.iterDownload({
      fileLocation: buildLocation(curDoc),
      offset: currentOffset,
      limit: MTPROTO_LIMIT,
      fileSize: total,
      dcId: curDoc.dcId,
    });
    try {
      for await (const chunk of iter) {
        if (!chunk || chunk.length === 0) continue;
        const remaining = length - written;
        const slice = chunk.length > remaining ? chunk.subarray(0, remaining) : chunk;
        if (!res.write(slice)) {
          await new Promise((resolve) => res.once('drain', resolve));
        }
        written += slice.length;
        currentOffset += slice.length;
        if (written >= length) return;
      }
      return; // iterator exhausted
    } catch (err) {
      const msg = String(err && err.message ? err.message : err);
      if (refreshAttempts < 2 && /FILE_REFERENCE|file reference|FLOOD_WAIT|Timeout|SESSION_REVOKED/i.test(msg)) {
        refreshAttempts++;
        curDoc = await getMessageDocument(channel, messageId);
        continue;
      }
      throw err;
    }
  }
}

async function handleStream(req, res, query) {
  if (!query.token || query.token !== STREAM_TOKEN) {
    res.writeHead(401, { 'Content-Type': 'text/plain' });
    res.end('Unauthorized');
    return;
  }
  const { channel, uname, msg, size, mime } = query;
  const ref = uname || channel;
  if (!ref || !msg) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Missing channel or msg');
    return;
  }
  const messageId = parseInt(msg, 10);

  let doc;
  try {
    doc = await getMessageDocument(ref, messageId);
  } catch (e) {
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end('Failed to resolve Telegram message: ' + e.message);
    return;
  }

  const total = parseInt(size, 10) || Number(doc.size) || 0;
  if (!total) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Unknown file size');
    return;
  }
  const contentType = mime || doc.mimeType || 'video/mp4';

  // Parse Range (browser <video> always sends one while seeking).
  let start = 0;
  let end = total - 1;
  const range = req.headers.range;
  if (range) {
    const m = /bytes=(\d*)-(\d*)/.exec(range);
    if (m) {
      if (m[1]) start = parseInt(m[1], 10);
      if (m[2]) end = parseInt(m[2], 10);
      if (end > total - 1) end = total - 1;
    }
  }
  // Cap the served window to 64 MB so each response is bounded.
  const maxEnd = start + CHUNK_WINDOW - 1;
  if (end > maxEnd) end = maxEnd;

  if (start < 0 || start >= total || start > end) {
    res.writeHead(416, { 'Content-Range': `bytes */${total}` });
    res.end();
    return;
  }

  const length = end - start + 1;
  res.writeHead(206, {
    'Content-Type': contentType,
    'Accept-Ranges': 'bytes',
    'Content-Range': `bytes ${start}-${end}/${total}`,
    'Content-Length': String(length),
    'Cache-Control': 'public, max-age=3600',
    'Access-Control-Allow-Origin': '*',
  });

  try {
    await pump(res, doc, ref, messageId, start, length, total);
    res.end();
  } catch (err) {
    console.error('Stream error:', err.message);
    try { res.destroy(); } catch (_) {}
  }
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  if (url.pathname === '/videos' || url.pathname === '/list') {
    const query = Object.fromEntries(url.searchParams.entries());
    handleList(res, query).catch((e) => {
      console.error('List error:', e.message);
      try {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      } catch (_) {}
    });
    return;
  }
  if (url.pathname === '/stream' || url.pathname.startsWith('/stream/')) {
    const query = Object.fromEntries(url.searchParams.entries());
    handleStream(req, res, query).catch((e) => {
      console.error('Handler error:', e.message);
      try { res.writeHead(500); res.end('Internal error'); } catch (_) {}
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

const listen = () =>
  server.listen(PORT, () => {
    console.log(`StudyHub MTProto streamer listening on :${PORT}`);
  });

// Sandbox preview only (BASE44_PREVIEW_MODE === '1'): the preview egress proxy cannot
// reach Telegram's MTProto servers (raw TCP/TLS is blocked), so awaiting the client here
// would leave the HTTP server never listening. Start serving immediately and connect in
// the background. When the flag is unset, behaviour is unchanged (connect first, then listen).
if (process.env.BASE44_PREVIEW_MODE === '1') {
  listen();
  initClient().catch((e) => {
    console.error('Telegram client unavailable (HTTP server still running):', e && e.message);
  });
} else {
  await initClient();
  listen();
}

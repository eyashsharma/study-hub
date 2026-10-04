import http from 'node:http';
import { TelegramClient, Api, sessions } from 'telegram';
import 'dotenv/config';

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

// Re-resolve the message by id to get a fresh document + fileReference (fileReference expires).
async function getMessageDocument(channel, messageId) {
  const messages = await client.getMessages(channel, { ids: [messageId] });
  const msg = messages && messages[0];
  if (!msg || !msg.media || !msg.media.document) {
    throw new Error('Message has no downloadable video document');
  }
  return msg.media.document;
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
  const { channel, msg, size, mime } = query;
  if (!channel || !msg) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Missing channel or msg');
    return;
  }
  const messageId = parseInt(msg, 10);

  let doc;
  try {
    doc = await getMessageDocument(channel, messageId);
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
    await pump(res, doc, channel, messageId, start, length, total);
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

await initClient();
server.listen(PORT, () => {
  console.log(`StudyHub MTProto streamer listening on :${PORT}`);
});

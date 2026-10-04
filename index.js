import http from 'node:http';
import telegram from 'telegram';
import 'dotenv/config';

const { TelegramClient, Api, StringSession } = telegram;

const {
  TELEGRAM_API_ID,
  TELEGRAM_API_HASH,
  SESSION_STRING,
  TELEGRAM_BOT_TOKEN,
  STREAM_TOKEN,
  PORT = '8080',
} = process.env;

if (!TELEGRAM_API_ID || !TELEGRAM_API_HASH || !STREAM_TOKEN) {
  console.error(
    'Missing required env: TELEGRAM_API_ID, TELEGRAM_API_HASH, STREAM_TOKEN'
  );
  process.exit(1);
}

// Maximum HTTP response window: 64 MB
const CHUNK_WINDOW = 64 * 1024 * 1024;

// Telegram MTProto fetch size: 1 MB
const MTPROTO_LIMIT = 1024 * 1024;

let client;

async function initClient() {
  const apiId = parseInt(TELEGRAM_API_ID, 10);
  const opts = {
    connectionRetries: 5,
  };

  if (SESSION_STRING && SESSION_STRING.trim().length > 20) {
    client = new TelegramClient(
      new StringSession(SESSION_STRING),
      apiId,
      TELEGRAM_API_HASH,
      opts
    );

    await client.connect();

    console.log('MTProto client ready (user session).');
  } else if (TELEGRAM_BOT_TOKEN) {
    client = new TelegramClient(
      new StringSession(''),
      apiId,
      TELEGRAM_API_HASH,
      opts
    );

    await client.start({
      botAuthToken: TELEGRAM_BOT_TOKEN,
    });

    console.log('MTProto client ready (bot session).');
  } else {
    console.error(
      'Provide either SESSION_STRING or TELEGRAM_BOT_TOKEN.'
    );
    process.exit(1);
  }
}

// Get a fresh Telegram document/fileReference.
async function getMessageDocument(channel, messageId) {
  const messages = await client.getMessages(channel, {
    ids: [messageId],
  });

  const msg = messages && messages[0];

  if (!msg || !msg.media || !msg.media.document) {
    throw new Error(
      'Message has no downloadable video document'
    );
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

// Stream requested range from Telegram through MTProto.
async function pump(
  res,
  doc,
  channel,
  messageId,
  start,
  length,
  total
) {
  let written = 0;
  let currentOffset = start;
  let currentDoc = doc;
  let refreshAttempts = 0;

  while (written < length) {
    const iter = client.iterDownload({
      fileLocation: buildLocation(currentDoc),
      offset: currentOffset,
      limit: MTPROTO_LIMIT,
      fileSize: total,
      dcId: currentDoc.dcId,
    });

    try {
      for await (const chunk of iter) {
        if (!chunk || chunk.length === 0) {
          continue;
        }

        const remaining = length - written;

        const slice =
          chunk.length > remaining
            ? chunk.subarray(0, remaining)
            : chunk;

        if (!res.write(slice)) {
          await new Promise((resolve) =>
            res.once('drain', resolve)
          );
        }

        written += slice.length;
        currentOffset += slice.length;

        if (written >= length) {
          return;
        }
      }

      return;
    } catch (err) {
      const message = String(
        err?.message || err
      );

      if (
        refreshAttempts < 2 &&
        /FILE_REFERENCE|file reference|FLOOD_WAIT|Timeout|SESSION_REVOKED/i.test(
          message
        )
      ) {
        refreshAttempts++;

        console.log(
          `Refreshing Telegram file reference (attempt ${refreshAttempts})`
        );

        currentDoc = await getMessageDocument(
          channel,
          messageId
        );

        continue;
      }

      throw err;
    }
  }
}

async function handleStream(req, res, query) {
  // Authenticate streamer request.
  if (!query.token || query.token !== STREAM_TOKEN) {
    res.writeHead(401, {
      'Content-Type': 'text/plain',
    });

    res.end('Unauthorized');
    return;
  }

  const {
    channel,
    msg,
    size,
    mime,
  } = query;

  if (!channel || !msg) {
    res.writeHead(400, {
      'Content-Type': 'text/plain',
    });

    res.end('Missing channel or msg');
    return;
  }

  const messageId = parseInt(msg, 10);

  if (!Number.isFinite(messageId)) {
    res.writeHead(400, {
      'Content-Type': 'text/plain',
    });

    res.end('Invalid message ID');
    return;
  }

  let doc;

  try {
    doc = await getMessageDocument(
      channel,
      messageId
    );
  } catch (error) {
    console.error(
      'Telegram message error:',
      error.message
    );

    res.writeHead(502, {
      'Content-Type': 'text/plain',
    });

    res.end(
      'Failed to resolve Telegram message'
    );

    return;
  }

  const total =
    parseInt(size, 10) ||
    Number(doc.size) ||
    0;

  if (!total) {
    res.writeHead(500, {
      'Content-Type': 'text/plain',
    });

    res.end('Unknown file size');
    return;
  }

  const contentType =
    mime ||
    doc.mimeType ||
    'video/mp4';

  // Parse HTTP Range header.
  let start = 0;
  let end = total - 1;

  const range = req.headers.range;

  if (range) {
    const match =
      /bytes=(\d*)-(\d*)/.exec(range);

    if (match) {
      if (match[1]) {
        start = parseInt(match[1], 10);
      }

      if (match[2]) {
        end = parseInt(match[2], 10);
      }

      if (end > total - 1) {
        end = total - 1;
      }
    }
  }

  // Never serve more than 64 MB in one HTTP response.
  const maxEnd =
    start + CHUNK_WINDOW - 1;

  if (end > maxEnd) {
    end = maxEnd;
  }

  if (
    start < 0 ||
    start >= total ||
    start > end
  ) {
    res.writeHead(416, {
      'Content-Range': `bytes */${total}`,
    });

    res.end();
    return;
  }

  const length =
    end - start + 1;

  res.writeHead(206, {
    'Content-Type': contentType,
    'Accept-Ranges': 'bytes',
    'Content-Range':
      `bytes ${start}-${end}/${total}`,
    'Content-Length': String(length),
    'Cache-Control':
      'public, max-age=3600',
    'Access-Control-Allow-Origin': '*',
  });

  try {
    await pump(
      res,
      doc,
      channel,
      messageId,
      start,
      length,
      total
    );

    res.end();
  } catch (error) {
    console.error(
      'Stream error:',
      error.message
    );

    try {
      res.destroy();
    } catch (_) {}
  }
}

const server = http.createServer(
  (req, res) => {
    const url = new URL(
      req.url,
      `http://${req.headers.host}`
    );

    // Health check
    if (url.pathname === '/health') {
      res.writeHead(200, {
        'Content-Type': 'text/plain',
      });

      res.end('ok');
      return;
    }

    // Video streaming endpoint
    if (
      url.pathname === '/stream' ||
      url.pathname.startsWith('/stream/')
    ) {
      const query =
        Object.fromEntries(
          url.searchParams.entries()
        );

      handleStream(
        req,
        res,
        query
      ).catch((error) => {
        console.error(
          'Handler error:',
          error.message
        );

        try {
          if (!res.headersSent) {
            res.writeHead(500);
          }

          res.end('Internal error');
        } catch (_) {}
      });

      return;
    }
    res.writeHead(404, {
      'Content-Type': 'text/plain',
    });

    res.end('Not found');
  }
);

await initClient();
server.listen(
  PORT,
  () => {
    console.log(
      `StudyHub MTProto streamer listening on :${PORT}`
    );
  }
);

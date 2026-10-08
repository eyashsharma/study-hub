const telegram = require('telegram');
const { TelegramClient, sessions } = telegram;
const { StringSession } = sessions;
(async () => {
  const client = new TelegramClient(
    new StringSession(process.env.SESSION_STRING),
    parseInt(process.env.TELEGRAM_API_ID, 10),
    process.env.TELEGRAM_API_HASH,
    { connectionRetries: 2, useWSS: true }
  );
  console.log('connecting with WSS transport...');
  const t0 = Date.now();
  await client.connect();
  console.log('WSS connect RESOLVED in', Date.now() - t0, 'ms');
  process.exit(0);
})().catch((e) => { console.log('WSS connect FAILED:', e && e.message); process.exit(1); });

import readline from 'node:readline';
import { TelegramClient, sessions } from 'telegram';
import 'dotenv/config';

const { StringSession } = sessions;

const { TELEGRAM_API_ID, TELEGRAM_API_HASH } = process.env;
if (!TELEGRAM_API_ID || !TELEGRAM_API_HASH) {
  console.error('Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env first (from https://my.telegram.org).');
  process.exit(1);
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise((r) => rl.question(q, r));

(async () => {
  const client = new TelegramClient(
    new StringSession(''),
    parseInt(TELEGRAM_API_ID, 10),
    TELEGRAM_API_HASH,
    { connectionRetries: 5 },
  );

  await client.start({
    phoneNumber: async () => ask('Phone number (international, e.g. +91...): '),
    password: async () => ask('2FA password (leave blank if none): '),
    phoneCode: async () => ask('Login code: '),
    onError: (e) => console.error(e),
  });

  const saved = client.session.save();
  console.log('\nDone. Copy this SESSION_STRING into your .env:\n');
  console.log(saved);
  console.log('\n(Keep it private — it grants full access to your account.)');
  rl.close();
  process.exit(0);
})();

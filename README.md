# 🇪🇹 Ethiopian Payment Verifier Bot

A Telegram bot that automates paid channel subscriptions by verifying Ethiopian bank payments in real time. Supports CBE, Telebirr, Dashen Bank, Bank of Abyssinia, CBE Birr, and receipt screenshot analysis.

---

## ✨ Features

- **Automatic payment verification** — CBE, Telebirr, Dashen, Abyssinia, CBE Birr, and screenshot OCR
- **Multi-channel support** — One bot manages unlimited paid channels
- **Subscription lifecycle** — Auto-issue invite links, warn before expiry, kick on expiry
- **Admin dashboard** — Register channels, view members, export CSV, broadcast messages
- **Fraud prevention** — Duplicate transaction detection, 24h age limit, exact amount & account matching
- **Docker-ready** — Single `docker compose up` deployment
- **SQLite storage** — Zero-infrastructure, file-based database with WAL mode

---

## 🚀 Quick Start

### Prerequisites
- Python 3.12+ or Docker
- A Telegram bot token (from [@BotFather](https://t.me/BotFather))
- A payment verification API key

### 1. Clone & Configure

```bash
git clone https://github.com/By-Michael/WanaVerify.git
cd et-payment-verifier-bot

# Copy the example config and fill in your values
cp .env.example .env
nano .env
```

### 2. Run with Docker (recommended)

```bash
# Create the data directory for the SQLite database
mkdir -p data

docker compose up -d
docker compose logs -f
```

### 3. Run locally

```bash
pip install -r requirements.txt
python et_payment_verifier_bot.py
```

---

## ⚙️ Configuration

All settings live in `.env` (copy from `.env.example`):

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | ✅ | From @BotFather |
| `BOT_USERNAME` | ✅ | Your bot's username (without @) |
| `VERIFIER_API_KEY` | ✅ | Payment verification API key |
| `VERIFIER_API_BASE` | Optional | API base URL (default: `https://verifyapi.leulzenebe.pro`) |
| `DATABASE_PATH` | Optional | SQLite DB path (default: `bot.db`) |
| `AMOUNT_TOLERANCE_PCT` | Optional | Allowed payment amount variance % (default: `2`) |
| `TXN_MAX_AGE_HOURS` | Optional | Max transaction age in hours (default: `24`) |
| `INVITE_LINK_EXPIRY_HOURS` | Optional | Invite link validity (default: `1`) |
| `MAX_CHANNELS_PER_ADMIN` | Optional | Max channels per admin (default: `10`) |
| `WARNING_CHECK_INTERVAL_HOURS` | Optional | Expiry warning frequency (default: `6`) |
| `KICK_CHECK_INTERVAL_HOURS` | Optional | Expiry kick check frequency (default: `1`) |
| `ADMIN_ALERT_CHAT_ID` | Optional | Chat ID to receive error alerts |

---

## 📋 Bot Commands

### User Commands
| Command | Description |
|---------|-------------|
| `/start` | Start the bot (also handles deep links) |
| `/join` | Browse and subscribe to paid channels |
| `/my_subscriptions` | View your active subscriptions |
| `/verify` | Standalone payment verification |
| `/help` | Show help message |
| `/cancel` | Cancel current operation |

### Admin Commands
| Command | Description |
|---------|-------------|
| `/setup_channel` | Register a new paid channel |
| `/my_channels` | View and manage your channels |
| `/members` | List active members of a channel |
| `/stats` | Channel revenue and member statistics |
| `/transactions` | View recent transaction history |
| `/export` | Export member list as CSV |
| `/broadcast` | Send a message to all active members |
| `/edit_channel` | Edit channel price, bank, or account details |
| `/regenerate_links` | Refresh all channel join links |

---

## 💳 Supported Banks

| Bank | Method |
|------|--------|
| Commercial Bank of Ethiopia (CBE) | Transaction ID + account suffix |
| Telebirr | Reference number |
| Dashen Bank | Reference number |
| Bank of Abyssinia | Reference number + account suffix |
| CBE Birr | Receipt number + phone number |
| Any bank | Screenshot / receipt image (OCR) |

---

## 🏗️ Architecture

```
et_payment_verifier_bot.py   ← Single-file bot (PTB + APScheduler)
├── Database (SQLite/aiosqlite)
│   ├── channels             — Registered paid channels
│   ├── members              — Active subscriptions
│   ├── transactions         — Payment verification log
│   ├── channel_admins       — Admin permissions
│   ├── notifications        — Expiry warning tracking
│   ├── user_sessions        — Conversation state
│   └── rate_limits          — Abuse prevention
├── VerificationService      — httpx async API client
├── Scheduled Jobs
│   ├── job_expiry_warnings  — Warn members before expiry
│   ├── job_kick_expired     — Remove expired members
│   ├── job_daily_cleanup    — Purge stale sessions/records
│   └── job_health_check     — Verify bot admin rights
└── Handlers
    ├── Commands             — /start, /join, /admin, etc.
    ├── CallbackQuery        — Inline button responses
    ├── ChatJoinRequest      — Auto-verify on join request
    └── Message              — State-machine payment flow
```

---

## 🐳 Docker Details

The Docker setup uses:
- **Non-root user** (`botuser`) for security
- **Volume mount** at `./data:/app/data` for database persistence
- **Memory limit** of 256MB
- **Restart policy** of `unless-stopped`
- **Log rotation** (10MB max, 3 files)

The `.env` file is passed via `env_file` — it is **not** baked into the image.

---

## 🔒 Security Notes

- **Never commit `.env`** — it contains live API keys and bot tokens. The `.gitignore` excludes it.
- **Never commit `bot.db`** — it contains member data. The `.gitignore` excludes it.
- The bot runs as a non-root user inside Docker.
- Rate limiting is applied per user per action type.
- Duplicate transaction IDs are rejected globally.

---

## 📁 Repository Structure

```
.
├── et_payment_verifier_bot.py   ← Main bot
├── requirements.txt             ← Python dependencies
├── Dockerfile                   ← Container definition
├── docker-compose.yml           ← Deployment config
├── .env.example                 ← Config template (safe to commit)
├── .gitignore                   ← Excludes .env, *.db, etc.
├── .dockerignore                ← Excludes secrets from image build
└── README.md                    ← This file
```

---

## 🛠️ Troubleshooting

| Problem | Solution |
|---------|----------|
| Bot not responding | Check `BOT_TOKEN` in `.env` |
| Payments not verifying | Check `VERIFIER_API_KEY` and `VERIFIER_API_BASE` |
| Can't approve join requests | Make bot an **Administrator** in the channel with "Add Members" permission |
| Database errors | Check write permissions on `data/` directory |
| `RuntimeError: Application still running` | Ensure you're on the latest version (this was fixed) |
| Memory issues | Increase Docker memory limit or reduce `MAX_CHANNELS_PER_ADMIN` |

---

## License

Copyright © 2026. All rights reserved.

This project is proprietary and closed-source. Unauthorized copying, modification, distribution, or commercial use of this software, via any medium, is strictly prohibited.

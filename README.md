# Gate.io HFT Scalping Bot v7

Async Python multi-symbol scalping bot for Gate.io Perpetual Futures.

## Features
- Monitors 24 trading pairs simultaneously via single WebSocket
- Dynamic leverage based on account balance and asset type
- Dynamic Stop Loss linked to leverage
- Server-side Trailing Stop
- Auto position recovery on restart
- Full CSV trade logging

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/gate-scalping-bot.git
cd gate-scalping-bot
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env and add your Gate.io API keys
```

### 4. Run (Testnet by default)
```bash
python gate_scalping_bot.py
```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| GATE_API_KEY | Gate.io API Key | required |
| GATE_API_SECRET | Gate.io API Secret | required |
| TESTNET_MODE | true = testnet, false = live | true |

## Deploy on Railway
1. Push to GitHub
2. Connect repo on Railway.app
3. Add environment variables in Railway dashboard
4. Deploy

## ⚠️ Risk Warning
This bot uses leveraged futures trading.
Never trade with money you cannot afford to lose.
Always test on Testnet first.

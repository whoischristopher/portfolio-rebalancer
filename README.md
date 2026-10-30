Disclaimer:
This project has been constructed mostly through vibe-coding with no manual code review.  No warranty is implied - use as it is.   

This is an attempt at a simple web application that will help rebalance a equity portfolio against a desired asset allocation target.

Key features:
- Google OAuth authentication
- Private and public security products that are tied to asset classes.  Public security price updates via yFianance.
- Securities can be restricted to specific accounts, a preference model, or unrestricted (any account)
- Multi account types: Registered and non-registered.   Future features will take advantage of the tax-exempt nature of registered accounts.
- Asset Allocation targets
- Rebalance against the targets

How the rebalancer works:
The plan is produced by an optimiser (mixed-integer program, solved with HiGHS via scipy) that
weighs all possible trades at once instead of applying rules one after another.
Logic: services/optimizer.py (pure, unit-tested). Database adapter: rebalancer.py.

It minimises, in order of weight:
- Drift outside the balanced band (Settings) – drift inside the band is nearly free, so it doesn't chase exact targets
- Capital gains realised in non-registered accounts (from each holding's book value / ACB; losses aren't penalised)
- Number of trades (+ the per-trade cost from Settings)
- Number of positions – discourages new positions and closes small scattered positions when cheap
- Buying in lower-priority accounts (Securities page priorities are preferences; restrictions are hard rules)
- Idle cash

Always respected:
- Each account funds its own buys; no account's cash goes negative; FX at spot rate
- Non-registered sells: Never / Ask / Allow (Settings). "Ask" plans without them and, if something is still
  outside its band, offers the alternative with the gain it would realise
- Superficial loss rule: no loss sale + repurchase of the same security in one plan, no buy within 30 days of a
  recorded loss sale, no loss sale within 30 days of a recorded buy (only trades executed in this app are known)
- Optional fractional final buy per account to use up leftover cash, without pushing a class past its band

Book value tracking: enter ACB (total, in base currency) for non-registered holdings on the Holdings page;
executing trades in the app updates it (average cost) and records realised gains.

Not (yet) handled: tax-loss harvesting, asset location beyond security priorities, cross-account transfers,
trades placed outside the app for the 30-day superficial loss window.

Tests: `python -m pytest tests/ -q`

Installation:
- Clone/pull the code
- Implement Google OAuth and obtain your Google_Client ID and Secret
- Create docker-compose.yml (using sample beblow and modify as necessary), and off you go.

sample docker-compose.yml:
````
services:
  web:
    build: .
    container_name: portfolio_rebalancer
    ports:
      - "8080:5000"
    environment:
      - FLASK_ENV=production
      - SECRET_KEY=${SECRET_KEY}
      - GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID}
      - GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET}
      - DATABASE_URL=${DATABASE_URL}
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

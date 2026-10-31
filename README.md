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

Two stages: first the cheapest plan that brings every class inside its band (portfolios already within
their bands get no trades). Then, if any trades are needed, the same number of trades with no extra
realised gain is re-sized to land as close to target as possible – the furthest-off classes first.

Always respected:
- Each account funds its own buys; no account's cash goes negative; FX at spot rate
- Non-registered sells: Never / Ask / Allow (Settings). "Ask" plans without them and, if something is still
  outside its band, offers the alternative with the gain it would realise
- Superficial loss rule: no loss sale + repurchase of the same security in one plan, no buy within 30 days of a
  recorded loss sale, no loss sale within 30 days of a recorded buy (only trades executed in this app are known)
- Illiquid holdings (private funds; flag on the Securities page) are never sold. If they alone exceed their
  class's target, the other targets shrink proportionally; beyond the band, the Rebalance page asks you to
  file a redemption request and suggests how many units, from which accounts (registered first).
  Recorded requests are left out of planning until you mark them received (units, ACB, cash, gain updated)
- Optional fractional final buy per account to use up leftover cash, without pushing a class past its band

Getting data in:
- Quick start (sidebar, and the dashboard checklist until setup is complete) walks through accounts → holdings & cash,
  one account at a time → target mix → a first plan. Each step saves as you go; it can be re-run any time to add more.
  New portfolios open straight into it.
- Holdings are entered by ticker, several rows at once. A ticker already in the portfolio is recognised (also with
  exchange prefixes/suffixes, e.g. XIC.TO → XIC); a new one asks for name, asset class (type a new name to create the
  class), currency and optional price, pre-filled from your other portfolios when the same ticker exists there.
  Entering a ticker you already hold in that account tops up the position (quantities and book values add).
  A batch with a bad row is rejected as a whole, so nothing is half-saved.
- Account types: choosing a registered type (RRSP, Spousal RRSP, TFSA, FHSA, RESP, RDSP, LIRA, LIF, RRIF) marks the
  account registered; Non-Registered and Corporate are always taxable; "Other" leaves the checkbox to you.
  Accounts can now be edited (name, type, currency, tax status) on the Accounts page.

Multiple portfolios (e.g. yours and your spouse's) under one login:
- Pick the active portfolio from the selector at the top of every page; manage them under "Manage portfolios…".
- Each portfolio is fully separate: its own accounts, securities, asset classes, targets, holdings/book values,
  rebalancing settings (Settings page, top card) and plan. A page or form can only touch the active portfolio's rows.
- Only prices are shared: the price sheet (a login setting) fills every portfolio in one pass, and a price typed by
  hand is copied to the same ticker in your other portfolios.
- New portfolios can start with a one-time copy of the current portfolio's asset classes, securities and targets.
- Superficial loss across portfolios is opt-in (Settings → "Superficial-Loss Check Across Portfolios", default off):
  link your spouse's portfolio so a loss sale in one isn't paired with a buy of the same ticker in the other.
- Upgrading: on first start, existing data moves into a portfolio called "My Portfolio" with your current settings
  (back up `data/portfolio.db` first; the securities and asset-class tables are rebuilt to make tickers unique
  per portfolio instead of globally).

Book value tracking (taxable accounts only): enter ACB (total, in the security's currency) for non-registered
holdings; executing trades in the app updates it (average cost) and records realised gains. ACB isn't stored for
registered accounts: existing values are cleared on startup, and changing an account to registered (or moving a
holding into one) clears it. Changing an account to taxable flags its holdings' book value as missing.

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

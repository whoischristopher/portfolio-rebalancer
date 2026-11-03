# Portfolio Rebalancer

A self-hosted web app that turns a target asset allocation into a short list of trades across all of your
accounts – RRSP, TFSA, LIRA, taxable and so on – while keeping realised capital gains, the number of trades and
superficial losses in check. Built for Canadian investors (CAD base currency, USD-listed securities supported).

> **Disclaimer:** this project was built mostly through AI-assisted ("vibe") coding without a line-by-line manual
> code review. No warranty is implied – check every trade before you place it, and keep backups of your data.

## Contents
- [Features at a glance](#features-at-a-glance)
- [Getting started](#getting-started)
- [Accounts and tax status](#accounts-and-tax-status)
- [Securities](#securities)
- [Prices and exchange rates](#prices-and-exchange-rates)
- [How the rebalancer works](#how-the-rebalancer-works)
- [Recording executed trades](#recording-executed-trades)
- [Multiple portfolios](#multiple-portfolios)
- [Not (yet) handled](#not-yet-handled)
- [Installation](#installation)
- [Upgrading](#upgrading)
- [Development](#development)
- [Dependencies](#dependencies)

## Features at a glance
- Sign in with Google; several fully separate portfolios per login (e.g. yours and your spouse's)
- Quick-start wizard: accounts → holdings and cash → target mix → first plan
- Holdings entered by ticker; new securities and asset classes are created on the spot
- Registered vs taxable accounts, with book value (ACB) tracked only where it matters for tax
- Prices from your own Google Sheet (`GOOGLEFINANCE`), refreshed automatically when stale
- An optimiser (mixed-integer program) that plans all trades at once: drift band, realised gains, trade count,
  account preferences, superficial-loss rule, illiquid and provider-managed holdings
- Dashboard showing each asset class's drift from target against the balanced band
- Amounts shown as `$` in the portfolio's base currency and `US$` (or `C$`) for the other currency

## Getting started
1. **Sign in** and open **Quick start** (sidebar). The dashboard shows a setup checklist until everything is in place;
   new portfolios open straight into the wizard. Each step saves as you go and can be re-run any time.
2. **Accounts** – add several at once (name, type, currency).
3. **Holdings and cash** – one account at a time. Type a ticker: one already in the portfolio is recognised (also with
   exchange prefixes/suffixes, e.g. `XIC.TO` → `XIC`); a new one asks for a name, asset class (type a new name to create
   the class), currency and optional price – pre-filled from your other portfolios when the same ticker exists there.
   Entering a ticker you already hold in that account tops up the position. A batch with a bad row is rejected as a
   whole, so nothing is half-saved.
4. **Targets** – the share of the portfolio for each asset class, shown next to your current mix. Must total 100%.
5. **Review** – generate the first plan.

Afterwards, the Holdings page offers the same ticker-first entry per account ("Add holdings"), and Accounts,
Securities and Targets can be edited on their own pages.

## Accounts and tax status
- **Registered (tax-sheltered):** RRSP, Spousal RRSP, TFSA, FHSA, RESP, RDSP, LIRA, LIF, RRIF. Choosing one of these
  types marks the account registered. Non-Registered and Corporate are always taxable; "Other" leaves it to you.
- The rebalancer only looks at registered vs taxable – all registered types behave the same. (A spousal RRSP belongs
  in the annuitant's portfolio if you run separate ones.)
- **Book value (ACB)** is tracked for taxable accounts only: enter the total cost, in the security's currency;
  executing trades in the app updates it (average cost) and records realised gains. It isn't stored for registered
  accounts – values there are cleared, and making an account registered (or moving a holding into one) clears it.
  Making an account taxable flags its holdings' book value as missing until you enter it; a missing book value is
  treated as all gain, the conservative choice.
- Each book value carries an "as of" date. For provider-managed holdings (below) the Holdings page – and the
  Rebalance page when taxable sells are in play – flags one that is missing, undated or more than 90 days old.

## Securities
- One price per security, used by every holding of it. Either filled from your price sheet (auto-update on) or typed
  by hand (e.g. private funds – turn auto-update off).
- **Account restrictions and priorities** (Securities page): *Restricted* – only the listed accounts may hold it
  (a hard rule); *Prioritized* – prefer some accounts over others (a preference). Use these for asset location, e.g.
  keep US-listed ETFs in the RRSP for the withholding-tax treaty exemption.
- **Illiquid** (private funds and similar): bought but never sold. If they alone exceed their class's target, the
  other targets shrink proportionally; beyond the band, the Rebalance page suggests a redemption request (how many
  units, from which accounts – registered first). Recorded requests are left out of planning until you mark them
  received, which updates units, book value, cash and realised gain in one step.
- **Provider-managed** (e.g. a direct-indexing account held as one security at $1/unit): traded like any other
  taxable position, but the superficial-loss checks are skipped for it – the provider's underlying trades are the
  real dispositions – and its book value needs regular updates from your statements (see above).

## Prices and exchange rates
- **Price sheet:** a Google Sheet with tickers in column A (e.g. `TSE:XEC`, `NYSEARCA:ITOT`; the exchange prefix is
  ignored when matching) and `=GOOGLEFINANCE(A2,"price")` in column B. Put its ID in Settings. One sheet prices every
  portfolio of your login – list each ticker once. (Google Sheets replaced yfinance, which kept being rate-limited.)
- **Names from the sheet (optional):** put `=IFERROR(GOOGLEFINANCE(A2,"name"),"")` in column C and each read fills
  in the name of any security that doesn't have one (names you typed are never overwritten). A new ticker that's
  already in the sheet gets its name pre-filled when you enter it. `"name"` isn't in Google's documented attribute
  list, so treat it as best-effort: it works for listed stocks and ETFs, and may come back blank for others.
- **Keeping the sheet in step:** the app only *reads* the sheet, so it can't add rows itself. After each read it
  remembers the sheet's tickers, and Dashboard, Holdings, Rebalance, Securities and Settings show a banner when an
  auto-priced security has no row (with a **Copy rows for the sheet** button – paste into the first empty cell of
  column A to fill columns A–C; the exchange prefix is a guess, `TSE:` for CAD and none for USD) or when its row gives no price (usually
  `#N/A` from a wrong prefix). Rows no security uses are listed on Settings.
- **Freshness:** Dashboard, Holdings and Rebalance show when prices were last read, with an **Update prices** button.
  When they're older than the refresh interval (Settings, default 15 minutes; 0 = off) those pages update them in the
  background and reload; generating a plan updates them first. A failed read keeps the old prices and shows a small
  "couldn't update" note (hover for the reason). GOOGLEFINANCE quotes can lag by up to 20 minutes.
- Each plan records when it was made and which prices it used; the Rebalance page warns when prices are newer than
  the plan, so share counts may be off.
- **Exchange rates** (CAD/USD, spot – no FX spread) come from exchangerate-api.com and are cached for 4 hours; if the
  site is unreachable the last cached rate is used.

## How the rebalancer works
The plan comes from an optimiser (mixed-integer program, solved with HiGHS via SciPy) that weighs all possible trades
at once instead of applying rules one after another. Logic: `services/optimizer.py` (pure, unit-tested);
database adapter: `rebalancer.py`.

It minimises, in order of weight:
- Drift outside the balanced band (Settings) – drift inside the band is nearly free, so it doesn't chase exact targets
- Capital gains realised in taxable accounts (from book value; losses aren't penalised)
- Number of trades (plus the per-trade cost from Settings)
- Number of positions – discourages new positions and closes small scattered ones when cheap
- Buying in lower-priority accounts
- Idle cash

Two stages: first the cheapest plan that brings every class inside its band (a portfolio already within its bands
gets no trades). Then, if trades are needed anyway, the same number of trades with no extra realised gain is re-sized
to land as close to target as possible – furthest-off classes first.

Always respected:
- Each account funds its own buys; no account's cash goes negative; FX at the spot rate (you're told when an account
  needs to convert currency before buying)
- Taxable sells: **Never / Ask / Allow** (Settings). *Ask* plans without them and, if something is still outside its
  band, offers the alternative along with the gain it would realise
- Superficial-loss rule: no loss sale and repurchase of the same security in one plan, no buy within 30 days of a
  recorded loss sale, no loss sale within 30 days of a recorded buy. Only trades executed in this app are known.
  Optionally also checked against a linked portfolio (Settings, default off)
- Optional fractional final buy per account to use up leftover cash, without pushing a class past its band

## Recording executed trades
Place the plan's trades at your brokerage, then click **Executed…** on each one and enter what was actually filled.
The fields come pre-filled with the plan's numbers, so a trade that went exactly as planned is one click.
- **Fill price and quantity:** these, not the plan's numbers, update holdings, cash and book value. The plan's
  numbers are kept on the trade for reference. A price more than 5% from the planned one asks you to confirm.
- **Commission** (taxable accounts, or any account when Settings has a per-trade cost): in the security's currency,
  pre-filled from the per-trade cost. It's added to book value on a buy and taken off the proceeds on a sale.
- **Trade date** (every account, default today): the 30-day superficial-loss window counts from this date. It matters
  in registered accounts too: buying in an RRSP or TFSA within 30 days of a taxable loss sale makes the loss superficial.
- **Exchange rate** (taxable trades in a security priced in another currency than the base): pre-filled with the Bank
  of Canada daily rate for the trade date (the last one published before it on weekends and holidays), which the CRA
  accepts. You can enter your own instead. It's stored on the trade and used for the realised gain; book value stays
  in the security's currency. If no rate can be found, the spot rate is used and you're told.
- Cash still moves at the spot rate, since the brokerage's own conversion isn't known here.
- After each trade, the rest of the plan is re-planned from the actual cash and holdings, so the last buy in each
  account is re-sized for you. If a buy cost slightly more than the cash recorded here (up to $1 or 0.5%), the cash is
  set to 0; a bigger gap means the cash balance is wrong, so you're asked to fix it on the Holdings page first.

## Multiple portfolios
- Switch the active portfolio from the selector at the top of the sidebar; manage them on the Portfolios page.
- Each portfolio is fully separate: accounts, securities, asset classes, targets, holdings and book values,
  rebalancing settings and plan. A page or form can only touch the active portfolio's data.
- Shared between them: the price sheet and refresh interval (login settings), and a price typed by hand is copied to
  the same ticker in your other portfolios.
- A new portfolio can start with a one-time copy of the current one's asset classes, securities and targets.
- Superficial loss across portfolios is opt-in: link your spouse's portfolio so a loss sale in one isn't paired with a
  buy of the same ticker in the other.

## Not (yet) handled
Tax-loss harvesting, asset location beyond security restrictions/priorities, cross-account transfers, and trades
placed outside the app (they aren't seen by the 30-day superficial-loss window).

## Installation
Runs as a single Docker container with a SQLite database in `./data`.

1. **Google Cloud project** (console.cloud.google.com):
   - Enable the **Google Sheets API**.
   - Configure the OAuth consent screen (External; add yourself as a test user) with the scopes `openid`, `email`,
     `profile` and `https://www.googleapis.com/auth/spreadsheets.readonly`.
   - Create an **OAuth client ID** (Web application) with the authorised redirect URI
     `https://<your-host>/auth/callback` (`http://localhost:8080/auth/callback` for local testing).
2. **Environment** – create a `.env` next to `docker-compose.yml`:
   ```
   SECRET_KEY=<long random string, e.g. from `openssl rand -hex 32`>
   GOOGLE_CLIENT_ID=<from step 1>
   GOOGLE_CLIENT_SECRET=<from step 1>
   ```
   `FLASK_ENV=production` makes the callback URL and session cookie HTTPS-only, so put the app behind a reverse proxy
   with TLS (or leave it unset for plain-HTTP testing).
3. **docker-compose.yml**:
   ```yaml
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
         - DATABASE_URL=sqlite:////app/data/portfolio.db   # 4 slashes = absolute path
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
   ```
   (The `docker-compose.yml` in the repo also mounts the source tree into the container – handy for development,
   not needed to run it.)
4. `docker compose up -d --build`, sign in, then set your price sheet ID in **Settings** and open **Quick start**.

## Upgrading
What changed in each release: [CHANGELOG.md](CHANGELOG.md).

Pull, rebuild and restart (`docker compose up -d --build` – the code is mounted into the container, but new
library versions only arrive with a rebuild). Schema and data changes run automatically on startup (`migrations.py`, no Alembic) and are
safe to run repeatedly. **Back up `data/portfolio.db` first** – some steps can't be undone:
- Pre-portfolio databases move into a portfolio called "My Portfolio" with your old settings; the securities and
  asset-class tables are rebuilt so tickers are unique per portfolio instead of globally.
- Book values stored for registered accounts are cleared.
- Unused columns are dropped (old per-user settings once copied to the first portfolio, `accounts.priority/notes`,
  `securities.is_public`). Needs SQLite 3.35+ (the Docker image has it); otherwise they're left in place, harmlessly.

## Development
```
pip install -r requirements.txt
SECRET_KEY=dev GOOGLE_CLIENT_ID=… GOOGLE_CLIENT_SECRET=… python app.py      # http://localhost:5000
python -m pytest tests/ -q
```
Layout: `app.py` (routes), `models.py`, `migrations.py`, `rebalancer.py` (database ↔ optimiser), `services/`
(`optimizer.py`, `prices.py`, `fx.py`, `portfolio.py`, `entry.py` for data entry, `money.py` for amount formatting),
`templates/`, `static/` (`app.css`, `app.js`), `tests/`.

## Dependencies
- `requirements.in` lists the direct dependencies with version ranges. `requirements.txt` is the **lock file**: every
  package, including dependencies of dependencies, pinned to an exact version with hashes, so each build installs
  exactly what was tested and pip refuses a tampered download.
- **Changing a dependency:** edit `requirements.in`, then regenerate the lock in the same Python as the image:
  ```
  docker run --rm -v "$PWD":/w -w /w -u "$(id -u):$(id -g)" -e HOME=/tmp python:3.13-slim sh scripts/lock-deps.sh
  ```
  Add `--upgrade` at the end to move every pin to the newest version the ranges allow.
- **Dependabot** opens update PRs against `develop` every Saturday (Python packages grouped into one PR for
  minor/patch, major versions separately; the base image; GitHub Actions monthly).
- **CI** (`.github/workflows/ci.yml`) runs on every PR and push to `main`/`develop`: the lock file must match
  `requirements.in`, the tests run inside the built Docker image, and `pip-audit` fails on any known vulnerability.
  The audit also runs every Monday, since new CVEs get published for versions that haven't changed.
- Merge an update PR when CI is green, then on the server: `git pull && docker compose up -d --build`.

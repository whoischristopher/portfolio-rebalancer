# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Phase-out by account preferences.** On Edit security, *Replace with → By account preferences (automatic)*
  replaces the old security, in each account, with the security of its asset class that the account preferences
  favour there (e.g. Canadian bonds: B restricted to the RRSP, C prioritized for the non-registered account → B in
  the RRSP, C in the non-registered account). The page previews the pick per account; the Securities badge lists
  them. *Exceptions for some accounts* can still set a specific replacement or *Keep* the old security in an account.
  The plan uses each account's replacement for new money and for *switch registered accounts now*, and pauses an
  account – with a note, and a warning when saving – when its replacement has no price or is restricted there, or
  automatic finds nothing allowed. The superficial-loss link covers the old security and every replacement in use.
  Existing phase-outs carry on unchanged; a `security_replacements` table and a column are added on startup.
- **Quick start, step 2: "Securities to buy".** Add a security you don't hold yet – e.g. the fund for a new asset
  class – without creating a holding, so the rebalancer has something to buy. Lists the portfolio's unheld
  securities. Blank fields are copied from the same ticker in your other portfolios, then from the price sheet.
- **Step 3 flags targets with nothing to buy:** an asset class with no securities is marked, with a link back.
- **Security names from the price sheet.** An optional column C (`=IFERROR(GOOGLEFINANCE(A2,"name"),"")`) fills in
  names that are blank on each price read and pre-fills the name of a new ticker already in the sheet. Typed names are
  never overwritten. The sheet is still read-only; **Copy rows for the sheet** now includes the column C formula.
- **Phase out a security (gradual migration).** On Edit security, pick a replacement in the same asset class. The
  old security is never bought; when something in its class has to be sold it goes first, unless selling it would
  realise more gain than the alternative (tax stays the priority, so taxable holdings aren't sold just to switch).
  Options: **switch registered accounts now** (sell it outright where there's no tax) and **treat as identical for
  the superficial-loss rule** (on by default – recent trades of either count for both, and a loss sale of one is never
  paired with a purchase of the other). Plans note what was moved and what's left; the Securities page shows a
  "phasing out → X" badge. New columns are added on startup.

## [1.1.0] - 2026-11-02

> **Upgrading:** back up `data/portfolio.db`, then `git pull && docker compose up -d --build`. The image moves to
> Python 3.13 with new library versions, so the rebuild is required; a new column is added automatically on startup.

### Added
- **Record the actual fill** when a trade is executed: fill price, quantity, commission and trade date, pre-filled
  with the plan's numbers. Holdings, cash, book value and realised gain use the actual fill; the planned quantity and
  price are kept on the trade. A fill price more than 5% from plan asks for confirmation.
- **Exchange rate on the trade date** for taxable trades in a foreign-currency security, pre-filled with the Bank of
  Canada daily rate (`/api/fx/boc`), stored on the trade and used to convert its realised gain.
- **Price-sheet gaps banner.** Securities with no row in the price sheet, or whose row gives no price, are listed on
  Dashboard, Holdings, Rebalance, Securities and Settings – including ones added since the last read – with a
  **Copy rows for the sheet** button that copies ready-to-paste `ticker ⇥ =GOOGLEFINANCE(…)` rows. Unused sheet rows
  are listed on Settings. The app still only reads the sheet.
- Small cash shortfalls on a buy (up to $1 or 0.5%, e.g. slippage on the last buy) set the account's cash to 0 instead
  of blocking the trade.

### Changed
- **Dependencies locked and kept current.** `requirements.in` (direct dependencies, version ranges) and a hashed
  lock file in `requirements.txt`, regenerated with `scripts/lock-deps.sh`. Dependabot opens weekly update PRs against
  `develop`, and CI checks the lock, runs the tests inside the Docker image and runs `pip-audit`.
- Library upgrades: Flask 3.1.3, Werkzeug 3.1.9, gunicorn 26.2.0 (request-smuggling fixes), Authlib 1.9.2,
  SQLAlchemy 2.1.6, Requests 2.34.2 (CVE-2024-35195, CVE-2024-47081), NumPy 2.5.4, SciPy 1.18.1,
  google-api-python-client 2.201.0, google-auth 2.62.0.
- Docker image moves to **Python 3.13** (`python:3.13-slim`); current NumPy and SciPy need 3.12 or later.
- A `.dockerignore` keeps `.env`, the database, logs, `.git` and tests out of the image.
- Login lookup uses `db.session.get()` instead of the legacy `Query.get()`.
- CI's GitHub Actions are pinned to full commit hashes (kept current by Dependabot).
- `gcc` is no longer installed in the image (every dependency installs from a prebuilt wheel); `apt-get` uses
  `--no-install-recommends`.
- The superficial-loss window counts from each trade's trade date rather than when it was recorded in the app.
  Existing trades get their recorded date as their trade date (automatic migration).
- With a per-trade cost set in Settings, recording a trade without entering a commission now uses that cost
  (added to book value and taken from cash).
- The background price refresh no longer hides securities the sheet has no price for (the Update prices
  button's warnings are replaced by the banner, which every refresh feeds). New column `users.price_sheet_snapshot`
  (automatic migration).

### Fixed
- A trade could be recorded twice by re-submitting the form.

## [1.0.0] - 2026-11-02

First production release. The rebalancing engine, data model and most of the UI have been rewritten since the
November 2025 beta.

> **Upgrading from the beta:** back up `data/portfolio.db` before starting the new version. Schema and data changes
> run automatically on startup and some cannot be undone – see [Upgrading](README.md#upgrading).

### Added
- **Tax-aware optimiser.** Trades are planned all at once by a mixed-integer program (HiGHS via SciPy) that minimises,
  in order: drift outside the balanced band, realised capital gains in taxable accounts, number of trades (plus the
  configurable per-trade cost), number of positions, buys in lower-priority accounts, and idle cash.
- Two-stage planning: the cheapest plan that brings every asset class inside its band, then – when trades are needed
  anyway – the same trades re-sized to land as close to target as possible, with no extra realised gain.
- Taxable sells setting: **Never / Ask / Allow**. *Ask* plans without them and offers the alternative, with the gain it
  would realise, if a class is still outside its band.
- Superficial-loss protection: no loss sale and repurchase in one plan, and a 30-day window around recorded trades.
  Optional cross-portfolio check against a linked portfolio (off by default).
- **Multiple portfolios per login**, each fully separate (accounts, securities, asset classes, targets, holdings,
  book values, settings and plan), with a sidebar switcher and an optional one-time copy when creating a portfolio.
- **Quick-start wizard**: accounts → holdings and cash → targets → first plan, plus a setup checklist on the dashboard.
- Ticker-first holding entry: existing securities are recognised (including exchange prefixes/suffixes); new
  securities and asset classes are created on the spot.
- **Illiquid holdings** (e.g. private funds): never sold; other targets are re-normalised around them; redemption
  requests can be suggested, recorded and marked received (updating units, book value, cash and realised gain).
- **Provider-managed holdings** (e.g. a direct-indexing account held as one security): dated book value with
  staleness warnings, and no superficial-loss check.
- Book value (ACB) tracking in taxable accounts, in the security's currency, with average-cost updates and realised
  gain recorded when trades are executed in the app.
- Account types (RRSP, Spousal RRSP, TFSA, FHSA, RESP, RDSP, LIRA, LIF, RRIF, Non-Registered, Corporate, Other) with
  the registered flag set automatically.
- Price freshness: "as of" time on Dashboard, Holdings and Rebalance; background refresh when prices are older than
  the refresh interval (Settings, default 15 minutes); prices refreshed before every plan; warning when prices are
  newer than the current plan.
- Optional fractional final buy per account to use up leftover cash without pushing a class past its band.
- Notice when an account must convert currency before it can buy.
- Clearer price-update errors that distinguish the three Google 403 causes (scope not granted, Sheets API disabled,
  no access to the sheet).
- Automatic, idempotent startup migrations (`migrations.py`) for existing databases.
- Test suite for the optimiser and the main app flows (`tests/`).

### Changed
- Replaced the rule-based (heuristic) rebalancer, including its cross-account swap and targeted-sell phases, with
  the optimiser above.
- Trades aim at the target allocation, not just the edge of the band; drift inside the band is nearly free, so the
  plan doesn't chase exact targets. A partial plan is never shown.
- One price per security, shared by all its holdings; new holdings no longer default to a price of 0.
- Book value is stored in the security's currency.
- Book value is no longer tracked for registered accounts; making an account registered clears it, and making one
  taxable flags its holdings' book value as missing (treated as all gain until entered).
- Securities and asset classes are now scoped to a portfolio (tickers unique per portfolio, not globally).
- Dashboard shows each asset class's drift from target against the balanced band.
- Money is shown as `$` in the portfolio's base currency and `US$` / `C$` for the other currency; Holdings value
  columns merged.
- Prices come from the user's Google Sheet (`GOOGLEFINANCE`) instead of yfinance, which was frequently rate-limited.
- FX conversions use the spot CAD/USD rate (no spread).
- Code reorganised into service modules (`services/`); README rewritten.

### Fixed
- Logout: the remember-me cookie survived and silently signed the user back in.
- Error 500 on `/rebalance` when a planned trade referenced a deleted account or security.
- Google OAuth sign-in.
- Price update failures.
- Several issues in the former heuristic engine (US equity receiving no buys, targeted sells over-selling).

### Removed
- Unused database columns (`accounts.priority`, `accounts.notes`, `securities.is_public`, and per-user settings now
  held on the portfolio), dead code, and unused dependencies.
- The unused `/rebalance/details` route.

## [Beta] - 2025-11-02

Initial public pre-release (tag `v1-develop`): heuristic rebalancer, Google sign-in, accounts, holdings and
target allocation.

[Unreleased]: https://github.com/whoischristopher/portfolio-rebalancer/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/whoischristopher/portfolio-rebalancer/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/whoischristopher/portfolio-rebalancer/compare/v1-develop...v1.0.0
[Beta]: https://github.com/whoischristopher/portfolio-rebalancer/releases/tag/v1-develop

# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/whoischristopher/portfolio-rebalancer/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/whoischristopher/portfolio-rebalancer/compare/v1-develop...v1.0.0
[Beta]: https://github.com/whoischristopher/portfolio-rebalancer/releases/tag/v1-develop

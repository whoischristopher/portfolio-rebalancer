"""
migrations.py – In-place schema/data upgrades run at startup (no Alembic).

db.create_all() creates missing tables but never alters existing ones, so anything
added to an existing table is handled here. Every step is idempotent: it inspects the
database and only does what is still missing, so it is safe on every start.

Multiple portfolios (introduced together with the Portfolio model):
  1. add portfolio_id to every per-portfolio table
  2. give every user a default portfolio ("My Portfolio") carrying the rebalancing
     settings that used to live on the User row, and assign their rows to it
  3. securities / asset classes used to be global (no owner at all): hand them to the
     oldest portfolio, then clone them into any other portfolio that references them,
     so no portfolio ever points at another portfolio's rows
  4. replace the global UNIQUE(ticker) / UNIQUE(name) constraints with
     UNIQUE(portfolio_id, ticker) / UNIQUE(portfolio_id, name). SQLite can't drop a
     constraint, so those two tables are rebuilt (the documented SQLite procedure:
     create new → copy → drop old → rename).
"""
import logging

from sqlalchemy import inspect, text

from extensions import db

log = logging.getLogger(__name__)

# Columns added after the initial schema (table, column, DDL).
ADDED_COLUMNS = [
    ('holdings', 'book_value', 'FLOAT'),
    ('rebalance_transactions', 'realized_gain', 'FLOAT'),
    ('securities', 'last_price', 'FLOAT'),
    ('securities', 'is_illiquid', 'BOOLEAN DEFAULT FALSE'),
]

# Tables scoped to a portfolio. Those with a user_id column are backfilled from it.
PORTFOLIO_TABLES = ['accounts', 'targets', 'security_preferences', 'rebalance_transactions',
                    'pending_redemptions', 'securities', 'asset_classes']
for _t in PORTFOLIO_TABLES:
    ADDED_COLUMNS.append((_t, 'portfolio_id', 'INTEGER'))

# Old single-column unique constraints → their per-portfolio replacement.
UNIQUE_REPLACEMENTS = {
    'securities': (['ticker'], 'uq_securities_portfolio_ticker', ['portfolio_id', 'ticker']),
    'asset_classes': (['name'], 'uq_asset_classes_portfolio_name', ['portfolio_id', 'name']),
}

DEFAULT_PORTFOLIO_NAME = 'My Portfolio'

# Columns nothing reads any more (table, column). Dropped at the end of run_all, after
# the steps that still need them (the users.* settings seed each user's first portfolio).
DROPPED_COLUMNS = [
    ('users', 'base_currency'), ('users', 'balanced_threshold'), ('users', 'precision_rebalancing'),
    ('users', 'trade_cost'), ('users', 'taxable_sell_mode'), ('users', 'last_plan_summary'),
    ('users', 'trading_costs_enabled'),
    ('accounts', 'priority'), ('accounts', 'notes'),       # never set or read
    ('securities', 'is_public'),                            # had no effect on anything
]
_LEGACY_USER_SETTINGS = ('base_currency', 'balanced_threshold', 'precision_rebalancing', 'trade_cost',
                         'taxable_sell_mode', 'last_plan_summary')


def run_all():
    add_missing_columns()
    create_default_portfolios()
    backfill_portfolio_ids()
    replace_global_uniques()          # before the repair: it may clone tickers/names
    repair_cross_portfolio_refs()
    add_portfolio_indexes()
    clear_registered_book_values()
    drop_dead_columns()                # last: earlier steps may still read these


# --------------------------------------------------------------------------- #
def add_missing_columns():
    insp = inspect(db.engine)
    tables = set(insp.get_table_names())
    with db.engine.begin() as conn:
        for table, column, ddl in ADDED_COLUMNS:
            if table not in tables:
                continue
            if column not in {c['name'] for c in insp.get_columns(table)}:
                conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}'))
                log.info('Migrated: added %s.%s', table, column)


def _legacy_user_settings(user_id):
    """Rebalancing settings from the pre-portfolio users table, if those columns still exist."""
    have = {c['name'] for c in inspect(db.engine).get_columns('users')}
    cols = [c for c in _LEGACY_USER_SETTINGS if c in have]
    if not cols:
        return {}
    row = db.session.execute(text(f"SELECT {', '.join(cols)} FROM users WHERE id = :id"),
                             {'id': user_id}).mappings().first()
    return dict(row) if row else {}


def new_default_portfolio(user, name=DEFAULT_PORTFOLIO_NAME):
    """A user's first portfolio – carrying their old per-user settings when upgrading."""
    from models import Portfolio
    old = _legacy_user_settings(user.id)
    return Portfolio(
        user_id=user.id, name=name,
        base_currency=old.get('base_currency') or 'CAD',
        balanced_threshold=old['balanced_threshold'] if old.get('balanced_threshold') is not None else 0.5,
        precision_rebalancing=True if old.get('precision_rebalancing') is None else bool(old['precision_rebalancing']),
        trade_cost=old.get('trade_cost') or 0.0,
        taxable_sell_mode=old.get('taxable_sell_mode') or 'ask',
        last_plan_summary=old.get('last_plan_summary'),
    )


def create_default_portfolios():
    from models import Portfolio, User
    have = {pid for (pid,) in db.session.query(Portfolio.user_id).distinct()}
    created = 0
    for user in User.query.order_by(User.id).all():
        if user.id not in have:
            db.session.add(new_default_portfolio(user))
            created += 1
    if created:
        db.session.commit()
        log.info('Migrated: created a default portfolio for %d user(s)', created)


def backfill_portfolio_ids():
    """Rows still without a portfolio go to their user's first portfolio; ownerless
    (formerly global) securities and asset classes go to the oldest portfolio."""
    insp = inspect(db.engine)
    with db.engine.begin() as conn:
        first = conn.execute(text('SELECT MIN(id) FROM portfolios')).scalar()
        if first is None:
            return                                   # empty database – nothing to assign
        for table in PORTFOLIO_TABLES:
            cols = {c['name'] for c in insp.get_columns(table)}
            if 'user_id' in cols:
                res = conn.execute(text(
                    f'UPDATE {table} SET portfolio_id = (SELECT MIN(p.id) FROM portfolios p '
                    f'WHERE p.user_id = {table}.user_id) WHERE portfolio_id IS NULL'))
            else:
                res = conn.execute(text(f'UPDATE {table} SET portfolio_id = :p WHERE portfolio_id IS NULL'),
                                   {'p': first})
            if res.rowcount:
                log.info('Migrated: assigned %d %s row(s) to a portfolio', res.rowcount, table)


def repair_cross_portfolio_refs():
    """Make every reference stay inside its portfolio, cloning securities / asset classes
    where needed. Only does anything for databases that had several users while
    securities were global; a no-op otherwise."""
    from models import (Account, AssetClass, Holding, PendingRedemption, RebalanceTransaction,
                        Security, SecurityPreference, Target)
    ac_cache, sec_cache = {}, {}

    def ac_in(pid, ac):
        if ac is None or ac.portfolio_id == pid:
            return ac
        key = (pid, ac.name)
        if key not in ac_cache:
            ac_cache[key] = (AssetClass.query.filter_by(portfolio_id=pid, name=ac.name).first()
                             or AssetClass(portfolio_id=pid, name=ac.name))
            db.session.add(ac_cache[key]); db.session.flush()
        return ac_cache[key]

    def sec_in(pid, sec):
        if sec is None or sec.portfolio_id == pid:
            return sec
        key = (pid, sec.ticker)
        if key not in sec_cache:
            clone = Security.query.filter_by(portfolio_id=pid, ticker=sec.ticker).first()
            if clone is None:
                clone = Security(portfolio_id=pid, ticker=sec.ticker, name=sec.name,
                                 asset_class=ac_in(pid, sec.asset_class), currency=sec.currency,
                                 auto_update_price=sec.auto_update_price,
                                 is_illiquid=sec.is_illiquid, last_price=sec.last_price)
                db.session.add(clone); db.session.flush()
            sec_cache[key] = clone
        return sec_cache[key]

    n = 0
    for h in Holding.query.join(Account).all():
        s = sec_in(h.account.portfolio_id, h.security)
        if s is not h.security:
            h.security = s; n += 1
    for model in (SecurityPreference, RebalanceTransaction, PendingRedemption):
        for row in model.query.all():
            if row.security is not None:
                s = sec_in(row.portfolio_id, row.security)
                if s is not row.security:
                    row.security = s; n += 1
    for t in Target.query.all():
        a = ac_in(t.portfolio_id, t.asset_class)
        if a is not t.asset_class:
            t.asset_class = a; n += 1
    for s in Security.query.all():
        a = ac_in(s.portfolio_id, s.asset_class)
        if a is not s.asset_class:
            s.asset_class = a; n += 1
    if n:
        db.session.commit()
        log.info('Migrated: re-pointed %d cross-portfolio reference(s)', n)
    else:
        db.session.rollback()


def replace_global_uniques():
    from models import AssetClass, Security
    models = {'securities': Security, 'asset_classes': AssetClass}
    insp = inspect(db.engine)
    for table, (old_cols, new_name, new_cols) in UNIQUE_REPLACEMENTS.items():
        uniques = insp.get_unique_constraints(table)
        old = [u for u in uniques if u['column_names'] == old_cols]
        if not old:
            continue
        if db.engine.dialect.name == 'sqlite':
            _sqlite_rebuild(models[table].__table__)
        else:
            with db.engine.begin() as conn:
                for u in old:
                    conn.execute(text(f'ALTER TABLE {table} DROP CONSTRAINT {u["name"]}'))
                if not any(u['column_names'] == new_cols for u in uniques):
                    conn.execute(text(f'ALTER TABLE {table} ADD CONSTRAINT {new_name} '
                                      f'UNIQUE ({", ".join(new_cols)})'))
        log.info('Migrated: %s unique(%s) → unique(%s)', table, ', '.join(old_cols), ', '.join(new_cols))


def _sqlite_rebuild(table):
    """Recreate *table* from the current model definition, keeping its rows and ids."""
    import re
    from sqlalchemy.schema import CreateTable
    name, tmp = table.name, f'_{table.name}_new'
    old_cols = {c['name'] for c in inspect(db.engine).get_columns(name)}
    cols = ', '.join(c.name for c in table.columns if c.name in old_cols)
    ddl = str(CreateTable(table).compile(dialect=db.engine.dialect))
    ddl, n = re.subn(rf'^(\s*CREATE TABLE ){re.escape(name)}\b', rf'\g<1>{tmp}', ddl, count=1)
    if n != 1:
        raise RuntimeError(f'Unexpected DDL for {name}: {ddl[:80]}')
    with db.engine.begin() as conn:
        fk_on = conn.execute(text('PRAGMA foreign_keys')).scalar()
        if fk_on:
            raise RuntimeError('SQLite foreign_keys is ON; cannot rebuild tables safely')
        conn.execute(text(ddl))
        conn.execute(text(f'INSERT INTO {tmp} ({cols}) SELECT {cols} FROM {name}'))
        conn.execute(text(f'DROP TABLE {name}'))
        conn.execute(text(f'ALTER TABLE {tmp} RENAME TO {name}'))
        for ix in table.indexes:
            ix.create(conn, checkfirst=True)


def add_portfolio_indexes():
    insp = inspect(db.engine)
    with db.engine.begin() as conn:
        for table in PORTFOLIO_TABLES:
            if {'portfolio_id'} <= {c['name'] for c in insp.get_columns(table)}:
                conn.execute(text(f'CREATE INDEX IF NOT EXISTS ix_{table}_portfolio_id '
                                  f'ON {table} (portfolio_id)'))


def clear_registered_book_values():
    """Book value (ACB) has no tax meaning in a registered account and isn't tracked
    there any more; clear old values so a stale number can't resurface if the account
    is later reclassified as taxable. Idempotent."""
    from models import Account, Holding
    ids = [a for (a,) in db.session.query(Account.id).filter(Account.is_registered.is_(True))]
    if not ids:
        return
    n = (Holding.query.filter(Holding.account_id.in_(ids), Holding.book_value.isnot(None))
         .update({'book_value': None}, synchronize_session=False))
    if n:
        db.session.commit()
        log.info('Migrated: cleared book value on %d holding(s) in registered accounts', n)


def drop_dead_columns():
    """Remove columns the app no longer uses (see DROPPED_COLUMNS). Uses ALTER TABLE …
    DROP COLUMN (SQLite 3.35+, PostgreSQL). If the database can't, the column is simply
    left in place – harmless, since nothing reads it."""
    # Earlier steps may have left the ORM session in an open read transaction; on SQLite
    # that lock blocks ALTER TABLE ("database is locked"). Close it first.
    db.session.commit()
    db.session.remove()
    insp = inspect(db.engine)
    tables = set(insp.get_table_names())
    for table, column in DROPPED_COLUMNS:
        if table not in tables or column not in {c['name'] for c in insp.get_columns(table)}:
            continue
        try:
            with db.engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE {table} DROP COLUMN {column}'))
            log.info('Migrated: dropped unused column %s.%s', table, column)
        except Exception as exc:                       # pragma: no cover – old SQLite
            log.warning('Could not drop unused column %s.%s (%s); leaving it in place', table, column, exc)

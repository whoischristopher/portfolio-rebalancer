/* Portfolio Rebalancer – small progressive enhancements. Every form still posts
   normally without this file; it only makes data entry quicker. */
(function () {
  'use strict';
  const PR = window.PR || {};
  const REGISTERED = new Set(PR.registeredTypes || []);
  const NON_REGISTERED = new Set(PR.nonRegisteredTypes || []);

  /* ---------- Account type → "registered" checkbox ---------- */
  // Inside any [data-account-scope]: a select[data-account-type] drives
  // input[type=checkbox][data-registered] (and a hidden [data-registered-value], used
  // by multi-row forms where unticked checkboxes would knock the lists out of step).
  function syncRegistered(scope) {
    const sel = scope.querySelector('[data-account-type]');
    const box = scope.querySelector('[data-registered]');
    if (!sel || !box) return;
    const t = sel.value;
    const label = box.closest('label');
    if (REGISTERED.has(t)) { box.checked = true; box.disabled = true; }
    else if (NON_REGISTERED.has(t)) { box.checked = false; box.disabled = true; }
    else { box.disabled = false; }
    if (label) {
      label.classList.toggle('locked', box.disabled);
      label.title = box.disabled ? (box.checked ? t + ' accounts are always registered' : t + ' accounts are never registered') : '';
    }
    const hidden = scope.querySelector('[data-registered-value]');
    if (hidden) hidden.value = box.checked ? 'on' : '';
  }
  document.addEventListener('change', (e) => {
    const scope = e.target.closest('[data-account-scope]');
    if (scope && (e.target.matches('[data-account-type]') || e.target.matches('[data-registered]'))) syncRegistered(scope);
  });

  /* ---------- Repeatable rows ---------- */
  function addRow(rows, focus) {
    const tpl = rows.querySelector('template');
    const body = rows.querySelector('.rows-body');
    const node = tpl.content.firstElementChild.cloneNode(true);
    body.appendChild(node);
    if (node.matches('[data-account-scope]')) syncRegistered(node);
    if (focus) { const first = node.querySelector('input, select'); if (first) first.focus(); }
    return node;
  }
  function lastRow(rows) { const b = rows.querySelector('.rows-body'); return b.lastElementChild; }
  function rowIsBlank(row) {
    return [...row.querySelectorAll('.entry-main input[type=text], .entry-main input:not([type])')].every((i) => !i.value.trim());
  }

  document.addEventListener('click', (e) => {
    const add = e.target.closest('[data-add-row]');
    if (add) { e.preventDefault(); addRow(document.querySelector(add.dataset.addRow), true); return; }
    const rm = e.target.closest('.remove-row');
    if (rm) {
      e.preventDefault();
      const rows = rm.closest('[data-rows]');
      rm.closest('.entry-row').remove();
      if (!rows.querySelector('.rows-body').children.length) addRow(rows, true);
    }
  });

  // Typing in the last row adds a fresh blank one below, so entry never stalls on a button.
  document.addEventListener('input', (e) => {
    const rows = e.target.closest('[data-rows]');
    if (!rows || rows.dataset.autogrow === 'off') return;
    const row = e.target.closest('.entry-row');
    if (row && row === lastRow(rows) && !rowIsBlank(row)) addRow(rows, false);
  });

  // Enter moves to the next row instead of submitting half-entered data.
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' || e.shiftKey || !e.target.matches('.entry-row input')) return;
    e.preventDefault();
    const row = e.target.closest('.entry-row');
    const rows = row.closest('[data-rows]');
    let next = row.nextElementSibling || addRow(rows, false);
    const first = next.querySelector('input:not([type=hidden]), select');
    if (first) first.focus();
  });

  /* ---------- Holding rows: ticker → existing security or new one ---------- */
  const KNOWN = PR.known || {};
  const fmt = (n) => (n == null ? '' : Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 }));
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const lookupCache = new Map();

  function setDetails(row, mode) {
    // mode: 'none' | 'new' | 'price'
    const d = row.querySelector('.entry-details');
    if (!d) return;
    d.classList.toggle('hidden', mode === 'none');
    d.classList.toggle('priced-only', mode === 'price');
    d.querySelectorAll('[data-new-only]').forEach((el) => el.classList.toggle('hidden', mode !== 'new'));
  }

  function describeKnown(row, sec) {
    const status = row.querySelector('.sec-status');
    status.classList.remove('new');
    const priced = sec.price ? `${esc(sec.currency)} ${fmt(sec.price)}` : '<span class="negative">no price yet</span>';
    status.innerHTML = `<b>${esc(sec.name || sec.ticker)}</b> <span class="faint">${esc(sec.asset_class)}, ${priced}</span>`;
    setDetails(row, sec.price ? 'none' : 'price');
  }

  function prefill(row, info) {
    const set = (sel, v) => { const el = row.querySelector(sel); if (el && !el.value && v) el.value = v; };
    set('.f-name', info.name);
    set('.f-ac', info.asset_class);
    if (info.currency) { const c = row.querySelector('.f-ccy'); if (c && !c.dataset.touched) c.value = info.currency; }
    if (info.price) set('.f-price', info.price);
  }

  async function checkTicker(row) {
    const input = row.querySelector('.f-ticker');
    const status = row.querySelector('.sec-status');
    const raw = input.value.trim();
    if (!raw) { status.textContent = ''; setDetails(row, 'none'); return; }
    const known = KNOWN[raw.toUpperCase()];
    if (known) { describeKnown(row, known); return; }
    status.classList.add('new');
    status.textContent = 'New security – fill in the details below';
    setDetails(row, 'new');
    const rows = row.closest('[data-rows]');
    const key = raw.toUpperCase();
    let info = lookupCache.get(key);
    if (!info) {
      try {
        const url = `${PR.lookupUrl}?ticker=${encodeURIComponent(raw)}&currency=${encodeURIComponent(rows.dataset.currency || '')}`;
        info = await (await fetch(url, { credentials: 'same-origin' })).json();
        lookupCache.set(key, info);
      } catch (_) { return; }
    }
    if (input.value.trim().toUpperCase() !== key) return;   // user kept typing
    const note = row.querySelector('.entry-details .note');
    if (info.found === 'here') { describeKnown(row, info); return; }
    if (info.found === 'elsewhere') {
      prefill(row, info);
      if (note) note.textContent = `Details copied from ${info.ticker} in your other portfolio – check them.`;
    } else {
      prefill(row, { currency: info.currency });
      if (note) note.textContent = 'Asset class is required; a new name creates that class. Price is optional if your price sheet has this ticker.';
    }
  }

  let timer;
  document.addEventListener('input', (e) => {
    if (e.target.matches('.f-ticker')) {
      const row = e.target.closest('.entry-row');
      clearTimeout(timer);
      timer = setTimeout(() => checkTicker(row), 250);
    }
    if (e.target.matches('.f-ccy')) e.target.dataset.touched = '1';
  });
  document.addEventListener('change', (e) => {
    if (e.target.matches('.f-ticker')) checkTicker(e.target.closest('.entry-row'));
  });

  /* ---------- Toggle panels ---------- */
  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-toggle]');
    if (!t) return;
    e.preventDefault();
    const panel = document.querySelector(t.dataset.toggle);
    if (!panel) return;
    panel.classList.toggle('hidden');
    if (!panel.classList.contains('hidden')) {
      const first = panel.querySelector('input:not([type=hidden]), select');
      if (first) first.focus();
    }
  });

  /* ---------- Live total for target inputs ---------- */
  function updateTargetTotal(form) {
    const out = form.querySelector('[data-target-total]');
    if (!out) return;
    let sum = 0;
    form.querySelectorAll('[data-target-input]').forEach((i) => { const v = parseFloat(i.value); if (!isNaN(v)) sum += v; });
    out.textContent = (Math.round(sum * 100) / 100) + '%';
    out.classList.toggle('good', Math.abs(sum - 100) < 0.01);
    out.classList.toggle('bad', Math.abs(sum - 100) >= 0.01);
  }
  document.addEventListener('input', (e) => {
    if (e.target.matches('[data-target-input]')) updateTargetTotal(e.target.form);
  });

  /* ---------- Book value (ACB) only exists in taxable accounts ---------- */
  document.addEventListener('change', (e) => {
    const sel = e.target.closest('[data-acb-toggle]');
    if (!sel) return;
    const registered = sel.selectedOptions[0] && sel.selectedOptions[0].dataset.registered === '1';
    document.querySelectorAll(sel.dataset.acbToggle)
      .forEach((group) => group.classList.toggle('hidden', registered));
    const none = document.getElementById('bv-none');
    if (none) none.classList.toggle('hidden', !registered);
  });
  document.addEventListener('submit', (e) => {
    const f = e.target;
    if (!f.dataset || !f.dataset.acbCount) return;
    const box = f.querySelector('[data-registered]');
    const n = parseInt(f.dataset.acbCount, 10);
    if (box && box.checked && f.dataset.wasRegistered === '0' && n > 0 &&
        !confirm(`Making this account registered clears the book value on ${n} holding(s). Continue?`)) {
      e.preventDefault();
    }
  });

  /* ---------- Prices: local times + background refresh when stale ---------- */
  function localTimes(root) {
    const today = new Date().toDateString();
    root.querySelectorAll('time[data-local]').forEach((t) => {
      const d = new Date(t.getAttribute('datetime'));
      if (isNaN(d)) return;
      const time = d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
      t.textContent = d.toDateString() === today ? `today ${time}`
        : `${d.toLocaleDateString([], { month: 'short', day: 'numeric' })}, ${time}`;
      t.title = d.toLocaleString();
    });
  }
  async function refreshIfStale() {
    const box = document.querySelector('[data-price-status][data-stale="1"]');
    if (!box) return;
    const note = box.querySelector('[data-price-note]');
    if (note) { note.className = 'small muted'; note.textContent = 'updating…'; }
    try {
      const r = await (await fetch('/api/prices/refresh-if-stale', { method: 'POST', credentials: 'same-origin' })).json();
      if (r.refreshed) { if (note) note.textContent = 'updated – reloading'; window.location.reload(); return; }
      if (note) {
        note.className = 'small negative';
        note.textContent = r.error ? 'couldn\u2019t update' : '';
        note.title = r.error || '';
      }
    } catch (_) {
      if (note) { note.className = 'small negative'; note.textContent = 'couldn\u2019t update'; }
    }
  }

  /* ---------- Copy buttons (e.g. rows for the price sheet) ---------- */
  // navigator.clipboard only exists on https/localhost; a self-hosted app on plain http
  // falls back to selecting the text and execCommand('copy').
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('[data-copy-target]');
    if (!btn) return;
    const src = document.getElementById(btn.dataset.copyTarget);
    if (!src) return;
    const text = src.value !== undefined ? src.value : src.textContent;
    let ok = false;
    try {
      if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); ok = true; }
    } catch (_) { ok = false; }
    if (!ok) {
      const details = src.closest('details');
      if (details) details.open = true;
      src.focus(); src.select();
      try { ok = document.execCommand('copy'); } catch (_) { ok = false; }
    }
    const label = btn.dataset.label || btn.textContent;
    btn.dataset.label = label;
    btn.textContent = ok ? 'Copied \u2713' : 'Selected \u2013 press Ctrl/\u2318+C';
    clearTimeout(btn._copyTimer);
    btn._copyTimer = setTimeout(() => { btn.textContent = label; }, 2500);
  });

  /* ---------- Record an executed trade (rebalance page) ---------- */
  function localISODate() {
    const d = new Date();
    return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
  }
  const numOf = (input) => (input ? parseFloat(input.value) : NaN);
  const fmtAmount = (v, ccy) => v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' ' + ccy;

  function updateExecTotal(form) {
    const out = form.querySelector('[data-exec-total]');
    if (!out) return;
    const q = numOf(form.elements.quantity), p = numOf(form.elements.fill_price);
    const c = numOf(form.elements.commission) || 0;
    if (!(q > 0 && p > 0)) { out.textContent = ''; return; }
    const ccy = form.dataset.ccy;
    const gross = q * p;
    const planned = parseFloat(form.dataset.plannedQty) * parseFloat(form.dataset.plannedPrice);
    const total = form.dataset.action === 'SELL' ? gross - c : gross + c;
    const diff = gross - planned;
    out.textContent = (form.dataset.action === 'SELL' ? 'Net proceeds: ' : 'Total cost: ') + fmtAmount(total, ccy) +
      (Math.abs(diff) >= 0.005 ? ` (${diff > 0 ? '+' : ''}${fmtAmount(diff, ccy)} vs plan)` : ' (as planned)');
  }

  async function lookupBoc(form) {
    const fxInput = form.querySelector('[data-fx-rate]');
    if (!fxInput || fxInput.dataset.touched) return;
    const status = form.querySelector('[data-fx-status]');
    const src = form.elements.fx_rate_source;
    const day = form.elements.trade_date.value;
    if (!day) return;
    if (status) status.textContent = 'Looking up the Bank of Canada rate…';
    try {
      const resp = await fetch(`${fxInput.dataset.bocUrl}?currency=${encodeURIComponent(fxInput.dataset.ccy)}&date=${day}`,
        { headers: { Accept: 'application/json' } });
      const r = await resp.json();
      if (!resp.ok || !r.rate) throw new Error(r.error || 'lookup failed');
      if (fxInput.dataset.touched || form.elements.trade_date.value !== day) return;   // user moved on
      fxInput.value = r.rate;
      if (src) src.value = 'boc';
      if (status) {
        status.textContent = `Bank of Canada rate for ${r.observed_on}` +
          (r.observed_on !== day ? ' (last published before the trade date)' : '') + '. Change it if you prefer your own.';
      }
    } catch (err) {
      if (src) src.value = 'manual';
      if (status) status.textContent = 'Couldn’t get the Bank of Canada rate – showing today’s spot rate. Enter the rate for the trade date if you have it.';
    }
  }

  document.addEventListener('click', (e) => {
    const opener = e.target.closest('[data-open-dialog]');
    if (!opener) return;
    const dlg = document.getElementById(opener.dataset.openDialog);
    if (!dlg) return;
    const form = dlg.querySelector('[data-exec-form]');
    if (form) {
      const d = form.elements.trade_date;
      if (d && !d.dataset.set) { d.value = localISODate(); d.max = d.value; d.dataset.set = '1'; }
      updateExecTotal(form);
      lookupBoc(form);
    }
    dlg.showModal();
  });

  document.addEventListener('input', (e) => {
    const form = e.target.closest('[data-exec-form]');
    if (!form) return;
    if (e.target.matches('[data-fx-rate]')) {
      e.target.dataset.touched = '1';
      if (form.elements.fx_rate_source) form.elements.fx_rate_source.value = 'manual';
      const status = form.querySelector('[data-fx-status]');
      if (status) status.textContent = 'Your own rate.';
    }
    updateExecTotal(form);
  });
  document.addEventListener('change', (e) => {
    const form = e.target.closest('[data-exec-form]');
    if (form && e.target.name === 'trade_date') lookupBoc(form);
  });

  // Catch typos before they end up in the book value: a fill far from the planned price.
  document.addEventListener('submit', (e) => {
    const form = e.target.closest('[data-exec-form]');
    if (!form) return;
    const p = numOf(form.elements.fill_price), planned = parseFloat(form.dataset.plannedPrice);
    const q = numOf(form.elements.quantity), plannedQty = parseFloat(form.dataset.plannedQty);
    const warn = [];
    if (planned > 0 && Math.abs(p - planned) / planned > 0.05) {
      warn.push(`The fill price ${p} is ${(100 * (p - planned) / planned).toFixed(1)}% away from the planned ${planned}.`);
    }
    if (form.dataset.action === 'SELL' && q > plannedQty + 1e-9) {
      warn.push(`You're recording a sale of ${q}, more than the ${plannedQty} planned.`);
    }
    if (warn.length && !confirm(warn.join('\n') + '\n\nRecord it anyway?')) e.preventDefault();
  });

  /* ---------- Init ---------- */
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('[data-account-scope]').forEach(syncRegistered);
    document.querySelectorAll('.entry-row .f-ticker').forEach((i) => { if (i.value.trim()) checkTicker(i.closest('.entry-row')); });
    document.querySelectorAll('form').forEach(updateTargetTotal);
    localTimes(document);
    // Skip the background refresh right after a form post failed to update prices.
    if (!document.querySelector('.alert-error')) refreshIfStale();
  });
})();

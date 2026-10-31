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
    const group = document.querySelector(sel.dataset.acbToggle);
    if (group) group.classList.toggle('hidden', registered);
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

  /* ---------- Init ---------- */
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('[data-account-scope]').forEach(syncRegistered);
    document.querySelectorAll('.entry-row .f-ticker').forEach((i) => { if (i.value.trim()) checkTicker(i.closest('.entry-row')); });
    document.querySelectorAll('form').forEach(updateTargetTotal);
  });
})();

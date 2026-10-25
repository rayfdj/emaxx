(function () {
  'use strict';
  const Q = window.QueensEngine;

  // ---------- constants ----------
  const COLORS = [
    { hex: '#bba3e2', name: 'purple' },
    { hex: '#ffc992', name: 'orange' },
    { hex: '#96beff', name: 'blue' },
    { hex: '#b3dfa0', name: 'green' },
    { hex: '#dfdfdf', name: 'gray' },
    { hex: '#ff7b60', name: 'red' },
    { hex: '#e6f388', name: 'yellow' },
    { hex: '#b9b29e', name: 'tan' },
    { hex: '#dfa0bf', name: 'pink' },
    { hex: '#a3d2d8', name: 'teal' },
  ];

  const ICON = {
    crown: '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M3 8.3l4.7 4.1L12 5.7l4.3 6.7L21 8.3l-1.8 9.2H4.8z"/><rect fill="currentColor" x="4.8" y="18.7" width="14.4" height="2.5" rx=".7"/><circle fill="currentColor" cx="3" cy="7.4" r="1.6"/><circle fill="currentColor" cx="12" cy="4.4" r="1.6"/><circle fill="currentColor" cx="21" cy="7.4" r="1.6"/></svg>',
    x: '<svg viewBox="0 0 10 10" aria-hidden="true"><path d="M1.6 1.6l6.8 6.8M8.4 1.6L1.6 8.4" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg>',
    back: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 5l-7 7 7 7" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    gear: '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.9" d="M12 15.2a3.2 3.2 0 1 0 0-6.4 3.2 3.2 0 0 0 0 6.4z"/><path fill="none" stroke="currentColor" stroke-width="1.9" stroke-linejoin="round" d="M10.3 3h3.4l.5 2.4 1.7 1 2.3-.8 1.7 2.9-1.8 1.6v2l1.8 1.6-1.7 2.9-2.3-.8-1.7 1-.5 2.4h-3.4l-.5-2.4-1.7-1-2.3.8-1.7-2.9 1.8-1.6v-2L3.9 8.5l1.7-2.9 2.3.8 1.7-1z"/></svg>',
    undo: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 7H4V2M4.4 7.2A8 8 0 1 1 4 12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    bulb: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18h6M10 21h4M12 3a6 6 0 0 0-3.6 10.8c.7.6 1.1 1.4 1.1 2.2h5c0-.8.4-1.6 1.1-2.2A6 6 0 0 0 12 3z" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    clear: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    close: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
    flame: '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 2c1 3.5 5 6 5 11a5 5 0 0 1-10 0c0-2.2 1-3.8 2.3-5 .2 1.6.9 2.7 2 3.3C11.4 8.4 11 5 12 2z"/></svg>',
    next: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 5l7 7-7 7" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  };

  const TECHS = [
    {
      id: 'reach', name: "Queen's reach", tier: 0,
      short: 'A queen rules out its row, column, color and the 8 cells around it.',
      what: ['Every queen claims its whole row, its whole column and its whole color. It also rules out the 8 cells touching it, diagonals included, because queens can’t touch.'],
      spot: [
        'Right after placing a queen, mark everything it rules out before you look anywhere else.',
        'The diagonal neighbors are the ones people forget.',
        'Auto-place ×’s does this for you. Leaving it off trains your eye; turn it on when you race the clock.',
      ],
      task: 'Mark every cell the highlighted queen rules out.',
      tip: 'After every queen, sweep its row, column, color and neighbors before anything else. A clean board makes the next move obvious.',
    },
    {
      id: 'single', name: 'Last spot', tier: 1,
      short: 'A row, column or color with one open cell must take its queen there.',
      what: ['Every row, every column and every color needs exactly one queen. When all but one of its cells are ruled out, the queen goes in the cell that’s left.'],
      spot: [
        'Small colors run out of room first. Check them after every sweep.',
        'A color of a single cell is a free queen. Place it first.',
        'Rows and columns next to a fresh queen often drop to one open cell.',
      ],
      task: 'Place a queen that is forced right now.',
      tip: 'Scan the smallest colors first. Fewer cells means fewer options, so they’re the first to drop to one.',
    },
    {
      id: 'colorLine', name: 'Color in a line', tier: 2,
      short: 'A color whose open cells all sit in one row or column claims that line.',
      what: ['If every open cell of a color lies in the same row, that color’s queen will sit somewhere in that row. A row only gets one queen, so every other cell in it is ruled out. The same works for columns.'],
      spot: [
        'Look for flat colors: a strip one cell tall or one cell wide.',
        'Wide colors turn into strips after a few ×’s. Re-check them as the board fills.',
        'Colors hugging an edge are frequent candidates.',
      ],
      task: 'A color is stuck in one line. Mark the cells that clears.',
      tip: 'Look for colors that lie in a single row or column. They claim that line, which often clears half a row at once.',
    },
    {
      id: 'lineColor', name: 'Line in a color', tier: 2,
      short: 'A row or column whose open cells are all one color claims that color.',
      what: ['If every open cell of a row lies inside one color, that row’s queen and that color’s queen are the same queen. So the color can’t have its queen anywhere outside that row. Same for columns.'],
      spot: [
        'Check rows and columns that are mostly ×’d already.',
        'A color that covers a whole row or column claims it straight away.',
        'This is the mirror image of Color in a line. Train yourself to check both directions.',
      ],
      task: 'A row or column has only one color left. Mark the cells that clears.',
      tip: 'When a row is nearly full of ×’s, check the colors of its open cells. If they’re all one color, that color is locked to the row.',
    },
    {
      id: 'squeeze', name: 'Squeeze', tier: 3,
      short: 'A cell that every option of a row, column or color would rule out is out.',
      what: ['A color with a few open cells must use one of them. If some other cell would be ruled out by each of those options, because it shares their row, column or touches them, then it’s out whichever option wins.'],
      spot: [
        'Two open cells side by side in a color: the cells directly above and below the pair are out.',
        'Three in a row: the cells directly above and below the middle one are out.',
        'Rows and columns squeeze too. A row with two neighboring open cells clears the cells above and below them.',
      ],
      task: 'Some row, column or color squeezes its neighbors. Mark the cells that clears.',
      tip: 'When a color is down to 2 or 3 cells, look at what they all touch. Any cell touched by every option is an ×.',
    },
    {
      id: 'group', name: 'Groups', tier: 4,
      short: 'N colors packed into N rows or columns own those lines.',
      what: ['If two colors fit entirely inside the same two rows, those rows’ two queens must be those colors’ queens, so every other cell in the two rows is out. It works with three colors in three rows, with columns, and in reverse: if the open cells of two rows come in only two colors, those colors can’t have a queen outside those rows.'],
      spot: [
        'Look for colors stacked in a band two or three rows tall.',
        'Count: N colors inside N lines is a lock.',
        'This is Color in a line, scaled up. Once that one is automatic, start looking for pairs.',
      ],
      task: 'Some colors share a band of lines. Mark the cells that clears.',
      tip: 'Look for two colors living in the same two rows or columns. Together they lock those lines and clear everything else in them.',
    },
    {
      id: 'whatif', name: 'What if?', tier: 5,
      short: 'Imagine a queen, follow the forced moves, and look for a dead end.',
      what: ['When nothing else works, pick a cell and imagine a queen there. Follow only the forced moves: last spots and colors stuck in a line. If a row, column or color ends up with no open cell, the imagined queen was impossible, so that cell is an ×.'],
      spot: [
        'Test cells that touch a small color, or where several colors meet.',
        'Keep chains short. If nothing breaks after two or three forced moves, try another cell.',
        'Never place a real queen on a hunch. Imagine it, check it, and only mark the ×.',
      ],
      task: 'Find one cell where an imagined queen leads to a dead end, and mark it.',
      tip: 'When you’re stuck, test the cells next to the smallest color. Imagine a queen, follow what’s forced, and see if something runs out of room.',
    },
  ];
  const TECH = {};
  TECHS.forEach((t) => { TECH[t.id] = t; });
  const TIER_LABEL = ['Basics', 'Basics', 'Core', 'Intermediate', 'Advanced', 'Expert'];

  const DIFFS = {
    easy: { label: 'Easy', sizes: [6, 7], target: 90e3, blurb: 'Last spots and lines', pips: 1, tier: 2 },
    medium: { label: 'Medium', sizes: [7, 8], target: 150e3, blurb: 'Adds the squeeze', pips: 2, tier: 3 },
    hard: { label: 'Hard', sizes: [8, 9], target: 240e3, blurb: 'Needs groups', pips: 3, tier: 4 },
    expert: { label: 'Expert', sizes: [9, 10], target: 360e3, blurb: 'Needs what-if chains', pips: 4, tier: 5 },
  };
  const DIFF_ORDER = ['easy', 'medium', 'hard', 'expert'];

  const SPEED_TIPS = [
    'Drag across cells to place ×’s in one stroke. On a timer, fast marking matters as much as fast thinking.',
    'Start every board by scanning for colors with 1 to 3 cells. They give the first queens and squeezes.',
    'Sweep the board in the same order every move: smallest colors, then nearly full rows and columns.',
    'If you’re about to guess, stop. Every board has a logical next move, and the coach’s first hint only shows where to look.',
    'Re-check colors after each sweep of ×’s. A wide color can become a strip in one move.',
  ];

  // ---------- storage ----------
  const KEY = 'puzzle-dojo.v1';
  function fresh() {
    return {
      settings: { autoX: false, conflicts: true, nudges: true, rules: true, theme: 'system' },
      stats: { solved: { easy: [], medium: [], hard: [], expert: [] }, daily: {}, streak: 0, bestStreak: 0, lastDaily: null },
      mastery: {},
      game: null,
      wins: 0,
      size: 'auto',
    };
  }
  function load() {
    const base = fresh();
    try {
      const raw = localStorage.getItem(KEY);
      if (!raw) return base;
      const s = JSON.parse(raw);
      return {
        ...base, ...s,
        settings: { ...base.settings, ...(s.settings || {}) },
        stats: { ...base.stats, ...(s.stats || {}), solved: { ...base.stats.solved, ...((s.stats || {}).solved || {}) } },
      };
    } catch (e) {
      return base;
    }
  }
  let store = load();
  let saveTimer = 0;
  function save(now) {
    clearTimeout(saveTimer);
    const write = () => { try { localStorage.setItem(KEY, JSON.stringify(store)); } catch (e) { /* storage unavailable */ } };
    if (now) write(); else saveTimer = setTimeout(write, 200);
  }

  // ---------- helpers ----------
  function h(tag, attrs, ...kids) {
    const el = document.createElement(tag);
    if (attrs) {
      for (const k in attrs) {
        const v = attrs[k];
        if (v == null || v === false) continue;
        if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
        else if (k === 'html') el.innerHTML = v;
        else if (k === 'class') el.className = v;
        else el.setAttribute(k, v === true ? '' : v);
      }
    }
    for (const kid of kids.flat()) {
      if (kid == null || kid === false) continue;
      el.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
    }
    return el;
  }
  function fmt(ms) {
    const s = Math.max(0, Math.floor(ms / 1000));
    return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
  }
  function todayKey(d) {
    d = d || new Date();
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }
  function shiftDay(key, delta) {
    const d = new Date(key + 'T12:00:00');
    d.setDate(d.getDate() + delta);
    return todayKey(d);
  }
  function prettyDate(key, opts) {
    return new Date(key + 'T12:00:00').toLocaleDateString(undefined, opts || { weekday: 'short', day: 'numeric', month: 'short' });
  }
  function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
  function listJoin(items) {
    if (items.length <= 1) return items.join('');
    return items.slice(0, -1).join(', ') + ' and ' + items[items.length - 1];
  }
  let toastTimer = 0;
  function toast(msg) {
    document.querySelectorAll('.toast').forEach((t) => t.remove());
    const t = h('div', { class: 'toast', role: 'status' }, msg);
    document.body.append(t);
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.remove(), 2600);
  }

  // ---------- describing deductions ----------
  function sw(k) { return `<span class="sw" style="--c:${COLORS[k].hex}"></span>${COLORS[k].name}`; }
  function unitHtml(n, u, cap) {
    if (u < n) return (cap ? 'Row ' : 'row ') + (u + 1);
    if (u < 2 * n) return (cap ? 'Column ' : 'column ') + (u - n + 1);
    return (cap ? 'The ' : 'the ') + sw(u - 2 * n) + ' region';
  }
  function linesHtml(n, units) {
    const rows = units.filter((u) => u < n).map((u) => u + 1);
    const cols = units.filter((u) => u >= n && u < 2 * n).map((u) => u - n + 1);
    if (rows.length) return (rows.length > 1 ? 'rows ' : 'row ') + listJoin(rows.map(String));
    return (cols.length > 1 ? 'columns ' : 'column ') + listJoin(cols.map(String));
  }
  function plural(k, one, many) { return k === 1 ? one : many; }
  function others(k) { return k === 1 ? 'the one other open cell' : `the other ${k} open cells`; }

  function chainSentence(n, st) {
    if (st.tech === 'single') return `${unitHtml(n, st.units[0], true)} is left with one spot, so a queen goes there.`;
    if (st.tech === 'colorLine') return `${unitHtml(n, st.units[0], true)} gets stuck in ${unitHtml(n, st.units[1])}, which closes the rest of that line.`;
    if (st.tech === 'lineColor') return `${unitHtml(n, st.units[0], true)} only has ${sw(st.units[1] - 2 * n)} cells left, which closes the rest of that color.`;
    return '';
  }

  // Returns { nudge, detail } as HTML for a solver step.
  function describe(n, st) {
    const k = st.elim.length;
    const cells = plural(k, 'cell', 'cells');
    switch (st.tech) {
      case 'single':
        return {
          nudge: `Look at ${unitHtml(n, st.units[0])}.`,
          detail: `${unitHtml(n, st.units[0], true)} has only one open cell left, so its queen goes there.`,
        };
      case 'colorLine': {
        const [R, L] = st.units;
        return {
          nudge: `Look at ${unitHtml(n, R)}.`,
          detail: `Every open cell of ${unitHtml(n, R)} lies in ${unitHtml(n, L)}. Its queen will take ${unitHtml(n, L)}, so ${others(k)} in that line can’t hold a queen.`,
        };
      }
      case 'lineColor': {
        const [L, R] = st.units;
        return {
          nudge: `Look at ${unitHtml(n, L)}.`,
          detail: `All open cells of ${unitHtml(n, L)} are ${sw(R - 2 * n)}. The queen of ${unitHtml(n, L)} will be the ${COLORS[R - 2 * n].name} queen, so ${unitHtml(n, R)} can’t have its queen anywhere else: ${k} ${cells} out.`,
        };
      }
      case 'squeeze': {
        const u = st.units[0];
        const m = st.cause.length;
        return {
          nudge: `Look at ${unitHtml(n, u)}. It has ${m} open cells.`,
          detail: `${unitHtml(n, u, true)} must put its queen in one of its ${m} open cells (ringed). Each of them would rule out the marked ${cells}, so ${plural(k, 'it is', 'they are')} out whichever one is used.`,
        };
      }
      case 'group': {
        const regs = st.units.filter((u) => u >= 2 * n).map((u) => u - 2 * n);
        const lines = st.units.filter((u) => u < 2 * n);
        const kind = lines[0] < n ? 'rows' : 'columns';
        const colorList = listJoin(regs.map(sw));
        if (st.kind === 'colors') {
          return {
            nudge: `Look at the ${colorList} regions together.`,
            detail: `The ${colorList} regions fit entirely inside ${linesHtml(n, lines)}. That’s ${regs.length} colors in ${lines.length} ${kind}, so those ${kind} hold exactly these queens. That rules out ${others(k)} in ${linesHtml(n, lines)}.`,
          };
        }
        return {
          nudge: `Look at ${linesHtml(n, lines)} together.`,
          detail: `The open cells of ${linesHtml(n, lines)} only come in ${colorList}. Those ${lines.length} ${kind} need ${lines.length} queens and only these colors can supply them, so the colors can’t have queens outside ${linesHtml(n, lines)}: ${k} ${cells} out.`,
        };
      }
      case 'whatif': {
        const steps = st.chain.map((c) => chainSentence(n, c)).join(' ');
        return {
          nudge: 'Imagine a queen on the highlighted cell. What does it break?',
          detail: `Suppose a queen sat on the highlighted cell. ${steps} Then ${unitHtml(n, st.dead)} has no open cell left. That’s impossible, so the cell is an ×.`,
        };
      }
      default:
        return { nudge: '', detail: '' };
    }
  }

  // Overlay sets (for board rendering) that illustrate a step.
  function stepOverlay(g, st, level) {
    const focus = new Set();
    for (const u of st.units) for (const i of g.units[u]) focus.add(i);
    const o = { focus, gx: new Set(), gq: new Set(), hypo: new Set(), cause: new Set() };
    if (st.tech === 'whatif') {
      const c = st.elim[0];
      focus.add(c);
      if (level >= 3) {
        o.gq.add(c); o.hypo.add(c);
        for (const ch of st.chain) if (ch.place >= 0) { o.gq.add(ch.place); o.hypo.add(ch.place); focus.add(ch.place); }
      } else {
        o.focus = new Set([c]);
        o.gq.add(c); o.hypo.add(c);
      }
      return o;
    }
    if (level >= 3) {
      if (st.place >= 0) o.gq.add(st.place);
      for (const e of st.elim) { o.gx.add(e); focus.add(e); }
      if (st.tech === 'squeeze' || st.tech === 'group') for (const c of st.cause) o.cause.add(c);
    }
    return o;
  }

  // ---------- mastery ----------
  function pushMastery(tech, value) {
    const m = store.mastery[tech] || (store.mastery[tech] = { h: [] });
    m.h.push(value);
    if (m.h.length > 10) m.h.shift();
  }
  function masteryOf(tech) {
    const m = store.mastery[tech];
    if (!m || !m.h.length) return { n: 0, score: 0, label: 'New' };
    const score = m.h.reduce((a, b) => a + b, 0) / m.h.length;
    let label = 'Learning';
    if (m.h.length >= 8 && score >= 0.9) label = 'Mastered';
    else if (m.h.length >= 4 && score >= 0.75) label = 'Strong';
    else if (score >= 0.5) label = 'Getting there';
    return { n: m.h.length, score, label };
  }
  function meter(tech) {
    const m = masteryOf(tech);
    return h('span', { class: 'meter', title: m.n ? `${Math.round(m.score * 100)}% across your last ${m.n}` : 'Not practiced yet' },
      h('span', { class: 'track' }, h('span', { class: 'fill', style: `width:${Math.round(m.score * 100)}%` })),
      m.label);
  }

  // ---------- board component ----------
  function bordersSvg(n, reg, width) {
    let d = '';
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < n; c++) {
        const i = r * n + c;
        if (c < n - 1 && reg[i] !== reg[i + 1]) d += `M${c + 1} ${r}V${r + 1}`;
        if (r < n - 1 && reg[i] !== reg[i + n]) d += `M${c} ${r + 1}H${c + 1}`;
      }
    }
    return `<svg class="borders" viewBox="0 0 ${n} ${n}" preserveAspectRatio="none" aria-hidden="true"><path d="${d}" style="fill:none;stroke:var(--board-line)" stroke-width="${width}" vector-effect="non-scaling-stroke" stroke-linecap="square"/></svg>`;
  }
  function lineCells(a, b, n) {
    // Cells from a (exclusive) to b (inclusive), so fast drags don't skip any.
    let r0 = (a / n) | 0, c0 = a % n;
    const r1 = (b / n) | 0, c1 = b % n;
    const out = [];
    const steps = Math.max(Math.abs(r1 - r0), Math.abs(c1 - c0));
    for (let s = 1; s <= steps; s++) {
      const r = Math.round(r0 + ((r1 - r0) * s) / steps);
      const c = Math.round(c0 + ((c1 - c0) * s) / steps);
      out.push(r * n + c);
    }
    return out;
  }

  function makeBoard(puzzle, opts) {
    opts = opts || {};
    const n = puzzle.n, reg = puzzle.regions;
    const el = h('div', { class: 'board' + (opts.mini ? ' mini' : ''), style: `--n:${n}` });
    const grid = h('div', { class: 'grid' });
    if (opts.mini) el.setAttribute('aria-hidden', 'true');
    else { el.tabIndex = 0; el.setAttribute('role', 'grid'); el.setAttribute('aria-label', `Queens board, ${n} by ${n}. Arrow keys move, Space cycles a cell.`); }
    const cells = [];
    for (let i = 0; i < n * n; i++) {
      const c = h('div', { class: 'cell', style: `background:${COLORS[reg[i]].hex}` });
      if (!opts.mini) c.setAttribute('role', 'gridcell');
      grid.append(c);
      cells.push({ el: c, key: null });
    }
    el.append(grid);
    el.insertAdjacentHTML('beforeend', bordersSvg(n, reg, opts.mini ? 1.6 : 2.5));
    let kbd = -1;

    function render(v) {
      el.classList.toggle('spot', !!(v.focus && v.focus.size));
      for (let i = 0; i < n * n; i++) {
        const m = v.marks[i];
        let glyph = '';
        if (m === 2) glyph = v.bad && v.bad.has(i) ? 'qb' : 'q';
        else if (m === 1) glyph = 'x';
        else if (v.auto && v.auto[i]) glyph = 'a';
        let ghost = '';
        if (v.gq && v.gq.has(i)) ghost = v.hypo && v.hypo.has(i) ? 'h' : 'q';
        else if (v.gx && v.gx.has(i)) ghost = 'x';
        const cause = v.cause && v.cause.has(i) ? 'c' : '';
        const key = glyph + '|' + ghost + '|' + cause;
        const cell = cells[i];
        if (key !== cell.key) {
          let html = '';
          if (glyph === 'q' || glyph === 'qb') html += `<span class="g q${glyph === 'qb' ? ' bad' : ''}${v.pop === i ? ' pop' : ''}">${ICON.crown}</span>`;
          else if (glyph) html += `<span class="g x${glyph === 'a' ? ' auto' : ''}">${ICON.x}</span>`;
          if (cause) html += '<span class="ring"></span>';
          if (ghost === 'x') html += `<span class="ghost gx">${ICON.x}</span>`;
          else if (ghost) html += `<span class="ghost gq${ghost === 'h' ? ' hypo' : ''}">${ICON.crown}</span>`;
          cell.el.innerHTML = html;
          if (!opts.mini) {
            const what = glyph === 'q' || glyph === 'qb' ? 'queen' : glyph ? 'marked ×' : 'empty';
            cell.el.setAttribute('aria-label', `Row ${((i / n) | 0) + 1}, column ${(i % n) + 1}, ${COLORS[reg[i]].name}, ${what}`);
          }
          cell.key = key;
        }
        cell.el.classList.toggle('clash', !!(v.clash && v.clash[i]));
        cell.el.classList.toggle('focus', !!(v.focus && v.focus.has(i)));
        cell.el.classList.toggle('kbd', kbd === i);
      }
    }

    function flash(i, good) {
      const c = cells[i].el;
      const cls = good ? 'flash-good' : 'flash-bad';
      c.classList.remove('flash-good', 'flash-bad');
      void c.offsetWidth;
      c.classList.add(cls);
      setTimeout(() => c.classList.remove(cls), 700);
    }

    function celebrate() {
      el.classList.add('won');
      cells.forEach((c, i) => { const q = c.el.querySelector('.g.q'); if (q) q.style.setProperty('--k', (i / n) | 0); });
    }

    if (opts.interactive) {
      let down = null;
      const cellAt = (x, y) => {
        const r = grid.getBoundingClientRect();
        const cx = Math.floor(((x - r.left) / r.width) * n);
        const cy = Math.floor(((y - r.top) / r.height) * n);
        return cx < 0 || cy < 0 || cx >= n || cy >= n ? -1 : cy * n + cx;
      };
      el.addEventListener('pointerdown', (e) => {
        if (e.pointerType === 'mouse' && e.button !== 0) return;
        const i = cellAt(e.clientX, e.clientY);
        if (i < 0) return;
        e.preventDefault();
        if (kbd >= 0 && e.pointerType !== 'keyboard') { kbd = -1; opts.onKbd && opts.onKbd(); }
        try { el.setPointerCapture(e.pointerId); } catch (err) { /* ignore */ }
        down = { id: e.pointerId, i, last: i, moved: false, mode: null };
      });
      el.addEventListener('pointermove', (e) => {
        if (!down || e.pointerId !== down.id) return;
        const i = cellAt(e.clientX, e.clientY);
        if (i < 0 || i === down.last) return;
        if (!down.moved) {
          down.moved = true;
          down.mode = opts.onPaintStart ? opts.onPaintStart(down.i) : null;
          if (down.mode) opts.onPaint(down.i, down.mode);
        }
        if (down.mode) for (const j of lineCells(down.last, i, n)) opts.onPaint(j, down.mode);
        down.last = i;
      });
      const end = (e) => {
        if (!down || e.pointerId !== down.id) return;
        const d = down;
        down = null;
        if (d.moved) { if (d.mode && opts.onPaintEnd) opts.onPaintEnd(); }
        else if (e.type === 'pointerup') opts.onTap(d.i);
      };
      el.addEventListener('pointerup', end);
      el.addEventListener('pointercancel', end);
      el.addEventListener('contextmenu', (e) => e.preventDefault());
      el.addEventListener('keydown', (e) => {
        const key = e.key;
        const moves = { ArrowUp: -n, ArrowDown: n, ArrowLeft: -1, ArrowRight: 1 };
        if (key in moves) {
          e.preventDefault();
          if (kbd < 0) kbd = 0;
          else {
            const r = (kbd / n) | 0, c = kbd % n;
            if (key === 'ArrowUp' && r > 0) kbd -= n;
            if (key === 'ArrowDown' && r < n - 1) kbd += n;
            if (key === 'ArrowLeft' && c > 0) kbd -= 1;
            if (key === 'ArrowRight' && c < n - 1) kbd += 1;
          }
          opts.onKbd && opts.onKbd();
          return;
        }
        if (kbd < 0) return;
        if (key === ' ' || key === 'Enter') { e.preventDefault(); opts.onTap(kbd); }
        else if (opts.onKey && (key === 'x' || key === 'X' || key === 'q' || key === 'Q' || key === 'Backspace' || key === 'Delete')) {
          e.preventDefault();
          opts.onKey(kbd, key.toLowerCase());
        }
      });
      el.addEventListener('blur', () => { if (kbd >= 0) { kbd = -1; opts.onKbd && opts.onKbd(); } });
    }
    return { el, render, flash, celebrate, cells };
  }

  function marksFromState(cand, queen) {
    const m = new Array(cand.length);
    for (let i = 0; i < cand.length; i++) m[i] = queen[i] ? 2 : cand[i] ? 0 : 1;
    return m;
  }

  // ---------- app shell ----------
  const root = document.getElementById('app');
  const app = { view: 'home', lessonId: null };
  let cleanup = [];
  function clearView() {
    cleanup.forEach((f) => f());
    cleanup = [];
    root.replaceChildren();
    document.querySelectorAll('.scrim').forEach((s) => s.remove());
  }
  function applyTheme() {
    const t = store.settings.theme;
    if (t === 'system') delete document.documentElement.dataset.theme;
    else document.documentElement.dataset.theme = t;
  }

  // ---------- generation ----------
  let genToken = 0;
  function sizeFor(diff) {
    if (store.size !== 'auto') return +store.size;
    return pick(DIFFS[diff].sizes);
  }
  function dailyConfig(key) {
    const dow = new Date(key + 'T12:00:00').getDay();
    return {
      seed: Q.hashString('queens:' + key),
      difficulty: ['hard', 'easy', 'easy', 'medium', 'medium', 'medium', 'hard'][dow],
      size: [9, 7, 7, 8, 8, 8, 9][dow],
    };
  }
  async function generatePuzzle(size, difficulty, seed) {
    let p = await Q.generateAsync({ size, difficulty, seed, maxAttempts: 30000 });
    if (!p) p = await Q.generateAsync({ size, seed: seed == null ? undefined : seed + 1, maxAttempts: 30000 });
    return p;
  }

  // ---------- game ----------
  let rt = null; // runtime state for the open game

  function newGameRecord(p, extra) {
    return {
      puzzle: { n: p.n, regions: p.regions, solution: p.solution, difficulty: p.difficulty, counts: p.counts, maxTier: p.maxTier },
      kind: extra.kind || 'free', date: extra.date || null, practice: !!extra.practice,
      marks: new Array(p.n * p.n).fill(0), elapsed: 0, done: false, undo: [], hints: [], clashes: 0, created: Date.now(),
    };
  }

  async function startGame(cfg) {
    // cfg: { difficulty, size, kind, date, seed, practice }
    const token = ++genToken;
    clearView();
    app.view = 'game';
    const subtitle = cfg.kind === 'daily' ? `Daily · ${prettyDate(cfg.date)}` : `${DIFFS[cfg.difficulty].label} · ${cfg.size}×${cfg.size}`;
    root.append(gameBar(subtitle, null), h('div', { class: 'wrap' },
      h('div', { class: 'board loading', style: 'max-width:30rem' },
        h('div', { class: 'loading-text' }, h('span', { class: 'spinner' }), 'Drawing a fresh board…'))));
    const p = await generatePuzzle(cfg.size, cfg.difficulty, cfg.seed);
    if (token !== genToken) return;
    if (!p) { toast('Could not build a board. Try another size.'); showHome(); return; }
    store.game = newGameRecord(p, cfg);
    save();
    openGame();
  }

  function gameBar(subtitle, timerEl) {
    return h('div', { class: 'wrap', style: 'padding-block:0;gap:0;position:sticky;top:env(safe-area-inset-top,0px);z-index:5;background:var(--bg)' },
      h('div', { class: 'bar' },
        h('button', { class: 'icon-btn', 'aria-label': 'Back to home', html: ICON.back, onclick: () => showHome() }),
        h('div', { class: 'title' }, h('b', null, 'Queens'), h('span', null, subtitle)),
        timerEl,
        h('button', { class: 'icon-btn', 'aria-label': 'Settings', html: ICON.gear, onclick: () => openSettings() })));
  }

  function gameSubtitle(gm) {
    const p = gm.puzzle;
    const base = `${DIFFS[p.difficulty].label} · ${p.n}×${p.n}`;
    return gm.kind === 'daily' ? `Daily · ${prettyDate(gm.date)} · ${base}` : base;
  }

  function analyze() {
    const gm = store.game, g = rt.g, marks = gm.marks, n = gm.puzzle.n;
    const queens = [];
    for (let i = 0; i < marks.length; i++) if (marks[i] === 2) queens.push(i);
    const clash = new Uint8Array(n * n);
    const bad = new Set();
    for (let u = 0; u < 3 * n; u++) {
      const qs = g.units[u].filter((i) => marks[i] === 2);
      if (qs.length > 1) { for (const c of g.units[u]) clash[c] = 1; qs.forEach((q) => bad.add(q)); }
    }
    for (let a = 0; a < queens.length; a++) {
      for (let b = a + 1; b < queens.length; b++) {
        const qa = queens[a], qb = queens[b];
        if (Math.abs(((qa / n) | 0) - ((qb / n) | 0)) <= 1 && Math.abs((qa % n) - (qb % n)) <= 1) {
          clash[qa] = clash[qb] = 1; bad.add(qa); bad.add(qb);
        }
      }
    }
    const auto = new Uint8Array(n * n);
    if (store.settings.autoX) for (const q of queens) for (const a of g.attacks[q]) if (marks[a] === 0) auto[a] = 1;
    return { queens, clash, bad, auto, solved: queens.length === n && bad.size === 0 };
  }

  function openGame() {
    clearView();
    app.view = 'game';
    const gm = store.game;
    const p = gm.puzzle;
    const g = Q.makeGeom(p.n, p.regions);
    rt = {
      g, solSet: new Set(p.solution.map((c, r) => r * p.n + c)),
      hint: null, level: 0, hintIdx: -1, pending: null, lastMove: Date.now(), pop: -1, clearArmed: false,
      last: performance.now(),
    };
    rt.analysis = analyze();

    const timerEl = h('div', { class: 'timer', 'aria-label': 'Elapsed time' }, fmt(gm.elapsed));
    const board = makeBoard(p, {
      interactive: true,
      onTap, onPaintStart, onPaint, onPaintEnd, onKey,
      onKbd: () => draw(),
    });
    rt.board = board;
    const undoBtn = h('button', { class: 'btn', html: ICON.undo + '<span>Undo</span>', onclick: undo });
    const hintBtn = h('button', { class: 'btn gold', html: ICON.bulb + '<span>Hint</span>', onclick: toggleHint });
    const clearBtn = h('button', { class: 'btn', html: ICON.clear + '<span>Clear</span>', onclick: clearBoard });
    const controls = h('div', { class: 'controls' }, undoBtn, hintBtn, clearBtn);
    const note = h('p', { class: 'stuck-note', 'aria-live': 'polite' });
    const coachSlot = h('div');
    const doneSlot = h('div', { class: 'row', style: 'max-width:30rem;width:100%;margin-inline:auto' });
    rt.ui = { timerEl, undoBtn, hintBtn, clearBtn, controls, note, coachSlot, doneSlot };

    const wrap = h('div', { class: 'wrap' }, h('div', { class: 'board-wrap' }, board.el), controls, note, coachSlot, doneSlot);
    if (store.settings.rules && store.wins < 5) wrap.append(rulesStrip());
    root.append(gameBar(gameSubtitle(gm), timerEl), wrap);
    draw();
    if (gm.done) showDone(false);

    const tick = setInterval(() => {
      const now = performance.now();
      if (!gm.done && !document.hidden) gm.elapsed += now - rt.last;
      rt.last = now;
      timerEl.textContent = fmt(gm.elapsed);
      checkIdle();
    }, 250);
    const onVis = () => { rt.last = performance.now(); if (document.hidden) save(true); };
    document.addEventListener('visibilitychange', onVis);
    const onKeys = (e) => {
      if (e.target && (e.target.tagName === 'INPUT')) return;
      if ((e.key === 'z' || e.key === 'Z') && (e.metaKey || e.ctrlKey)) { e.preventDefault(); undo(); }
      else if ((e.key === 'h' || e.key === 'H') && !e.metaKey && !e.ctrlKey && !e.altKey) toggleHint();
    };
    document.addEventListener('keydown', onKeys);
    cleanup.push(() => {
      clearInterval(tick);
      document.removeEventListener('visibilitychange', onVis);
      document.removeEventListener('keydown', onKeys);
      save(true);
      rt = null;
    });
  }

  function rulesStrip() {
    const ex = (body) => `<svg viewBox="0 0 34 34" aria-hidden="true">${body}</svg>`;
    const crownAt = (x, y, s) => `<g transform="translate(${x} ${y}) scale(${s})" fill="#1a1c21"><path d="M3 8.3l4.7 4.1L12 5.7l4.3 6.7L21 8.3l-1.8 9.2H4.8z"/><rect x="4.8" y="18.7" width="14.4" height="2.5"/></g>`;
    const strip = h('div', { class: 'rules', role: 'note', 'aria-label': 'Rules' },
      h('div', null, h('span', { html: ex(`<rect width="34" height="34" fill="#dfdfdf"/><rect y="11.3" width="34" height="11.3" fill="#96beff"/><rect x="11.3" width="11.3" height="34" fill="#96beff" opacity=".55"/>${crownAt(12.3, 12.3, 0.4)}`) }),
        'One queen in every row and column'),
      h('div', null, h('span', { html: ex(`<rect width="34" height="34" fill="#ffc992"/><path d="M0 0h20v14H0z" fill="#bba3e2"/>${crownAt(5, 2, 0.42)}${crownAt(19, 19, 0.42)}`) }),
        'One queen in every color'),
      h('div', null, h('span', { html: ex(`<rect width="34" height="34" fill="#b3dfa0"/><rect x="11.3" y="11.3" width="11.3" height="11.3" fill="#ff7b60" opacity=".5"/><rect x="0" y="0" width="11.3" height="11.3" fill="#ff7b60" opacity=".5"/><rect x="22.6" y="22.6" width="11.3" height="11.3" fill="#ff7b60" opacity=".5"/>${crownAt(12.3, 12.3, 0.4)}<path d="M2 2l7 7M9 2L2 9M25 25l7 7M32 25l-7 7" stroke="#1a1c21" stroke-width="1.6"/>`) }),
        'Queens never touch, not even diagonally'),
      h('button', {
        class: 'icon-btn close', 'aria-label': 'Hide rules', html: ICON.close,
        onclick: () => { store.settings.rules = false; save(); strip.remove(); how.remove(); },
      }));
    const how = h('p', { class: 'how' }, 'Tap once for ×, twice for a queen. Drag to mark many ×’s.');
    const frag = document.createDocumentFragment();
    frag.append(strip, how);
    return frag;
  }

  function hintOverlay() {
    if (!rt.hint) return {};
    const lv = rt.hint.levels[Math.min(rt.level, rt.hint.levels.length) - 1];
    return lv.overlay || {};
  }

  function draw() {
    if (!rt) return;
    const gm = store.game;
    const a = rt.analysis;
    const ov = hintOverlay();
    rt.board.render({
      marks: gm.marks,
      auto: a.auto,
      clash: store.settings.conflicts ? a.clash : null,
      bad: store.settings.conflicts ? a.bad : null,
      pop: rt.pop,
      ...ov,
    });
    rt.pop = -1;
    const { undoBtn, clearBtn, hintBtn } = rt.ui;
    undoBtn.disabled = gm.done || !gm.undo.length;
    clearBtn.disabled = gm.done || !gm.marks.some((m) => m);
    hintBtn.disabled = gm.done;
    if (!rt.clearArmed) clearBtn.querySelector('span').textContent = 'Clear';
  }

  function commit(changes, pop) {
    const gm = store.game;
    const real = [];
    for (const { i, to } of changes) {
      const from = gm.marks[i];
      if (from !== to) { gm.marks[i] = to; real.push({ i, from, to }); }
    }
    if (!real.length) return;
    gm.undo.push(real);
    if (gm.undo.length > 400) gm.undo.shift();
    afterMove(real, pop);
  }

  function afterMove(real, pop) {
    const gm = store.game;
    rt.lastMove = Date.now();
    rt.ui.hintBtn.classList.remove('glow');
    rt.ui.note.textContent = '';
    if (rt.hint) closeHint(false);
    rt.analysis = analyze();
    for (const c of real) if (c.to === 2 && rt.analysis.bad.has(c.i)) gm.clashes++;
    rt.pop = pop == null ? -1 : pop;
    draw();
    if (rt.analysis.solved && !gm.done) win();
    save();
  }

  function onTap(i) {
    const gm = store.game;
    if (gm.done) return;
    const m = gm.marks[i];
    let to;
    if (m === 0) to = rt.analysis.auto[i] ? 2 : 1;
    else if (m === 1) to = 2;
    else to = 0;
    commit([{ i, to }], to === 2 ? i : -1);
  }
  function onKey(i, key) {
    if (store.game.done) return;
    const to = key === 'x' ? 1 : key === 'q' ? 2 : 0;
    commit([{ i, to }], to === 2 ? i : -1);
  }
  function onPaintStart(i) {
    if (store.game.done) return null;
    rt.pending = [];
    return store.game.marks[i] === 1 ? 'erase' : 'x';
  }
  function onPaint(i, mode) {
    const gm = store.game;
    const m = gm.marks[i];
    if (mode === 'x' && m === 0) { gm.marks[i] = 1; rt.pending.push({ i, from: 0, to: 1 }); }
    else if (mode === 'erase' && m === 1) { gm.marks[i] = 0; rt.pending.push({ i, from: 1, to: 0 }); }
    else return;
    if (rt.hint) closeHint(false);
    draw();
  }
  function onPaintEnd() {
    const changes = rt.pending || [];
    rt.pending = null;
    rt.last = performance.now();
    if (!changes.length) return;
    store.game.undo.push(changes);
    afterMove(changes);
  }
  function undo() {
    const gm = store.game;
    if (!rt || gm.done || !gm.undo.length) return;
    const group = gm.undo.pop();
    for (let k = group.length - 1; k >= 0; k--) gm.marks[group[k].i] = group[k].from;
    afterMove([]);
  }
  function clearBoard() {
    const gm = store.game;
    if (gm.done) return;
    const btn = rt.ui.clearBtn;
    if (!rt.clearArmed) {
      rt.clearArmed = true;
      btn.querySelector('span').textContent = 'Tap to confirm';
      setTimeout(() => { if (rt) { rt.clearArmed = false; draw(); } }, 2500);
      return;
    }
    rt.clearArmed = false;
    commit(gm.marks.map((m, i) => ({ i, to: 0 })).filter((c) => gm.marks[c.i] !== 0));
  }

  function checkIdle() {
    const gm = store.game;
    if (!rt || gm.done || rt.hint || !store.settings.nudges) return;
    if (Date.now() - rt.lastMove > 45000 && !rt.ui.hintBtn.classList.contains('glow')) {
      rt.ui.hintBtn.classList.add('glow');
      rt.ui.note.textContent = 'Stuck? A hint can just point you somewhere, without the answer.';
    }
  }

  // ---------- coach ----------
  function computeHint() {
    const gm = store.game, g = rt.g, n = gm.puzzle.n, marks = gm.marks;
    const a = rt.analysis;
    const wrongQ = a.queens.filter((i) => !rt.solSet.has(i));
    const wrongX = [...rt.solSet].filter((i) => marks[i] === 1);
    if (wrongQ.length || wrongX.length) {
      const clashing = a.bad.size > 0;
      const cells = wrongQ.length ? wrongQ : wrongX;
      const isQ = wrongQ.length > 0;
      const first = clashing
        ? 'Two queens break a rule in the striped area. One of them has to go.'
        : isQ ? `${plural(cells.length, 'One of your queens is', 'Some of your queens are')} in the wrong place. Nothing is broken yet, but the board can’t be finished this way.`
          : `${plural(cells.length, 'One of your ×’s covers', 'Some of your ×’s cover')} a cell that needs a queen.`;
      return {
        tech: 'fix',
        levels: [
          { html: first, overlay: {} },
          {
            html: isQ ? `Here ${plural(cells.length, 'it is', 'they are')}. Remove ${plural(cells.length, 'it', 'them')} and look again.` : `Here ${plural(cells.length, 'it is', 'they are')}. Clear ${plural(cells.length, 'it', 'them')} and look again.`,
            overlay: { focus: new Set(cells), gx: isQ ? new Set(cells) : new Set() },
            apply: cells.map((i) => ({ i, to: 0 })),
            applyLabel: isQ ? 'Remove' : 'Clear',
          },
        ],
      };
    }
    const s = Q.stateFromMarks(g, marks);
    const st = Q.findStep(g, s);
    if (!st) return null;
    const d = describe(n, st);
    const t = TECH[st.tech];
    const apply = st.place >= 0 ? [{ i: st.place, to: 2 }] : st.elim.map((i) => ({ i, to: 1 }));
    return {
      tech: st.tech, step: st,
      levels: [
        { html: d.nudge, overlay: stepOverlay(g, st, 1) },
        {
          html: `<span class="tech-name">${t.name}</span><span class="rule">${t.short}</span>`,
          overlay: stepOverlay(g, st, 2), learn: st.tech,
        },
        { html: d.detail, overlay: stepOverlay(g, st, 3), apply, applyLabel: st.place >= 0 ? 'Place it' : 'Mark them', learn: st.tech },
      ],
    };
  }

  function toggleHint() {
    const gm = store.game;
    if (!rt || gm.done) return;
    if (rt.hint) { closeHint(true); return; }
    const hint = computeHint();
    if (!hint) return;
    rt.hint = hint;
    rt.level = 1;
    gm.hints.push({ tech: hint.tech, level: 1 });
    rt.hintIdx = gm.hints.length - 1;
    rt.ui.hintBtn.classList.remove('glow');
    rt.ui.note.textContent = '';
    renderCoach();
    draw();
    save();
  }
  function escalate() {
    rt.level++;
    store.game.hints[rt.hintIdx].level = rt.level;
    renderCoach();
    draw();
    save();
  }
  function closeHint(redraw) {
    rt.hint = null;
    rt.level = 0;
    rt.ui.coachSlot.replaceChildren();
    if (redraw) draw();
  }
  function renderCoach() {
    const hint = rt.hint;
    const lv = hint.levels[rt.level - 1];
    const total = hint.levels.length;
    const pips = h('span', { class: 'lv', 'aria-label': `Hint ${rt.level} of ${total}` });
    for (let k = 0; k < total; k++) pips.append(h('i', { class: k < rt.level ? 'on' : '' }));
    const actions = h('div', { class: 'row' });
    if (rt.level < total) {
      const label = hint.tech === 'fix' ? 'Show me' : rt.level === 1 ? 'Name the technique' : 'Show me';
      actions.append(h('button', { class: 'btn small', onclick: escalate }, label));
    }
    if (lv.apply) {
      actions.append(h('button', {
        class: 'btn small primary',
        onclick: () => { const ap = lv.apply; closeHint(false); commit(ap, ap.length === 1 && ap[0].to === 2 ? ap[0].i : -1); },
      }, lv.applyLabel));
    }
    if (lv.learn) {
      actions.append(h('button', { class: 'btn small ghost', onclick: () => openLesson(lv.learn, 'game') }, `Learn: ${TECH[lv.learn].name}`));
    }
    const card = h('section', { class: 'coach' + (rt.level === 1 ? ' enter' : ''), 'aria-live': 'polite' },
      h('div', { class: 'coach-head' },
        h('span', { class: 'who', html: ICON.bulb + '<span>Coach</span>' }),
        pips,
        h('button', { class: 'icon-btn', 'aria-label': 'Close hint', html: ICON.close, onclick: () => closeHint(true), style: 'width:32px;height:32px' })),
      h('p', { class: 'coach-text', html: lv.html }),
      actions);
    rt.ui.coachSlot.replaceChildren(card);
  }

  // ---------- winning ----------
  function win() {
    const gm = store.game;
    gm.done = true;
    if (rt.hint) closeHint(false);
    const p = gm.puzzle;
    const entry = { ms: Math.round(gm.elapsed), n: p.n, hints: gm.hints.length, at: Date.now() };
    const list = store.stats.solved[p.difficulty];
    const prev = list.slice();
    let firstDaily = false;
    if (!gm.practice) {
      list.push(entry);
      if (list.length > 60) list.shift();
      if (gm.kind === 'daily' && !store.stats.daily[gm.date]) {
        firstDaily = true;
        store.stats.daily[gm.date] = entry;
        const st = store.stats;
        st.streak = st.lastDaily === shiftDay(gm.date, -1) ? st.streak + 1 : 1;
        st.lastDaily = gm.date;
        st.bestStreak = Math.max(st.bestStreak, st.streak);
      }
      // Mastery: each technique the board needed counts as solo, nudged or helped.
      const outcome = {};
      for (const hnt of gm.hints) {
        if (hnt.tech === 'fix') continue;
        const v = hnt.level >= 2 ? 0 : 0.5;
        outcome[hnt.tech] = Math.min(outcome[hnt.tech] == null ? 1 : outcome[hnt.tech], v);
      }
      for (const tech in p.counts) if (p.counts[tech] > 0) pushMastery(tech, outcome[tech] == null ? 1 : outcome[tech]);
      pushMastery('reach', gm.clashes === 0 ? 1 : gm.clashes === 1 ? 0.5 : 0);
      store.wins++;
    }
    gm.result = { prev, firstDaily };
    save(true);
    draw();
    rt.board.celebrate();
    showDone(true);
  }

  function showDone(fresh) {
    const gm = store.game;
    rt.ui.controls.hidden = true;
    rt.ui.note.textContent = '';
    rt.ui.doneSlot.replaceChildren(
      h('button', { class: 'btn ghost', onclick: () => openDebrief() }, 'Review'),
      h('button', { class: 'btn primary', onclick: nextBoard, html: '<span>Next board</span>' + ICON.next }));
    if (fresh) setTimeout(() => { if (rt && store.game === gm) openDebrief(); }, 850);
  }

  function nextBoard() {
    const d = store.game.puzzle.difficulty;
    startGame({ difficulty: d, size: sizeFor(d), kind: 'free' });
  }

  function keyMoment(p) {
    const g = Q.makeGeom(p.n, p.regions);
    const res = Q.solve(g, null, { snapshots: true });
    if (!res.ok || !res.steps.length) return null;
    let best = res.steps[0];
    for (const st of res.steps) if (st.tier > best.tier) best = st;
    return { g, st: best };
  }

  function openDebrief() {
    const gm = store.game;
    if (!gm || !gm.done) return;
    const p = gm.puzzle;
    const ms = gm.elapsed;
    const prev = (gm.result && gm.result.prev) || [];
    const best = prev.length ? Math.min(...prev.map((e) => e.ms)) : null;
    const recent = prev.slice(-10);
    const avg = recent.length ? recent.reduce((a, e) => a + e.ms, 0) / recent.length : null;
    let line = 'Your first ' + DIFFS[p.difficulty].label.toLowerCase() + ' board. Nice.';
    let pb = false;
    if (gm.practice) line = 'Practice run, not counted in your stats.';
    else if (best != null && ms < best) { line = `New personal best, ${fmt(best - ms)} faster than before.`; pb = true; }
    else if (avg != null) line = ms <= avg ? `${fmt(avg - ms)} faster than your recent average.` : `Recent average ${fmt(avg)}. Best ${fmt(best)}.`;
    const hintsUsed = gm.hints.length;

    // How each technique went.
    const outcome = {};
    for (const hnt of gm.hints) {
      if (hnt.tech === 'fix') continue;
      const v = hnt.level >= 2 ? 0 : 0.5;
      outcome[hnt.tech] = Math.min(outcome[hnt.tech] == null ? 1 : outcome[hnt.tech], v);
    }
    const used = h('div', { class: 'used' });
    for (const t of TECHS) {
      const k = (p.counts || {})[t.id];
      if (!k) continue;
      const o = outcome[t.id] == null ? 1 : outcome[t.id];
      const [cls, txt] = o === 1 ? ['solo', 'On your own'] : o === 0.5 ? ['nudged', 'With a nudge'] : ['helped', 'With help'];
      used.append(h('div', { class: 'used-row' },
        h('button', { class: 'linkish', style: 'text-align:left', onclick: () => openLesson(t.id, 'game') }, `${t.name} ×${k}`),
        h('span', { class: 'how-got ' + cls }, txt)));
    }

    const km = keyMoment(p);
    let moment = null;
    if (km) {
      const mini = makeBoard(p, { mini: true });
      const ov = stepOverlay(km.g, km.st, 3);
      mini.render({ marks: marksFromState(km.st.before.cand, km.st.before.queen), ...ov });
      const d = describe(p.n, km.st);
      moment = h('section', { class: 'section' },
        h('div', { class: 'section-head' }, h('h3', null, 'The key moment'), h('span', { class: 'eyebrow' }, TECH[km.st.tech].name)),
        h('div', { class: 'moment' }, mini.el, h('p', { html: d.detail })));
    }

    // One tip: a technique you needed help with, else the board's hardest technique if it's new to you, else speed.
    let tipText = null;
    const helped = TECHS.find((t) => outcome[t.id] != null && outcome[t.id] < 1);
    if (helped) tipText = helped.tip;
    else if (km && masteryOf(km.st.tech).n <= 2 && km.st.tier >= 2) tipText = TECH[km.st.tech].tip;
    else tipText = SPEED_TIPS[store.wins % SPEED_TIPS.length];

    const rec = recommendDifficulty();
    const nextBtns = h('div', { class: 'row' },
      h('button', { class: 'btn ghost', onclick: () => { close(); showHome(); } }, 'Home'),
      h('button', { class: 'btn primary', onclick: () => { close(); nextBoard(); }, html: '<span>Next board</span>' + ICON.next }));
    if (!gm.practice && DIFF_ORDER.indexOf(rec) > DIFF_ORDER.indexOf(p.difficulty)) {
      nextBtns.append(h('button', { class: 'btn gold', onclick: () => { close(); startGame({ difficulty: rec, size: sizeFor(rec), kind: 'free' }); } }, `Try ${DIFFS[rec].label}`));
    }

    const streakLine = gm.kind === 'daily' && gm.result && gm.result.firstDaily
      ? h('span', { class: 'streak', html: ICON.flame + `<span>${store.stats.streak}-day streak</span>` }) : null;

    const { close } = openSheet(
      h('div', { class: 'verdict' },
        h('span', { class: 'eyebrow' }, `Solved · ${gameSubtitle(gm)}`),
        h('div', { class: 'big-time' }, fmt(ms)),
        h('p', { class: pb ? 'pb' : 'muted' }, line),
        h('p', { class: 'muted', style: 'font-size:.85rem' }, hintsUsed ? `${hintsUsed} ${plural(hintsUsed, 'hint', 'hints')} used` : 'No hints used'),
        streakLine),
      h('section', { class: 'section' }, h('h3', null, 'What this board asked of you'), used),
      moment,
      h('div', { class: 'tip', html: ICON.bulb + `<span>${tipText}</span>` }),
      nextBtns);
  }

  function openSheet(...content) {
    const sheet = h('div', { class: 'sheet', role: 'dialog', 'aria-modal': 'true' }, h('div', { class: 'grab' }), ...content);
    const scrim = h('div', { class: 'scrim' }, sheet);
    const close = () => { scrim.remove(); document.removeEventListener('keydown', onEsc); };
    const onEsc = (e) => { if (e.key === 'Escape') close(); };
    scrim.addEventListener('click', (e) => { if (e.target === scrim) close(); });
    document.addEventListener('keydown', onEsc);
    document.body.append(scrim);
    const focusable = sheet.querySelector('button');
    if (focusable) focusable.focus({ preventScroll: true });
    return { close, sheet };
  }

  // ---------- settings ----------
  function openSettings() {
    const s = store.settings;
    const toggle = (id, label, desc, key) => h('div', { class: 'setting' },
      h('label', { for: id }, label),
      h('span', { class: 'switch' },
        h('input', {
          id, type: 'checkbox', role: 'switch', checked: s[key] ? true : null,
          onchange: (e) => {
            s[key] = e.target.checked; save();
            if (rt) { rt.analysis = analyze(); draw(); }
          },
        }), h('span')),
      h('small', null, desc));
    const seg = h('div', { class: 'seg', role: 'group', 'aria-label': 'Theme' });
    for (const [val, lab] of [['system', 'System'], ['light', 'Light'], ['dark', 'Dark']]) {
      seg.append(h('button', {
        'aria-pressed': s.theme === val ? 'true' : 'false',
        onclick: (e) => {
          s.theme = val; save(); applyTheme();
          seg.querySelectorAll('button').forEach((b) => b.setAttribute('aria-pressed', b === e.currentTarget ? 'true' : 'false'));
        },
      }, lab));
    }
    let armed = false;
    const reset = h('button', {
      class: 'btn ghost small',
      onclick: () => {
        if (!armed) { armed = true; reset.textContent = 'Tap again to erase everything'; return; }
        const theme = s.theme;
        store = fresh();
        store.settings.theme = theme;
        save(true);
        close();
        toast('Progress erased.');
        showHome();
      },
    }, 'Reset progress');
    const { close } = openSheet(
      h('h2', null, 'Settings'),
      h('div', null,
        toggle('set-autox', 'Auto-place ×’s', 'Mark every cell a queen rules out. Leaving it off trains your eye.', 'autoX'),
        toggle('set-clash', 'Show clashes', 'Stripe rows, columns and colors where two queens break a rule.', 'conflicts'),
        toggle('set-nudge', 'Nudge when stuck', 'After 45 seconds without a move, the hint button glows. Nothing pops up.', 'nudges'),
        toggle('set-rules', 'Rules reminder', 'Show the three rules under the board for your first games.', 'rules'),
        h('div', { class: 'setting' }, h('label', null, 'Theme'), h('span', { style: 'grid-row:1 / span 2;grid-column:2' }, seg))),
      h('div', { class: 'row', style: 'justify-content:space-between;align-items:center' },
        h('small', { class: 'muted' }, 'Everything stays in this browser.'), reset),
      h('button', { class: 'btn primary', onclick: () => close() }, 'Done'));
  }

  // ---------- recommendations ----------
  function cleared(d) {
    const recent = store.stats.solved[d].slice(-5);
    return recent.filter((e) => e.hints === 0).length >= 3;
  }
  function recommendDifficulty() {
    return DIFF_ORDER.find((d) => !cleared(d)) || 'expert';
  }
  function totalSolved() {
    return DIFF_ORDER.reduce((a, d) => a + store.stats.solved[d].length, 0);
  }

  // ---------- home ----------
  function gameGlyph(kind) {
    if (kind === 'queens') {
      return `<svg viewBox="0 0 30 30" aria-hidden="true"><rect width="10" height="10" fill="#bba3e2"/><rect x="10" width="10" height="10" fill="#bba3e2"/><rect x="20" width="10" height="10" fill="#ffc992"/><rect y="10" width="10" height="10" fill="#96beff"/><rect x="10" y="10" width="10" height="10" fill="#b3dfa0"/><rect x="20" y="10" width="10" height="10" fill="#ffc992"/><rect y="20" width="10" height="10" fill="#96beff"/><rect x="10" y="20" width="10" height="10" fill="#b3dfa0"/><rect x="20" y="20" width="10" height="10" fill="#ff7b60"/><g transform="translate(11 11) scale(.34)" fill="#1a1c21"><path d="M3 8.3l4.7 4.1L12 5.7l4.3 6.7L21 8.3l-1.8 9.2H4.8z"/><rect x="4.8" y="18.7" width="14.4" height="2.5"/></g></svg>`;
    }
    if (kind === 'sudoku') {
      return `<svg viewBox="0 0 30 30" aria-hidden="true"><rect width="30" height="30" fill="var(--surface-2)"/><path d="M15 0v30M0 10h30M0 20h30" stroke="var(--line)" stroke-width="1"/><text x="7.5" y="8" font-size="7" text-anchor="middle" fill="var(--ink)" font-family="system-ui" font-weight="700">4</text><text x="22.5" y="18" font-size="7" text-anchor="middle" fill="var(--ink)" font-family="system-ui" font-weight="700">1</text><text x="7.5" y="28" font-size="7" text-anchor="middle" fill="var(--ink)" font-family="system-ui" font-weight="700">6</text></svg>`;
    }
    if (kind === 'tango') {
      return `<svg viewBox="0 0 30 30" aria-hidden="true"><rect width="30" height="30" fill="var(--surface-2)"/><circle cx="8.5" cy="8.5" r="5" fill="#f2b632"/><path d="M26 21.5a5 5 0 1 1-4-6.5 4 4 0 0 0 4 6.5z" fill="#5b7bd6"/><path d="M21 4l5 5M4 21l5 5" stroke="var(--line)" stroke-width="1.5"/></svg>`;
    }
    return `<svg viewBox="0 0 30 30" aria-hidden="true"><rect width="30" height="30" fill="var(--surface-2)"/><path d="M5 25V15a5 5 0 0 1 10 0v0a5 5 0 0 0 10 0V5" fill="none" stroke="var(--ink)" stroke-width="2.5" stroke-linecap="round"/><circle cx="5" cy="25" r="2.6" fill="var(--gold)"/></svg>`;
  }

  function showHome() {
    genToken++;
    clearView();
    app.view = 'home';
    const wrap = h('div', { class: 'wrap' });

    wrap.append(h('div', { class: 'bar' },
      h('div', { class: 'brand', style: 'flex:1' },
        h('span', { class: 'brand-mark', html: ICON.crown }),
        h('h1', null, 'Puzzle Dojo')),
      h('button', { class: 'icon-btn', 'aria-label': 'Settings', html: ICON.gear, onclick: () => openSettings() })));

    const games = h('div', { class: 'games', role: 'list' });
    for (const [kind, name] of [['queens', 'Queens'], ['sudoku', 'Mini Sudoku'], ['tango', 'Tango'], ['wend', 'Wend']]) {
      const active = kind === 'queens';
      games.append(h('button', {
        class: 'game-tile', role: 'listitem', 'aria-current': active ? 'true' : null, 'aria-disabled': active ? null : 'true',
        onclick: () => { if (!active) toast(`${name} is next on the build list. Queens is ready now.`); },
      }, h('span', { class: 'glyph', html: gameGlyph(kind) }), h('b', null, name), h('small', null, active ? 'Play' : 'Coming soon')));
    }
    wrap.append(games);

    // Continue
    const gm = store.game;
    if (gm && !gm.done) {
      wrap.append(h('button', { class: 'card', style: 'border:0;text-align:left;cursor:pointer', onclick: openGame },
        h('div', { class: 'section-head' },
          h('div', null, h('span', { class: 'eyebrow' }, 'Continue'), h('h2', null, gameSubtitle(gm))),
          h('span', { class: 'timer' }, fmt(gm.elapsed)))));
    }

    // Daily
    const today = todayKey();
    const dc = dailyConfig(today);
    const solvedToday = store.stats.daily[today];
    const st = store.stats;
    const liveStreak = st.lastDaily === today || st.lastDaily === shiftDay(today, -1) ? st.streak : 0;
    wrap.append(h('div', { class: 'card daily' },
      h('div', { class: 'cal', 'aria-hidden': 'true' },
        h('small', null, prettyDate(today, { month: 'short' }).toUpperCase()),
        h('b', null, String(new Date().getDate()))),
      h('div', { class: 'meta' },
        h('span', { class: 'eyebrow' }, 'Today’s board'),
        h('b', null, `${DIFFS[dc.difficulty].label} · ${dc.size}×${dc.size}`),
        solvedToday ? h('span', { class: 'muted', style: 'font-size:.85rem' }, `Solved in ${fmt(solvedToday.ms)}`)
          : h('span', { class: 'muted', style: 'font-size:.85rem' }, 'Same board all day, new one tomorrow'),
        liveStreak ? h('span', { class: 'streak', html: ICON.flame + `<span>${liveStreak}-day streak</span>` }) : null),
      h('button', {
        class: 'btn ' + (solvedToday ? 'ghost' : 'primary'),
        onclick: () => {
          if (gm && !gm.done && gm.kind === 'daily' && gm.date === today) { openGame(); return; }
          startGame({ ...dc, kind: 'daily', date: today, practice: !!solvedToday });
        },
      }, solvedToday ? 'Replay' : 'Play')));

    // Coach's pick
    wrap.append(coachPickCard());

    // Play
    const sizes = h('div', { class: 'sizes', role: 'group', 'aria-label': 'Board size' }, h('span', { class: 'muted', style: 'font-size:.85rem;margin-right:4px' }, 'Size'));
    for (const v of ['auto', 6, 7, 8, 9, 10]) {
      sizes.append(h('button', {
        class: 'chip', 'aria-pressed': String(store.size) === String(v) ? 'true' : 'false',
        onclick: (e) => {
          store.size = v; save();
          sizes.querySelectorAll('.chip').forEach((c) => c.setAttribute('aria-pressed', c === e.currentTarget ? 'true' : 'false'));
          diffs.replaceChildren(...diffCards());
        },
      'aria-label': v === 'auto' ? 'Size by level' : `${v} by ${v}`,
      }, v === 'auto' ? 'Auto' : String(v)));
    }
    const diffs = h('div', { class: 'diffs' });
    function diffCards() {
      const rec = recommendDifficulty();
      return DIFF_ORDER.map((d) => {
        const D = DIFFS[d];
        const list = store.stats.solved[d];
        const best = list.length ? Math.min(...list.map((e) => e.ms)) : null;
        const pips = h('span', { class: 'pips', 'aria-hidden': 'true' });
        for (let k = 1; k <= 4; k++) pips.append(h('i', { class: k <= D.pips ? 'on' : '' }));
        const size = store.size === 'auto' ? `${D.sizes[0]}×${D.sizes[0]} to ${D.sizes[1]}×${D.sizes[1]}` : `${store.size}×${store.size}`;
        return h('button', { class: 'diff', onclick: () => startGame({ difficulty: d, size: sizeFor(d), kind: 'free' }) },
          pips, h('b', null, D.label), h('small', null, D.blurb), h('span', { class: 'size' }, size),
          h('span', { class: 'best' }, best != null ? `Best ${fmt(best)} · ${list.length} solved` : 'Not played yet'),
          d === rec && totalSolved() > 0 ? h('span', { class: 'tag' }, 'Up next') : null);
      });
    }
    diffs.append(...diffCards());
    wrap.append(h('section', { class: 'section' },
      h('div', { class: 'section-head' }, h('h2', null, 'Play'), h('span', { class: 'eyebrow' }, 'Unlimited boards')),
      diffs, sizes));

    // Train
    const techs = h('div', { class: 'techs' });
    for (const t of TECHS) {
      techs.append(h('button', { class: 'tech-row', onclick: () => openLesson(t.id, 'home') },
        h('span', null, h('b', null, t.name), ' ', h('span', { class: 'tier' }, TIER_LABEL[t.tier])),
        meter(t.id),
        h('small', null, t.short)));
    }
    wrap.append(h('section', { class: 'section' },
      h('div', { class: 'section-head' }, h('h2', null, 'Train your eye'), h('span', { class: 'eyebrow' }, 'Short drills')),
      h('div', { class: 'card', style: 'padding:6px 12px' }, techs)));

    // Progress
    wrap.append(progressSection());
    wrap.append(h('p', { class: 'muted', style: 'font-size:.8rem;text-align:center' }, 'Works offline. Progress stays in this browser.'));
    root.append(wrap);
  }

  function coachPickCard() {
    const total = totalSolved();
    const rec = recommendDifficulty();
    const card = h('div', { class: 'card pick' }, h('span', { class: 'crown-badge', html: ICON.bulb }));
    const body = h('div');
    card.append(body);
    if (!total) {
      body.append(
        h('span', { class: 'eyebrow' }, 'Coach’s pick'),
        h('h2', null, 'Start with an Easy board'),
        h('p', { class: 'muted' }, 'The rules fit in one line under the board. When you get stuck, the hint button gives a nudge first, then the technique, then the answer.'),
        h('div', { class: 'row' },
          h('button', { class: 'btn primary small', onclick: () => startGame({ difficulty: 'easy', size: 6, kind: 'free' }) }, 'Play a 6×6'),
          h('button', { class: 'btn ghost small', onclick: () => openLesson('reach', 'home') }, 'Rules drill')));
      return card;
    }
    const recent = store.stats.solved[rec].slice(-5);
    const solo = recent.filter((e) => e.hints === 0).length;
    const prevD = DIFF_ORDER[DIFF_ORDER.indexOf(rec) - 1];
    let why;
    if (prevD && cleared(prevD) && !recent.length) why = `You solved 3 of your last 5 ${DIFFS[prevD].label} boards without hints. Time to move up.`;
    else why = `Solve 3 of your last 5 without hints to move up. You’re at ${solo} of ${recent.length || 0}.`;
    body.append(h('span', { class: 'eyebrow' }, 'Coach’s pick'), h('h2', null, `Up next: ${DIFFS[rec].label}`), h('p', { class: 'muted' }, why));

    // A weak technique, or the next one to learn for this level.
    const weak = TECHS.map((t) => ({ t, m: masteryOf(t.id) }))
      .filter((x) => x.m.n >= 2 && x.m.score < 0.6)
      .sort((a, b) => a.m.score - b.m.score)[0];
    const unseen = TECHS.find((t) => t.tier >= 2 && t.tier <= DIFFS[rec].tier && masteryOf(t.id).n === 0);
    const actions = h('div', { class: 'row' },
      h('button', { class: 'btn primary small', onclick: () => startGame({ difficulty: rec, size: sizeFor(rec), kind: 'free' }) }, `Play ${DIFFS[rec].label}`));
    if (weak) {
      body.append(h('p', null, `Sharpen ${weak.t.name}: ${Math.round(weak.m.score * 100)}% on your own lately.`));
      actions.append(h('button', { class: 'btn gold small', onclick: () => openLesson(weak.t.id, 'home') }, `Drill ${weak.t.name}`));
    } else if (unseen) {
      body.append(h('p', null, `${DIFFS[rec].label} boards lean on ${unseen.name}. A two-minute drill first will pay off.`));
      actions.append(h('button', { class: 'btn gold small', onclick: () => openLesson(unseen.id, 'home') }, `Learn ${unseen.name}`));
    }
    body.append(actions);
    return card;
  }

  function progressSection() {
    const st = store.stats;
    const total = totalSolved();
    const today = todayKey();
    const liveStreak = st.lastDaily === today || st.lastDaily === shiftDay(today, -1) ? st.streak : 0;
    const mastered = TECHS.filter((t) => ['Strong', 'Mastered'].includes(masteryOf(t.id).label)).length;
    const stats = h('div', { class: 'stats' },
      h('div', null, h('b', null, String(total)), h('small', null, 'Solved')),
      h('div', null, h('b', null, String(liveStreak)), h('small', null, 'Day streak')),
      h('div', null, h('b', null, String(st.bestStreak)), h('small', null, 'Best streak')),
      h('div', null, h('b', null, `${mastered}/${TECHS.length}`), h('small', null, 'Techniques strong')));
    const rows = DIFF_ORDER.map((d) => {
      const list = st.solved[d];
      const recent = list.slice(-10);
      const best = list.length ? fmt(Math.min(...list.map((e) => e.ms))) : '–';
      const avg = recent.length ? fmt(recent.reduce((a, e) => a + e.ms, 0) / recent.length) : '–';
      const solo = list.length ? Math.round((list.filter((e) => e.hints === 0).length / list.length) * 100) + '%' : '–';
      return h('tr', null, h('td', null, DIFFS[d].label), h('td', null, String(list.length)), h('td', null, best), h('td', null, avg), h('td', null, solo));
    });
    return h('section', { class: 'section' },
      h('div', { class: 'section-head' }, h('h2', null, 'Your progress')),
      h('div', { class: 'card' }, stats,
        h('div', { class: 'table-wrap' },
          h('table', { class: 'times' },
            h('thead', null, h('tr', null, h('th', null, 'Level'), h('th', null, 'Solved'), h('th', null, 'Best'), h('th', null, 'Avg (last 10)'), h('th', null, 'No hints'))),
            h('tbody', null, rows)))));
  }

  // ---------- lessons & drills ----------
  let drillToken = 0;
  function makeDrillAsync(tech) {
    const token = ++drillToken;
    const rand = Q.mulberry32((Math.random() * 2 ** 32) >>> 0);
    return new Promise((resolve) => {
      let tries = 0;
      function slice() {
        if (token !== drillToken) { resolve(null); return; }
        const until = Date.now() + 24;
        while (Date.now() < until) {
          tries++;
          const size = tech === 'group' || tech === 'whatif' ? 7 + (tries % 2) : 6 + (tries % 3);
          const d = Q.drillAttempt(tech, size, rand);
          if (d) { resolve(d); return; }
          if (tries > 60000) { resolve(null); return; }
        }
        setTimeout(slice, 0);
      }
      slice();
    });
  }

  function openLesson(id, from) {
    genToken++;
    clearView();
    app.view = 'lesson';
    app.lessonId = id;
    const t = TECH[id];
    const idx = TECHS.indexOf(t);
    const back = () => (from === 'game' && store.game ? openGame() : showHome());

    const bar = h('div', { class: 'wrap', style: 'padding-block:0;gap:0;position:sticky;top:env(safe-area-inset-top,0px);z-index:5;background:var(--bg)' },
      h('div', { class: 'bar' },
        h('button', { class: 'icon-btn', 'aria-label': from === 'game' ? 'Back to your board' : 'Back to home', html: ICON.back, onclick: back }),
        h('div', { class: 'title' }, h('b', null, t.name), h('span', null, `${TIER_LABEL[t.tier]} technique · ${idx + 1} of ${TECHS.length}`)),
        meter(id)));

    const text = h('div', { class: 'lesson-text' },
      ...t.what.map((p) => h('p', null, p)),
      h('h3', null, 'How to spot it'),
      h('ul', null, ...t.spot.map((s) => h('li', null, s))));

    const taskEl = h('p', { style: 'font-weight:600' });
    const countEl = h('span', { class: 'count' });
    const feedback = h('p', { class: 'feedback', 'aria-live': 'polite' });
    const boardSlot = h('div', { class: 'board-wrap' });
    const showBtn = h('button', { class: 'btn' }, 'Show me');
    const nextBtn = h('button', { class: 'btn primary', html: '<span>New position</span>' + ICON.next });
    const drillCard = h('section', { class: 'card' },
      h('div', { class: 'drill-task' }, h('span', { class: 'eyebrow' }, 'Practice'), countEl),
      taskEl, boardSlot, feedback, h('div', { class: 'row' }, showBtn, nextBtn));

    const nav = h('div', { class: 'lesson-nav' },
      idx > 0 ? h('button', { class: 'btn ghost small', onclick: () => openLesson(TECHS[idx - 1].id, from), html: ICON.back + `<span>${TECHS[idx - 1].name}</span>` }) : h('span'),
      idx < TECHS.length - 1 ? h('button', { class: 'btn ghost small', onclick: () => openLesson(TECHS[idx + 1].id, from), html: `<span>${TECHS[idx + 1].name}</span>` + ICON.next }) : h('span'));

    root.append(bar, h('div', { class: 'wrap' }, text, drillCard, nav));

    let dr = null; // current drill
    const isElim = id !== 'single';
    const findOne = id === 'whatif';

    function render() {
      if (!dr) return;
      const ov = dr.overlay || {};
      dr.board.render({ marks: dr.marks, ...ov });
      if (id === 'single' || findOne) countEl.textContent = dr.done ? 'Done' : 'Find 1';
      else countEl.textContent = dr.done ? 'Done' : `${dr.found.size} of ${dr.d.elim.length} found`;
    }

    function finish(revealed, stepForText) {
      dr.done = true;
      const n = dr.d.puzzle.n;
      if (!dr.example && !dr.recorded) {
        dr.recorded = true;
        const v = revealed ? 0 : dr.mistakes === 0 ? 1 : dr.mistakes <= 2 ? 0.5 : 0.25;
        pushMastery(id, v);
        save();
        const m = bar.querySelector('.meter');
        if (m) m.replaceWith(meter(id));
      }
      let html;
      if (id === 'reach') {
        html = 'A queen rules out its row, its column, its color and the 8 cells around it.';
        dr.overlay = { focus: new Set([dr.d.focus, ...dr.d.elim]), gx: revealed ? new Set(dr.d.elim.filter((e) => !dr.found.has(e))) : new Set() };
      } else {
        const st = stepForText || dr.d.first;
        html = describe(n, st).detail;
        const ov = stepOverlay(dr.g, st, 3);
        if (!revealed) {
          // Keep found answers as real marks; only ghost what's still missing.
          ov.gx = new Set([...ov.gx].filter((e) => dr.marks[e] === 0));
          if (st.place >= 0 && dr.marks[st.place] === 2) ov.gq = new Set();
        }
        if (revealed && findOne) ov.gx = new Set([st.elim[0]]);
        dr.overlay = ov;
      }
      feedback.className = 'feedback ' + (revealed ? '' : 'good');
      feedback.innerHTML = (dr.example ? '' : revealed ? '' : (dr.mistakes ? 'Got it. ' : 'Spot on. ')) + html;
      showBtn.disabled = true;
      render();
    }

    function load(example) {
      dr = null;
      boardSlot.replaceChildren(h('div', { class: 'board loading', style: 'max-width:30rem' }, h('div', { class: 'loading-text' }, h('span', { class: 'spinner' }), 'Setting up a position…')));
      feedback.textContent = '';
      feedback.className = 'feedback';
      countEl.textContent = '';
      showBtn.disabled = true;
      taskEl.textContent = example ? 'Example: here is what this looks like on a real board.' : t.task;
      makeDrillAsync(id).then((d) => {
        if (!d || app.view !== 'lesson' || app.lessonId !== id) return;
        const g = Q.makeGeom(d.puzzle.n, d.puzzle.regions);
        const board = makeBoard(d.puzzle, {
          interactive: true,
          onTap: (i) => attempt(i),
          onPaintStart: () => (dr && !dr.done && isElim && !findOne ? 'x' : null),
          onPaint: (i) => attempt(i, true),
          onPaintEnd: () => { if (dr) dr.dragMiss = false; },
          onKbd: () => render(),
        });
        dr = { d, g, board, marks: marksFromState(d.cand, d.queen), found: new Set(), mistakes: 0, done: false, example, dragMiss: false };
        if (id === 'reach') dr.overlay = { focus: new Set([d.focus, ...g.attacks[d.focus]]) };
        boardSlot.replaceChildren(board.el);
        showBtn.disabled = false;
        if (example) { finish(true); feedback.className = 'feedback'; }
        render();
      });
    }

    function attempt(i, dragging) {
      if (!dr || dr.done) return;
      if (dr.marks[i] !== 0) return;
      const d = dr.d;
      if (id === 'single') {
        if (d.places.includes(i)) {
          dr.marks[i] = 2;
          dr.board.flash(i, true);
          finish(false, d.steps.find((s) => s.place === i));
        } else miss(i, 'Not forced yet. Look for a row, column or color with exactly one open cell.');
        return;
      }
      if (findOne) {
        if (d.elim.includes(i)) {
          dr.marks[i] = 1;
          dr.board.flash(i, true);
          finish(false, d.steps.find((s) => s.elim[0] === i));
        } else miss(i, 'A queen there doesn’t hit a dead end quickly. Try a cell beside a small color.');
        return;
      }
      if (d.elim.includes(i)) {
        dr.marks[i] = 1;
        dr.found.add(i);
        dr.board.flash(i, true);
        feedback.className = 'feedback';
        feedback.textContent = '';
        if (dr.found.size === d.elim.length) finish(false);
        else render();
      } else {
        if (dragging) { if (dr.dragMiss) return; dr.dragMiss = true; }
        const msg = id === 'reach'
          ? 'That cell isn’t in the queen’s row, column or color, and doesn’t touch it.'
          : `That cell can’t be ruled out with ${t.name} yet. A queen could still go there as far as this rule knows.`;
        miss(i, msg);
      }
    }
    function miss(i, msg) {
      dr.mistakes++;
      dr.board.flash(i, false);
      feedback.className = 'feedback bad';
      feedback.textContent = msg;
    }

    showBtn.onclick = () => { if (dr && !dr.done) finish(true); };
    nextBtn.onclick = () => load(false);
    const seen = masteryOf(id).n > 0;
    load(!seen);
    if (!seen) {
      nextBtn.innerHTML = '<span>Your turn</span>' + ICON.next;
      nextBtn.onclick = () => { nextBtn.innerHTML = '<span>New position</span>' + ICON.next; nextBtn.onclick = () => load(false); load(false); };
    }
    cleanup.push(() => { drillToken++; });
  }

  // ---------- boot ----------
  function start(data) {
    applyTheme();
    data = data || {};
    if (data.view === 'lesson' && data.lessonId && TECH[data.lessonId]) openLesson(data.lessonId, 'home');
    else if (data.view === 'game' && store.game) openGame();
    else showHome();
  }
  const hot = window.claude && window.claude.hot;
  try { if (hot && hot.snapshot) hot.snapshot(() => { save(true); return { view: app.view, lessonId: app.lessonId }; }); } catch (e) { /* not in a viewer */ }
  window.addEventListener('pagehide', () => save(true));
  if (hot && hot.ready) hot.ready(start);
  else start((hot && hot.data) || {});
})();

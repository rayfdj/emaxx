// Queens engine: generation, solution counting and a human-style deduction solver.
//
// Units are numbered 0..n-1 for rows, n..2n-1 for columns, 2n..3n-1 for regions.
// A state holds `cand` (1 = a queen may still go here) and `queen` (1 = queen placed).
// Every technique returns a step: { tech, tier, units, place, elim, cause, ... }.
(function (root) {
  'use strict';

  // ---------- randomness ----------
  function mulberry32(seed) {
    let a = seed >>> 0;
    return function () {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  function hashString(s) {
    let h = 2166136261;
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }
  function shuffle(arr, rand) {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1));
      const t = arr[i]; arr[i] = arr[j]; arr[j] = t;
    }
    return arr;
  }
  function popcount(x) {
    x = x - ((x >>> 1) & 0x55555555);
    x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
    return (((x + (x >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
  }

  // ---------- geometry ----------
  function makeGeom(n, regions) {
    const N2 = n * n;
    const units = [];
    for (let r = 0; r < n; r++) { const u = []; for (let c = 0; c < n; c++) u.push(r * n + c); units.push(u); }
    for (let c = 0; c < n; c++) { const u = []; for (let r = 0; r < n; r++) u.push(r * n + c); units.push(u); }
    for (let k = 0; k < n; k++) units.push([]);
    for (let i = 0; i < N2; i++) units[2 * n + regions[i]].push(i);
    const attacks = [];
    for (let i = 0; i < N2; i++) {
      const r = (i / n) | 0, c = i % n, list = [];
      for (let j = 0; j < N2; j++) {
        if (j === i) continue;
        const r2 = (j / n) | 0, c2 = j % n;
        if (r2 === r || c2 === c || regions[j] === regions[i] ||
            (Math.abs(r2 - r) <= 1 && Math.abs(c2 - c) <= 1)) list.push(j);
      }
      attacks.push(list);
    }
    return {
      n, N2, regions, units, attacks,
      row: (i) => (i / n) | 0,
      col: (i) => i % n,
      reg: (i) => regions[i],
      unitsOf: (i) => [(i / n) | 0, n + (i % n), 2 * n + regions[i]],
    };
  }

  // ---------- state ----------
  function newState(g) {
    return { cand: new Uint8Array(g.N2).fill(1), queen: new Uint8Array(g.N2), placed: new Uint8Array(3 * g.n) };
  }
  function cloneState(s) {
    return { cand: s.cand.slice(), queen: s.queen.slice(), placed: s.placed.slice() };
  }
  function placeQueen(g, s, i) {
    s.queen[i] = 1;
    s.cand[i] = 0;
    for (const u of g.unitsOf(i)) s.placed[u] = 1;
    for (const a of g.attacks[i]) s.cand[a] = 0;
  }
  function applyStep(g, s, step) {
    if (step.place >= 0) placeQueen(g, s, step.place);
    for (const e of step.elim) s.cand[e] = 0;
  }
  function cands(g, s, u) {
    const out = [];
    for (const i of g.units[u]) if (s.cand[i]) out.push(i);
    return out;
  }
  function emptyUnit(g, s) {
    for (let u = 0; u < 3 * g.n; u++) {
      if (s.placed[u]) continue;
      let any = false;
      for (const i of g.units[u]) if (s.cand[i]) { any = true; break; }
      if (!any) return u;
    }
    return -1;
  }
  function queenCount(s) { let k = 0; for (const q of s.queen) k += q; return k; }

  // Build a state from what a player has on the board: 0 empty, 1 X, 2 queen.
  // Cells attacked by a placed queen count as ruled out even when not marked.
  function stateFromMarks(g, marks) {
    const s = newState(g);
    for (let i = 0; i < g.N2; i++) if (marks[i] === 1) s.cand[i] = 0;
    for (let i = 0; i < g.N2; i++) if (marks[i] === 2) placeQueen(g, s, i);
    return s;
  }

  // Rebuild a full state from cand/queen arrays (e.g. a solver snapshot).
  function stateFrom(g, cand, queen) {
    const s = { cand: Uint8Array.from(cand), queen: Uint8Array.from(queen), placed: new Uint8Array(3 * g.n) };
    for (let i = 0; i < g.N2; i++) if (s.queen[i]) for (const u of g.unitsOf(i)) s.placed[u] = 1;
    return s;
  }

  // Unit order for scanning: regions first (most visual), then rows, then columns.
  function unitOrder(n) {
    const out = [];
    for (let k = 0; k < n; k++) out.push(2 * n + k);
    for (let k = 0; k < 2 * n; k++) out.push(k);
    return out;
  }

  // ---------- tier 1: last spot ----------
  function findSingle(g, s) {
    for (const u of unitOrder(g.n)) {
      if (s.placed[u]) continue;
      const cs = cands(g, s, u);
      if (cs.length === 1) return { tech: 'single', tier: 1, units: [u], place: cs[0], elim: [], cause: cs };
    }
    return null;
  }

  // ---------- tier 2: confinement ----------
  // A color whose open cells sit in one line claims that line.
  function findColorLine(g, s) {
    const n = g.n;
    for (let k = 0; k < n; k++) {
      const R = 2 * n + k;
      if (s.placed[R]) continue;
      const cs = cands(g, s, R);
      if (cs.length < 2) continue;
      for (const axis of ['row', 'col']) {
        const line = g[axis](cs[0]);
        if (!cs.every((i) => g[axis](i) === line)) continue;
        const L = axis === 'row' ? line : n + line;
        const elim = cands(g, s, L).filter((i) => g.regions[i] !== k);
        if (elim.length) return { tech: 'colorLine', tier: 2, units: [R, L], place: -1, elim, cause: cs };
      }
    }
    return null;
  }
  // A line whose open cells sit in one color claims that color.
  function findLineColor(g, s) {
    const n = g.n;
    for (let L = 0; L < 2 * n; L++) {
      if (s.placed[L]) continue;
      const cs = cands(g, s, L);
      if (cs.length < 2) continue;
      const k = g.regions[cs[0]];
      if (!cs.every((i) => g.regions[i] === k)) continue;
      const R = 2 * n + k;
      const elim = cands(g, s, R).filter((i) => !g.units[L].includes(i));
      if (elim.length) return { tech: 'lineColor', tier: 2, units: [L, R], place: -1, elim, cause: cs };
    }
    return null;
  }
  function findConfine(g, s) {
    return findColorLine(g, s) || findLineColor(g, s);
  }

  // ---------- tier 3: squeeze ----------
  // A cell that every option of some unit would rule out cannot hold a queen.
  function findSqueeze(g, s) {
    const list = [];
    for (const u of unitOrder(g.n)) {
      if (s.placed[u]) continue;
      const cs = cands(g, s, u);
      if (cs.length >= 2) list.push({ u, cs });
    }
    list.sort((a, b) => a.cs.length - b.cs.length);
    const count = new Uint8Array(g.N2);
    for (const { u, cs } of list) {
      count.fill(0);
      for (const c of cs) for (const a of g.attacks[c]) count[a]++;
      const elim = [];
      for (let i = 0; i < g.N2; i++) {
        if (s.cand[i] && count[i] === cs.length && !g.units[u].includes(i)) elim.push(i);
      }
      if (elim.length) return { tech: 'squeeze', tier: 3, units: [u], place: -1, elim, cause: cs };
    }
    return null;
  }

  // ---------- tier 4: groups ----------
  // k colors packed into k lines own those lines; k lines covered by k colors own those colors.
  function combos(arr, k, fn) {
    const idx = [];
    function rec(start) {
      if (idx.length === k) return fn(idx.map((x) => arr[x]));
      for (let i = start; i < arr.length; i++) {
        idx.push(i);
        const r = rec(i + 1);
        idx.pop();
        if (r) return r;
      }
      return null;
    }
    return rec(0);
  }
  function findGroups(g, s) {
    const n = g.n;
    const openRegions = [];
    for (let k = 0; k < n; k++) if (!s.placed[2 * n + k]) openRegions.push(k);
    const openLines = { row: [], col: [] };
    for (let r = 0; r < n; r++) if (!s.placed[r]) openLines.row.push(r);
    for (let c = 0; c < n; c++) if (!s.placed[n + c]) openLines.col.push(c);

    const regCands = {};
    for (const k of openRegions) regCands[k] = cands(g, s, 2 * n + k);
    const lineMask = { row: {}, col: {} };
    for (const k of openRegions) {
      let rm = 0, cm = 0;
      for (const i of regCands[k]) { rm |= 1 << g.row(i); cm |= 1 << g.col(i); }
      lineMask.row[k] = rm; lineMask.col[k] = cm;
    }
    const maxK = openRegions.length - 1;
    for (let size = 2; size <= maxK; size++) {
      for (const axis of ['row', 'col']) {
        const hit = combos(openRegions, size, (group) => {
          let m = 0;
          for (const k of group) m |= lineMask[axis][k];
          if (popcount(m) !== size) return null;
          const lines = [];
          for (let x = 0; x < n; x++) if (m & (1 << x)) lines.push(x);
          const elim = [];
          for (const x of lines) {
            const L = axis === 'row' ? x : n + x;
            for (const i of cands(g, s, L)) if (!group.includes(g.regions[i])) elim.push(i);
          }
          if (!elim.length) return null;
          const cause = [];
          for (const k of group) cause.push(...regCands[k]);
          return {
            tech: 'group', tier: 4, axis, kind: 'colors',
            units: group.map((k) => 2 * n + k).concat(lines.map((x) => (axis === 'row' ? x : n + x))),
            place: -1, elim, cause,
          };
        });
        if (hit) return hit;
      }
      for (const axis of ['row', 'col']) {
        const lines = openLines[axis];
        if (size > lines.length - 1) continue;
        const hit = combos(lines, size, (group) => {
          const regs = new Set();
          const cause = [];
          for (const x of group) {
            const L = axis === 'row' ? x : n + x;
            for (const i of cands(g, s, L)) { regs.add(g.regions[i]); cause.push(i); }
          }
          if (regs.size !== size) return null;
          const elim = [];
          for (const k of regs) {
            for (const i of regCands[k]) if (!group.includes(g[axis](i))) elim.push(i);
          }
          if (!elim.length) return null;
          return {
            tech: 'group', tier: 4, axis, kind: 'lines',
            units: group.map((x) => (axis === 'row' ? x : n + x)).concat([...regs].map((k) => 2 * n + k)),
            place: -1, elim, cause,
          };
        });
        if (hit) return hit;
      }
    }
    return null;
  }

  // ---------- tier 5: what if ----------
  // Suppose a queen here, follow last spots and confinements, and reach a unit with no room.
  function tryQueen(g, s, i, limit) {
    const t = cloneState(s);
    placeQueen(g, t, i);
    const chain = [];
    for (let iter = 0; iter <= limit; iter++) {
      const dead = emptyUnit(g, t);
      if (dead >= 0) return { chain, dead };
      if (iter === limit) break;
      const step = findSingle(g, t) || findConfine(g, t);
      if (!step) break;
      chain.push(step);
      applyStep(g, t, step);
    }
    return null;
  }
  function findWhatIf(g, s, limit) {
    limit = limit || 12;
    let best = null;
    for (let i = 0; i < g.N2; i++) {
      if (!s.cand[i]) continue;
      const res = tryQueen(g, s, i, best ? best.chain.length : limit);
      if (res && (!best || res.chain.length < best.chain.length)) {
        best = { cell: i, chain: res.chain, dead: res.dead };
        if (best.chain.length <= 1) break;
      }
    }
    if (!best) return null;
    return {
      tech: 'whatif', tier: 5, units: [best.dead], place: -1, elim: [best.cell], cause: [best.cell],
      chain: best.chain, dead: best.dead,
    };
  }

  const FINDERS = [
    { tech: 'single', tier: 1, fn: findSingle },
    { tech: 'colorLine', tier: 2, fn: findColorLine },
    { tech: 'lineColor', tier: 2, fn: findLineColor },
    { tech: 'squeeze', tier: 3, fn: findSqueeze },
    { tech: 'group', tier: 4, fn: findGroups },
    { tech: 'whatif', tier: 5, fn: findWhatIf },
  ];
  const TIER = {};
  for (const f of FINDERS) TIER[f.tech] = f.tier;

  function findStep(g, s, maxTier) {
    maxTier = maxTier || 5;
    for (const f of FINDERS) {
      if (f.tier > maxTier) break;
      const step = f.fn(g, s);
      if (step) return step;
    }
    return null;
  }

  // Every cell where an imagined queen runs into a dead end, with its chain.
  function whatIfCells(g, s, limit) {
    const out = new Map();
    for (let i = 0; i < g.N2; i++) {
      if (!s.cand[i]) continue;
      const res = tryQueen(g, s, i, limit || 12);
      if (res) out.set(i, res);
    }
    return out;
  }

  // Everything one technique can do in this position right now: the union of
  // its eliminations (applied one after another) or, for last spots, every
  // forced cell. Drills accept any of these.
  function allOfTech(g, s, tech) {
    const f = FINDERS.find((x) => x.tech === tech);
    const out = { first: null, elim: [], places: [], steps: [] };
    if (tech === 'single') {
      for (const u of unitOrder(g.n)) {
        if (s.placed[u]) continue;
        const cs = cands(g, s, u);
        if (cs.length === 1 && !out.places.includes(cs[0])) {
          const step = { tech: 'single', tier: 1, units: [u], place: cs[0], elim: [], cause: cs };
          if (!out.first) out.first = step;
          out.places.push(cs[0]);
          out.steps.push(step);
        }
      }
      return out;
    }
    if (tech === 'whatif') {
      for (const [i, res] of whatIfCells(g, s)) {
        const step = { tech: 'whatif', tier: 5, units: [res.dead], place: -1, elim: [i], cause: [i], chain: res.chain, dead: res.dead };
        if (!out.first || res.chain.length < out.first.chain.length) out.first = step;
        out.elim.push(i);
        out.steps.push(step);
      }
      return out;
    }
    const t = cloneState(s);
    const seen = new Set();
    for (let guard = 0; guard < 100; guard++) {
      const step = f.fn(g, t);
      if (!step) break;
      if (!out.first) out.first = step;
      out.steps.push(step);
      for (const e of step.elim) { if (!seen.has(e)) { seen.add(e); out.elim.push(e); } t.cand[e] = 0; }
    }
    return out;
  }

  // ---------- logical solve / grading ----------
  function solve(g, s0, opts) {
    opts = opts || {};
    const s = cloneState(s0 || newState(g));
    const steps = [];
    const counts = { single: 0, colorLine: 0, lineColor: 0, squeeze: 0, group: 0, whatif: 0 };
    let maxTier = 0;
    while (queenCount(s) < g.n) {
      if (emptyUnit(g, s) >= 0) return { ok: false, steps, counts, maxTier, broken: true };
      const step = findStep(g, s, opts.maxTier || 5);
      if (!step) return { ok: false, steps, counts, maxTier, state: s };
      if (opts.snapshots) step.before = { cand: s.cand.slice(), queen: s.queen.slice() };
      steps.push(step);
      counts[step.tech]++;
      if (step.tier > maxTier) maxTier = step.tier;
      applyStep(g, s, step);
    }
    return { ok: true, steps, counts, maxTier, state: s };
  }

  const DIFFICULTIES = ['easy', 'medium', 'hard', 'expert'];
  function classify(res) {
    if (!res.ok) return null;
    if (res.maxTier <= 2) return 'easy';
    if (res.maxTier === 3) return 'medium';
    if (res.maxTier === 4) return 'hard';
    return 'expert';
  }

  // ---------- generation ----------
  function randomSolution(n, rand) {
    const sol = new Array(n).fill(-1);
    const used = new Uint8Array(n);
    function rec(r) {
      if (r === n) return true;
      const cols = shuffle([...Array(n).keys()], rand);
      for (const c of cols) {
        if (used[c]) continue;
        if (r > 0 && Math.abs(sol[r - 1] - c) <= 1) continue;
        used[c] = 1; sol[r] = c;
        if (rec(r + 1)) return true;
        used[c] = 0;
      }
      sol[r] = -1;
      return false;
    }
    rec(0);
    return sol;
  }

  const DIRS4 = [[1, 0], [-1, 0], [0, 1], [0, -1]];
  function growRegions(n, sol, rand) {
    const N2 = n * n;
    const reg = new Int8Array(N2).fill(-1);
    const weight = [];
    const compact = [];
    for (let r = 0; r < n; r++) {
      reg[r * n + sol[r]] = r;
      const x = rand();
      weight.push(0.15 + x * x * 3.5);
      compact.push(rand() < 0.5);
    }
    let left = N2 - n;
    while (left > 0) {
      // frontier per region
      const front = Array.from({ length: n }, () => []);
      for (let i = 0; i < N2; i++) {
        if (reg[i] >= 0) continue;
        const r = (i / n) | 0, c = i % n;
        const seen = new Set();
        for (const [dr, dc] of DIRS4) {
          const r2 = r + dr, c2 = c + dc;
          if (r2 < 0 || c2 < 0 || r2 >= n || c2 >= n) continue;
          const k = reg[r2 * n + c2];
          if (k >= 0 && !seen.has(k)) { seen.add(k); front[k].push(i); }
        }
      }
      let total = 0;
      for (let k = 0; k < n; k++) if (front[k].length) total += weight[k];
      let x = rand() * total, pick = -1;
      for (let k = 0; k < n; k++) {
        if (!front[k].length) continue;
        x -= weight[k];
        if (x <= 0) { pick = k; break; }
      }
      if (pick < 0) for (let k = n - 1; k >= 0; k--) if (front[k].length) { pick = k; break; }
      let cells = front[pick];
      if (compact[pick]) {
        // Prefer cells that touch the region on more sides, for blob-like shapes.
        let best = -1;
        const scored = cells.map((i) => {
          const r = (i / n) | 0, c = i % n;
          let s = 0;
          for (const [dr, dc] of DIRS4) {
            const r2 = r + dr, c2 = c + dc;
            if (r2 >= 0 && c2 >= 0 && r2 < n && c2 < n && reg[r2 * n + c2] === pick) s++;
          }
          if (s > best) best = s;
          return s;
        });
        cells = cells.filter((_, j) => scored[j] === best);
      }
      reg[cells[Math.floor(rand() * cells.length)]] = pick;
      left--;
    }
    return reg;
  }

  function findSolutions(n, reg, limit) {
    const out = [];
    const sol = new Array(n);
    function rec(r, usedCols, usedRegs, prev) {
      if (out.length >= limit) return;
      if (r === n) { out.push(sol.slice()); return; }
      for (let c = 0; c < n; c++) {
        if (usedCols & (1 << c)) continue;
        if (prev >= 0 && Math.abs(prev - c) <= 1) continue;
        const k = reg[r * n + c];
        if (usedRegs & (1 << k)) continue;
        sol[r] = c;
        rec(r + 1, usedCols | (1 << c), usedRegs | (1 << k), c);
        if (out.length >= limit) return;
      }
    }
    rec(0, 0, 0, -1);
    return out;
  }

  function regionConnectedWithout(n, reg, k, skip) {
    const cells = [];
    for (let i = 0; i < n * n; i++) if (reg[i] === k && i !== skip) cells.push(i);
    if (!cells.length) return false;
    const seen = new Set([cells[0]]);
    const stack = [cells[0]];
    while (stack.length) {
      const i = stack.pop();
      const r = (i / n) | 0, c = i % n;
      for (const [dr, dc] of DIRS4) {
        const r2 = r + dr, c2 = c + dc;
        if (r2 < 0 || c2 < 0 || r2 >= n || c2 >= n) continue;
        const j = r2 * n + c2;
        if (j !== skip && reg[j] === k && !seen.has(j)) { seen.add(j); stack.push(j); }
      }
    }
    return seen.size === cells.length;
  }

  // Reshape regions until the intended solution is the only one.
  function makeUnique(n, reg, sol, rand) {
    const solCells = new Set(sol.map((c, r) => r * n + c));
    for (let iter = 0; iter < 60; iter++) {
      const sols = findSolutions(n, reg, 2);
      if (sols.length === 1) return true;
      const alt = sols.find((x) => x.some((c, r) => c !== sol[r]));
      if (!alt) return false;
      const diff = shuffle(alt.map((c, r) => r * n + c).filter((i) => !solCells.has(i)), rand);
      let moved = false;
      for (const i of diff) {
        const k = reg[i];
        if (!regionConnectedWithout(n, reg, k, i)) continue;
        const r = (i / n) | 0, c = i % n;
        const options = [];
        for (const [dr, dc] of DIRS4) {
          const r2 = r + dr, c2 = c + dc;
          if (r2 < 0 || c2 < 0 || r2 >= n || c2 >= n) continue;
          const k2 = reg[r2 * n + c2];
          if (k2 !== k && !options.includes(k2)) options.push(k2);
        }
        if (!options.length) continue;
        reg[i] = options[Math.floor(rand() * options.length)];
        moved = true;
        break;
      }
      if (!moved) return false;
    }
    return false;
  }

  // Renumber regions so colors are not tied to rows.
  function permuteRegions(n, reg, rand) {
    const perm = shuffle([...Array(n).keys()], rand);
    const out = new Int8Array(n * n);
    for (let i = 0; i < n * n; i++) out[i] = perm[reg[i]];
    return out;
  }

  // One generation attempt. Returns a puzzle or null.
  function attempt(n, rand, want) {
    const sol = randomSolution(n, rand);
    let reg = growRegions(n, sol, rand);
    if (!makeUnique(n, reg, sol, rand)) return null;
    reg = permuteRegions(n, reg, rand);
    const g = makeGeom(n, reg);
    const res = solve(g);
    const diff = classify(res);
    if (!diff) return null;
    if (want && diff !== want) return null;
    return {
      n,
      regions: Array.from(reg),
      solution: sol,
      difficulty: diff,
      counts: res.counts,
      maxTier: res.maxTier,
      steps: res.steps.length,
    };
  }

  // Synchronous generator with an attempt cap. `seed` makes it deterministic.
  function generate(opts) {
    const rand = opts.rand || mulberry32(opts.seed != null ? opts.seed : (Math.random() * 2 ** 32) >>> 0);
    const max = opts.maxAttempts || 5000;
    for (let a = 0; a < max; a++) {
      const p = attempt(opts.size, rand, opts.difficulty);
      if (p) { p.attempts = a + 1; return p; }
    }
    return null;
  }

  // Time-sliced generator: yields to the event loop so the page stays responsive.
  function generateAsync(opts) {
    const rand = opts.rand || mulberry32(opts.seed != null ? opts.seed : (Math.random() * 2 ** 32) >>> 0);
    const max = opts.maxAttempts || 20000;
    let a = 0;
    return new Promise((resolve) => {
      function slice() {
        const until = Date.now() + (opts.sliceMs || 24);
        while (Date.now() < until && a < max) {
          a++;
          const p = attempt(opts.size, rand, opts.difficulty);
          if (p) { p.attempts = a; resolve(p); return; }
        }
        if (a >= max) { resolve(null); return; }
        setTimeout(slice, 0);
      }
      slice();
    });
  }

  // ---------- drills ----------
  // One attempt at a practice position for `tech`: a real puzzle paused right
  // before the solver first needed that technique (so nothing simpler works).
  function drillAttempt(tech, size, rand) {
    const p = attempt(size, rand, null);
    if (!p) return null;
    if (tech !== 'reach' && TIER[tech] > p.maxTier) return null;
    const g = makeGeom(p.n, p.regions);
    const res = solve(g, null, { snapshots: true });
    if (tech === 'reach') {
      const opts = res.steps.filter((st, j) => j >= 1 && st.place >= 0);
      if (!opts.length) return null;
      const st = opts[Math.floor(rand() * opts.length)];
      const c = st.place;
      const s = stateFrom(g, st.before.cand, st.before.queen);
      const targets = g.attacks[c].filter((a) => s.cand[a]);
      if (targets.length < 3 || targets.length > 14) return null;
      s.queen[c] = 1; s.cand[c] = 0;
      for (const u of g.unitsOf(c)) s.placed[u] = 1;
      return { puzzle: p, tech, cand: Array.from(s.cand), queen: Array.from(s.queen), elim: targets, places: [], focus: c, first: null };
    }
    const opts = res.steps.filter((st) => st.tech === tech);
    if (!opts.length) return null;
    const st = opts[Math.floor(rand() * opts.length)];
    const s = stateFrom(g, st.before.cand, st.before.queen);
    const all = allOfTech(g, s, tech);
    if (tech === 'single') { if (!all.places.length) return null; }
    else if (!all.elim.length || (tech !== 'whatif' && all.elim.length > 8)) return null;
    return { puzzle: p, tech, cand: Array.from(s.cand), queen: Array.from(s.queen), elim: all.elim, places: all.places, first: all.first, steps: all.steps };
  }

  const api = {
    mulberry32, hashString, shuffle,
    makeGeom, newState, cloneState, placeQueen, applyStep, cands, emptyUnit, queenCount, stateFromMarks, stateFrom,
    findSingle, findColorLine, findLineColor, findConfine, findSqueeze, findGroups, findWhatIf, tryQueen,
    findStep, allOfTech, whatIfCells, FINDERS, TIER, unitOrder,
    solve, classify, DIFFICULTIES,
    randomSolution, growRegions, findSolutions, makeUnique, attempt, generate, generateAsync, drillAttempt,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.QueensEngine = api;
})(typeof self !== 'undefined' ? self : this);

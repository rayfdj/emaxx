const Q = require('../src/engine.js');
const assert = require('assert');
let puzzles = 0, steps = 0, tierHist = {}, tier1only = 0, midChecks = 0;
for (const n of [5,6,7,8,9,10]) {
  const rand = Q.mulberry32(99 + n);
  for (let i = 0; i < 400; i++) {
    const sol = Q.randomSolution(n, rand);
    const reg = Q.growRegions(n, sol, rand);
    if (!Q.makeUnique(n, reg, sol, rand)) continue;
    // regions contiguous + each region has exactly one solution queen
    const g = Q.makeGeom(n, reg);
    for (let k = 0; k < n; k++) {
      const cells = g.units[2*n+k];
      assert(cells.length > 0);
      assert.strictEqual(cells.filter(c => sol[(c/n)|0] === c % n).length, 1);
    }
    assert.strictEqual(Q.findSolutions(n, reg, 3).length, 1);
    const solCells = new Set(sol.map((c, r) => r*n+c));
    const res = Q.solve(g);
    assert(res.ok, 'unsolved');
    puzzles++;
    let max = 0;
    for (const st of res.steps) {
      steps++;
      if (st.place >= 0) assert(solCells.has(st.place), 'bad place ' + st.tech);
      for (const e of st.elim) assert(!solCells.has(e), 'bad elim ' + st.tech);
      max = Math.max(max, st.tier);
    }
    tierHist[max] = (tierHist[max]||0)+1;
    if (max === 1) tier1only++;
    // Mid-game: random correct marks, then findStep must be sound and make progress.
    const marks = new Uint8Array(n*n);
    for (let c = 0; c < n*n; c++) {
      const x = rand();
      if (solCells.has(c)) marks[c] = x < 0.2 ? 2 : 0;
      else marks[c] = x < 0.3 ? 1 : 0;
    }
    const s = Q.stateFromMarks(g, marks);
    const st = Q.findStep(g, s);
    if (Q.queenCount(s) < n) {
      assert(st, 'no step mid-game');
      if (st.place >= 0) assert(solCells.has(st.place));
      for (const e of st.elim) { assert(!solCells.has(e)); assert(s.cand[e]); }
      midChecks++;
    }
  }
}
console.log({puzzles, steps, tierHist, tier1only, midChecks});

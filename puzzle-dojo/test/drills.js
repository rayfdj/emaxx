const Q = require('../src/engine.js');
const assert = require('assert');
for (const tech of ['reach','single','colorLine','lineColor','squeeze','group','whatif']) {
  const rand = Q.mulberry32(7);
  let found = 0, tries = 0; const t0 = Date.now(); const sizes = [];
  while (found < 30 && tries < 200000) {
    tries++;
    const size = tech === 'group' || tech === 'whatif' ? 7 + (tries % 2) : 6 + (tries % 3);
    const d = Q.drillAttempt(tech, size, rand);
    if (!d) continue;
    found++; sizes.push(d.elim.length || d.places.length);
    // soundness of targets
    const sol = new Set(d.puzzle.solution.map((c, r) => r*d.puzzle.n + c));
    for (const e of d.elim) assert(!sol.has(e), tech + ' elim hits solution');
    for (const p of d.places) assert(sol.has(p), tech + ' place not in solution');
    // nothing simpler applies (except reach)
    if (tech !== 'reach') {
      const g = Q.makeGeom(d.puzzle.n, d.puzzle.regions);
      const s = Q.stateFrom(g, d.cand, d.queen);
      const st = Q.findStep(g, s);
      assert(st.tier === Q.TIER[tech], tech + ' simpler step exists: ' + st.tech);
    }
  }
  const ms = Date.now() - t0;
  console.log(tech.padEnd(10), 'found', found, 'tries', tries, 'ms/drill', (ms/found).toFixed(1), 'targets', Math.min(...sizes)+'-'+Math.max(...sizes));
}

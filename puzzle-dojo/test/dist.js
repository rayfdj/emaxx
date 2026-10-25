const Q = require('../src/engine.js');
for (const n of [6,7,8,9,10]) {
  const rand = Q.mulberry32(1234 + n);
  const tally = {easy:0, medium:0, hard:0, expert:0, fail:0, nonunique:0};
  const t0 = Date.now();
  const tries = n >= 9 ? 300 : 500;
  for (let i = 0; i < tries; i++) {
    const sol = Q.randomSolution(n, rand);
    const reg = Q.growRegions(n, sol, rand);
    if (!Q.makeUnique(n, reg, sol, rand)) { tally.nonunique++; continue; }
    const g = Q.makeGeom(n, reg);
    const res = Q.solve(g);
    const d = Q.classify(res);
    if (!d) tally.fail++; else tally[d]++;
  }
  const ms = (Date.now() - t0) / tries;
  console.log(n, JSON.stringify(tally), ms.toFixed(2) + 'ms/attempt');
}

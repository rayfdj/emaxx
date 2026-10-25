const { chromium } = require('playwright');
const path = require('path');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 400, height: 900 } });
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  p.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  await p.goto('file://' + path.resolve(__dirname, '../dist/puzzle-dojo.html'));
  for (const name of ["Queen's reach", 'Last spot', 'Squeeze', 'Groups', 'What if?']) {
    await p.click(`.tech-row:has-text("${name}")`);
    await p.waitForFunction(() => document.querySelector('.drill-task') && !document.querySelector('.board.loading'));
    if (await p.locator('text=Your turn').count()) {
      await p.click('text=Your turn');
      await p.waitForFunction(() => !document.querySelector('.board.loading'));
    }
    let taps = 0;
    for (;;) {
      const done = await p.evaluate(() => document.querySelector('.drill-task .count').textContent === 'Done');
      if (done) break;
      const idx = await p.evaluate(() => [...document.querySelectorAll('.board .cell')].findIndex((c, i) => !c.querySelector('.g') && !c.dataset.tried && (c.dataset.tried = '1')));
      if (idx < 0) break;
      await p.locator('.board .cell').nth(idx).click();
      taps++;
    }
    const fb = await p.locator('.feedback').innerText();
    const meter = await p.locator('.bar .meter').innerText();
    console.log(name.padEnd(14), 'taps', taps, '| meter', meter, '|', fb.slice(0, 110));
    await p.click('.icon-btn[aria-label="Back to home"]');
  }
  console.log('errors:', errors.length ? errors : 'none');
  await b.close();
})();

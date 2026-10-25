// Check every view at phone width for sideways overflow.
const { chromium } = require('playwright');
const path = require('path');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage({ viewport: { width: 360, height: 760 } });
  await p.goto('file://' + path.resolve(__dirname, '../dist/puzzle-dojo.html'));
  const check = async (label) => console.log(label, await p.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth));
  await check('home');
  await p.click('.icon-btn[aria-label="Settings"]'); await p.waitForTimeout(300); await check('settings');
  await p.screenshot({ path: path.resolve(__dirname, '../shots/10-settings.png') });
  await p.click('text=Done');
  await p.click('.diff:has-text("Expert")');
  await p.waitForSelector('.board .grid', { timeout: 15000 }); await p.waitForTimeout(200); await check('expert game');
  const g = await p.evaluate(() => JSON.parse(localStorage.getItem('puzzle-dojo.v1')).game.puzzle);
  console.log('expert size', g.n, 'difficulty', g.difficulty, 'counts', JSON.stringify(g.counts));
  // walk the hint ladder to the end three times
  for (let k = 0; k < 3; k++) {
    await p.click('button:has-text("Hint")');
    await p.waitForTimeout(80);
    while (await p.locator('.coach >> text=Name the technique').count() || await p.locator('.coach >> text=Show me').count()) {
      await p.locator('.coach .row .btn').first().click();
      await p.waitForTimeout(50);
    }
    console.log('  step:', (await p.locator('.coach-text').innerText()).slice(0, 160));
    await p.locator('.coach .btn.primary').click();
    await p.waitForTimeout(300);
  }
  await p.screenshot({ path: path.resolve(__dirname, '../shots/11-expert.png'), fullPage: true });
  // solve it
  const box = await p.locator('.board .grid').boundingBox();
  const n = g.n;
  for (let r = 0; r < n; r++) {
    const i = r * n + g.solution[r];
    for (let t = 0; t < 3; t++) {
      await p.waitForTimeout(240);
      const m = await p.evaluate((i) => JSON.parse(localStorage.getItem('puzzle-dojo.v1')).game.marks[i], i);
      if (m === 2) break;
      await p.mouse.click(box.x + (i % n + 0.5) * box.width / n, box.y + (Math.floor(i / n) + 0.5) * box.height / n);
    }
  }
  await p.waitForSelector('.sheet', { timeout: 5000 });
  await p.waitForTimeout(400);
  await check('debrief');
  await p.screenshot({ path: path.resolve(__dirname, '../shots/12-expert-debrief.png') });
  console.log((await p.locator('.sheet').innerText()).replace(/\n+/g, ' | ').slice(0, 700));
  await b.close();
})();

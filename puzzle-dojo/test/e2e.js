const { chromium } = require('playwright');
const path = require('path');
const url = 'file://' + path.resolve(__dirname, '../dist/puzzle-dojo.html');
const shots = path.resolve(__dirname, '../shots');
(async () => {
  const browser = await chromium.launch();
  const errors = [];
  async function newPage(opts) {
    const ctx = await browser.newContext(opts);
    const page = await ctx.newPage();
    page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') errors.push(m.text()); });
    page.on('pageerror', (e) => errors.push('PAGEERROR ' + e.message));
    await page.goto(url);
    await page.waitForTimeout(300);
    return page;
  }
  const page = await newPage({ viewport: { width: 400, height: 860 }, deviceScaleFactor: 2, hasTouch: true });
  await page.screenshot({ path: shots + '/01-home.png', fullPage: true });

  // Start an easy 6x6 via coach pick
  await page.click('text=Play a 6×6');
  await page.waitForSelector('.board .grid');
  await page.waitForTimeout(200);
  await page.screenshot({ path: shots + '/02-game.png', fullPage: true });

  const game = async () => { await page.waitForTimeout(260); return page.evaluate(() => JSON.parse(localStorage.getItem('puzzle-dojo.v1')).game); };
  let g = await game();
  const n = g.puzzle.n;
  const box = await page.locator('.board .grid').boundingBox();
  const cellXY = (i) => ({ x: box.x + ((i % n) + 0.5) * box.width / n, y: box.y + (Math.floor(i / n) + 0.5) * box.height / n });

  // Hint ladder
  await page.click('button:has-text("Hint")');
  await page.waitForTimeout(150);
  console.log('hint1:', await page.locator('.coach-text').innerText());
  await page.click('text=Name the technique');
  console.log('hint2:', await page.locator('.coach-text').innerText());
  await page.click('.coach >> text=Show me');
  console.log('hint3:', await page.locator('.coach-text').innerText());
  await page.waitForTimeout(400); await page.screenshot({ path: shots + '/03-hint.png', fullPage: false });
  const applyBtn = page.locator('.coach .btn.primary');
  await applyBtn.click();
  await page.waitForTimeout(100);

  // Drag to mark X across row 0 cells that aren't solution
  const p0 = cellXY(0), p1 = cellXY(n - 1);
  await page.mouse.move(p0.x, p0.y); await page.mouse.down();
  await page.mouse.move(p1.x, p1.y, { steps: 8 }); await page.mouse.up();
  g = await game();
  console.log('after drag marks row0:', g.marks.slice(0, n).join(''));
  // Undo the drag
  await page.click('button:has-text("Undo")');
  g = await game();
  console.log('after undo row0:', g.marks.slice(0, n).join(''));

  // Wrong queen to trigger clash
  const sol = g.puzzle.solution;
  const wrong = (sol[0] + 1) % n; // row 0 wrong col
  await page.mouse.click(cellXY(wrong).x, cellXY(wrong).y);
  await page.mouse.click(cellXY(wrong).x, cellXY(wrong).y);
  await page.click('button:has-text("Hint")');
  console.log('fix hint:', await page.locator('.coach-text').innerText());
  await page.click('.coach >> text=Show me');
  await page.screenshot({ path: shots + '/04-fix.png', fullPage: true });
  await page.click('.coach .btn.primary');

  // Solve: place all queens
  g = await game();
  for (let r = 0; r < n; r++) {
    const i = r * n + sol[r];
    let m = (await game()).marks[i];
    let guard = 0;
    while (m !== 2 && guard++ < 3) { await page.mouse.click(cellXY(i).x, cellXY(i).y); m = (await game()).marks[i]; }
  }
  await page.waitForTimeout(1300);
  await page.screenshot({ path: shots + '/05-debrief.png', fullPage: false });
  console.log('debrief:', (await page.locator('.sheet').innerText()).replace(/\n+/g, ' | ').slice(0, 600));
  await page.click('.sheet >> text=Home');
  await page.waitForTimeout(200);
  await page.screenshot({ path: shots + '/06-home-after.png', fullPage: true });

  // Lessons: every technique, example then a drill
  for (const name of ["Queen's reach", 'Last spot', 'Color in a line', 'Line in a color', 'Squeeze', 'Groups', 'What if?']) {
    await page.click(`.tech-row:has-text("${name}")`);
    await page.waitForSelector('.drill-task');
    await page.waitForSelector('.lesson-text');
    await page.waitForFunction(() => !document.querySelector('.board.loading'), null, { timeout: 8000 });
    const fb = await page.locator('.feedback').innerText();
    console.log('lesson', name, '| example:', fb.slice(0, 140));
    if (name === 'Squeeze' || name === 'What if?') await page.screenshot({ path: shots + `/07-lesson-${name.replace(/\W/g, '')}.png`, fullPage: true });
    if (await page.locator('text=Your turn').count()) await page.click('text=Your turn');
    else await page.click('text=New position');
    await page.waitForFunction(() => !document.querySelector('.board.loading'), null, { timeout: 8000 });
    console.log('   task:', await page.locator('.drill-task').innerText(), '|', (await page.locator('.card p').first().innerText()).slice(0, 80));
    await page.click('button:has-text("Back to home"), .icon-btn[aria-label="Back to home"]');
    await page.waitForTimeout(100);
  }

  // Dark mode screenshots
  const dark = await newPage({ viewport: { width: 1200, height: 900 }, colorScheme: 'dark' });
  await dark.screenshot({ path: shots + '/08-dark-home.png', fullPage: false });
  await dark.click('.diff:has-text("Medium")');
  await dark.waitForSelector('.board .grid');
  await dark.waitForTimeout(300);
  await dark.click('button:has-text("Hint")');
  await dark.waitForTimeout(400); await dark.screenshot({ path: shots + '/09-dark-game.png', fullPage: false });

  // Overflow check
  for (const p of [page, dark]) {
    const ov = await p.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
    console.log('horizontal overflow:', ov);
  }
  console.log('ERRORS:', errors.length ? errors : 'none');
  await browser.close();
})();

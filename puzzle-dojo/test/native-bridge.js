// Runs the iOS page in Chromium with a stand-in for the Swift bridge, to check
// that saves, haptics and the theme go through the bridge and that a saved
// game comes back on the next launch.
const { chromium } = require('playwright');
const path = require('path');
const assert = require('assert');
const url = 'file://' + path.resolve(__dirname, '../ios/PuzzleDojo/Web/index.html');

async function launch(browser, saved) {
  const ctx = await browser.newContext({ viewport: { width: 393, height: 852 }, hasTouch: true });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('request', (r) => { if (!r.url().startsWith('file:')) errors.push('network request: ' + r.url()); });
  await page.addInitScript((saved) => {
    window.__posted = [];
    window.__dojoNative = { saved };
    window.webkit = { messageHandlers: { dojo: { postMessage: (m) => window.__posted.push(m) } } };
  }, saved);
  await page.goto(url);
  await page.waitForSelector('h1');
  const posted = () => page.evaluate(() => window.__posted);
  return { page, posted, errors };
}

(async () => {
  const browser = await chromium.launch();
  const a = await launch(browser, null);
  assert(await a.page.evaluate(() => document.documentElement.classList.contains('native')), 'native class');
  assert.deepStrictEqual((await a.posted())[0], { type: 'theme', value: 'system' });
  assert(await a.page.locator('text=Progress stays on this device.').count(), 'device wording');

  await a.page.click('text=Play a 6×6');
  await a.page.waitForSelector('.board .grid');
  await a.page.waitForTimeout(400);
  let msgs = await a.posted();
  const firstSave = msgs.filter((m) => m.type === 'save').pop();
  assert(firstSave, 'a save was posted');
  const game = JSON.parse(firstSave.data).game;
  assert.strictEqual(game.puzzle.n, 6);
  assert.strictEqual(await a.page.evaluate(() => localStorage.getItem('puzzle-dojo.v1')), null, 'localStorage untouched');

  // Place the first row's queen (two taps): one light haptic.
  const n = 6, box = await a.page.locator('.board .grid').boundingBox();
  const tap = (i) => a.page.mouse.click(box.x + ((i % n) + 0.5) * box.width / n, box.y + (Math.floor(i / n) + 0.5) * box.height / n);
  await tap(game.puzzle.solution[0]); await tap(game.puzzle.solution[0]);
  await a.page.waitForTimeout(400);
  msgs = await a.posted();
  assert.deepStrictEqual(msgs.filter((m) => m.type === 'haptic').map((m) => m.style), ['light']);
  // A second queen right beside it clashes: warning haptic.
  const beside = game.puzzle.solution[0] + (game.puzzle.solution[0] % n === n - 1 ? -1 : 1);
  await tap(beside); await tap(beside);
  await a.page.waitForTimeout(400);
  msgs = await a.posted();
  assert.deepStrictEqual(msgs.filter((m) => m.type === 'haptic').map((m) => m.style), ['light', 'warning']);
  const lastSave = msgs.filter((m) => m.type === 'save').pop().data;
  assert.deepStrictEqual(a.errors, []);

  // "Relaunch" with what the shell stored: the game is offered again.
  const b = await launch(browser, lastSave);
  assert(await b.page.locator('.eyebrow:text-is("Continue")').count(), 'continue card');
  await b.page.click('.card:has-text("Continue")');
  await b.page.waitForSelector('.board .grid');
  assert.strictEqual(await b.page.locator('.board .g.q').count(), 2, 'both queens restored');

  // Theme changes reach the shell.
  await b.page.click('.icon-btn[aria-label="Settings"]');
  await b.page.click('.seg >> text=Dark');
  assert.deepStrictEqual((await b.posted()).filter((m) => m.type === 'theme').pop(), { type: 'theme', value: 'dark' });
  assert.deepStrictEqual(b.errors, []);
  console.log('native bridge ok');
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });

// Renders design/icon.svg to the 1024px app icon. Run: node design/render-icon.js
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
(async () => {
  const svg = fs.readFileSync(path.join(__dirname, 'icon.svg'), 'utf8');
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1024, height: 1024 }, deviceScaleFactor: 1 });
  await page.setContent(`<body style="margin:0">${svg}</body>`);
  const out = path.join(__dirname, '..', 'PuzzleDojo', 'Assets.xcassets', 'AppIcon.appiconset', 'AppIcon.png');
  await page.screenshot({ path: out, clip: { x: 0, y: 0, width: 1024, height: 1024 } });
  await browser.close();
  console.log('wrote', out);
})();

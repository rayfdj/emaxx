// Inlines the engine, app and styles into two single-file pages:
//   dist/puzzle-dojo.html  a complete document that runs offline from disk
//   dist/artifact.html     the same content without the document skeleton
const fs = require('fs');
const path = require('path');
const read = (f) => fs.readFileSync(path.join(__dirname, 'src', f), 'utf8');
const css = read('style.css');
const engine = read('engine.js');
const app = read('app.js');
const fonts = '<link rel="preconnect" href="https://fonts.googleapis.com">\n' +
  '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n' +
  '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,700;12..96,800&display=swap">';
const body = `<div id="app"></div>\n<script>\n${engine}</script>\n<script>\n${app}</script>\n`;
const head = `<title>Puzzle Dojo</title>\n<meta name="claude:viewport-fit-sides" content="cover">\n${fonts}\n<style>\n${css}</style>\n`;
const reset = ':root{padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}body{margin:0}img{max-width:100%}[hidden]{display:none!important}';
const full = `<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n` +
  `<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n` +
  `<meta name="apple-mobile-web-app-capable" content="yes">\n<meta name="mobile-web-app-capable" content="yes">\n` +
  `<style>${reset}</style>\n${head}</head>\n<body>\n${body}</body>\n</html>\n`;
fs.mkdirSync(path.join(__dirname, 'dist'), { recursive: true });
fs.writeFileSync(path.join(__dirname, 'dist', 'puzzle-dojo.html'), full);
fs.writeFileSync(path.join(__dirname, 'dist', 'artifact.html'), head + body);
console.log('built', (full.length / 1024).toFixed(1) + ' KB');

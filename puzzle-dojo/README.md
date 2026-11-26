# Puzzle Dojo

Offline versions of the daily LinkedIn puzzle games, starting with **Queens**,
with a coach built in so you keep getting better at them.

Open [`dist/puzzle-dojo.html`](dist/puzzle-dojo.html) in any browser. It is one
self-contained file with no network dependencies (the display font falls back to
the system font offline). Progress is saved in that browser's local storage.

## Queens

Place one queen in every row, every column and every color region. Queens may
not touch, not even diagonally. Tap once for ×, twice for a queen; drag to mark
many ×'s. Keyboard: arrows, Space, `x`, `q`, Backspace, Ctrl/Cmd+Z, `h` for a hint.

- Unlimited boards from 6×6 to 10×10 at four levels, plus a daily board (the
  same for everyone on a given date) with a streak.
- Every board has exactly one solution and can be solved by logic alone.

### How the coach teaches

Everything runs on one human-style solver (`src/engine.js`) that knows these
techniques, from simplest to hardest:

| Technique | Level | Idea |
|---|---|---|
| Queen's reach | Basics | A queen rules out its row, column, color and 8 neighbors. |
| Last spot | Basics | A row, column or color with one open cell takes its queen there. |
| Color in a line | Core | A color confined to one row/column claims that line. |
| Line in a color | Core | A row/column confined to one color claims that color. |
| Squeeze | Intermediate | A cell that every option of a unit would rule out is out. |
| Groups | Advanced | N colors packed into N lines own those lines (and the reverse). |
| What if? | Expert | Imagine a queen, follow the forced moves, find a dead end. |

The solver uses it for:

- **Difficulty.** A board's level is the hardest technique it needs
  (Easy ≤ Core, Medium = Squeeze, Hard = Groups, Expert = What if?).
- **Hints in three steps.** First a nudge that only spotlights where to look,
  then the technique's name and rule, then the exact cells and the reasoning.
  Mistakes are caught first (a wrong queen, or an × on a queen's cell).
- **Debrief.** After each solve: your time against your best and average,
  every technique the board needed and whether you found it alone, with a
  nudge or with help, the board's key moment replayed, and one tip.
- **Drills.** Each technique has a lesson and endless practice positions taken
  from real boards at the exact moment that technique becomes necessary.
- **Mastery and coach's pick.** Per-technique mastery from your games and
  drills; the home screen suggests when to move up a level and which technique
  to sharpen.

Nothing pops up uninvited. The only unprompted signal is a glow on the hint
button after 45 seconds without a move, and it can be turned off.

## iOS app

[`ios/PuzzleDojo.xcodeproj`](ios/PuzzleDojo.xcodeproj) is a native shell around
the same game: a SwiftUI app whose one screen is a web view showing the bundled
page (`ios/PuzzleDojo/Web/index.html`). It makes no network requests. The shell
adds what a page can't do by itself:

- progress is stored on the device (UserDefaults) rather than in web storage;
- haptics when a queen lands, when two queens clash, and on a solve;
- the in-app light/dark choice also drives the status bar and launch screen.

To put it on your iPhone you need a Mac with Xcode 16 or later:

1. Open `ios/PuzzleDojo.xcodeproj`.
2. Select the PuzzleDojo target, then Signing & Capabilities, and pick your
   Team. A free Apple ID works. If Xcode says the bundle identifier is taken,
   change `io.github.rayfdj.PuzzleDojo` to anything unique.
3. Plug in the phone, choose it as the run destination and press Run. The first
   time, iOS asks you to turn on Developer Mode (Settings, Privacy & Security)
   and to trust your developer certificate (Settings, General, VPN & Device
   Management).

With a free Apple ID the install expires after 7 days; run it from Xcode again
to renew, and your progress is kept. A paid Apple Developer membership removes
that limit and allows TestFlight and the App Store.

After changing anything in `src/`, run `npm run build` to refresh the bundled
page. The icon is drawn in `ios/design/icon.svg` (`node ios/design/render-icon.js`).
A debug build launched with `-selftest` starts a board and writes
`Documents/selftest.json`; the `Puzzle Dojo iOS` workflow uses that to check the
app in a simulator on every change.

## Development

```
npm run build          # writes dist/puzzle-dojo.html and the page bundled into the iOS app
npm test               # solver soundness + drill checks (Node only)
npm run test:browser   # Playwright: full game, hints, lessons, phone widths, iOS bridge
```

`src/engine.js` (generation, uniqueness, solver), `src/app.js` (UI) and
`src/style.css` are inlined into the single HTML file by `build.js`.

## Next

Mini Sudoku, Tango and Wend are on the home screen as coming soon.

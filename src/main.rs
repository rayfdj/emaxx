#![deny(clippy::unwrap_used)]

use std::io::{self, stdout};
use std::{env, fs};

use crossterm::terminal;

use emaxx::buffer::Buffer;
use emaxx::command::{self, CommandResult};
use emaxx::display::{self, Screen};
use emaxx::keymap::{self, Key};

fn main() -> io::Result<()> {
    // Load a file if given, otherwise start with *scratch*
    let mut buf = match env::args().nth(1) {
        Some(path) => {
            let contents = fs::read_to_string(&path).unwrap_or_else(|_| String::new());
            let name = path.rsplit('/').next().unwrap_or(&path);
            let mut b = Buffer::from_text(name, &contents);
            b.file = Some(path);
            b.set_unmodified();
            b
        }
        None => Buffer::from_text("*scratch*", ""),
    };

    let global_map = keymap::default_global_keymap();
    let mut screen = Screen::new()?;
    let mut stdout = stdout();
    let mut message: Option<String> = None;
    let mut key_buffer: Vec<Key> = Vec::new();

    // Enter raw mode
    terminal::enable_raw_mode()?;
    crossterm::execute!(
        stdout,
        terminal::EnterAlternateScreen,
        crossterm::cursor::Show
    )?;

    // Initial render
    display::scroll_to_cursor(&buf, &mut screen);
    display::render(&mut stdout, &buf, &screen, message.as_deref())?;

    // Main command loop — this is our keyboard.c command_loop
    loop {
        let key = match display::read_key()? {
            Some(k) => k,
            None => {
                // Resize event
                screen.refresh_size()?;
                display::scroll_to_cursor(&buf, &mut screen);
                display::render(&mut stdout, &buf, &screen, message.as_deref())?;
                continue;
            }
        };

        message = None;

        // Key sequence handling: accumulate keys for prefix bindings
        key_buffer.push(key.clone());

        let result = global_map.lookup_seq(&key_buffer);

        match result {
            Ok(Some(cmd)) => {
                // Full key sequence resolved to a command
                let last_char = match key_buffer.last() {
                    Some(Key::Char(c)) => Some(*c),
                    _ => None,
                };
                key_buffer.clear();

                match command::execute(&mut buf, &cmd, last_char) {
                    CommandResult::Ok => {}
                    CommandResult::Quit => break,
                    CommandResult::Error(msg) => message = Some(msg),
                    CommandResult::NeedInput(prompt) => {
                        message = Some(format!("{} [not yet implemented]", prompt));
                    }
                }
            }
            Ok(None) => {
                // Unbound key sequence
                if key_buffer.len() == 1 {
                    // Single unbound key: if it's a printable char, self-insert
                    if let Key::Char(c) = &key_buffer[0] {
                        let c = *c;
                        key_buffer.clear();
                        match command::execute(&mut buf, "self-insert-command", Some(c)) {
                            CommandResult::Ok => {}
                            CommandResult::Error(msg) => message = Some(msg),
                            _ => {}
                        }
                    } else {
                        let desc = format!("{:?} is undefined", &key_buffer[0]);
                        key_buffer.clear();
                        message = Some(desc);
                    }
                } else {
                    let desc = format!("{:?} is undefined", &key_buffer);
                    key_buffer.clear();
                    message = Some(desc);
                }
            }
            Err(()) => {
                // Prefix key: show partial sequence in echo area, wait for more
                message = Some(format!(
                    "{}-",
                    key_buffer
                        .iter()
                        .map(|k| format!("{:?}", k))
                        .collect::<Vec<_>>()
                        .join(" ")
                ));
                // Don't clear key_buffer, keep accumulating
                display::render(&mut stdout, &buf, &screen, message.as_deref())?;
                continue;
            }
        }

        display::scroll_to_cursor(&buf, &mut screen);
        display::render(&mut stdout, &buf, &screen, message.as_deref())?;
    }

    // Clean up
    terminal::disable_raw_mode()?;
    crossterm::execute!(stdout, terminal::LeaveAlternateScreen)?;

    Ok(())
}

use std::io::{self, Write};

use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    style::Print,
    terminal::{self, ClearType},
};

use crate::buffer::Buffer;
use crate::keymap::Key;

/// Terminal dimensions.
pub struct Screen {
    pub rows: u16,
    pub cols: u16,
    /// How many rows are for the text area (excluding status line and echo area).
    pub text_rows: u16,
    /// First visible line (0-based line index into the buffer).
    pub top_line: usize,
}

impl Screen {
    pub fn new() -> io::Result<Self> {
        let (cols, rows) = terminal::size()?;
        Ok(Screen {
            rows,
            cols,
            text_rows: rows.saturating_sub(2), // status line + echo area
            top_line: 0,
        })
    }

    pub fn refresh_size(&mut self) -> io::Result<()> {
        let (cols, rows) = terminal::size()?;
        self.rows = rows;
        self.cols = cols;
        self.text_rows = rows.saturating_sub(2);
        Ok(())
    }
}

/// Draw the buffer contents, status line, and echo area.
pub fn render(
    stdout: &mut io::Stdout,
    buf: &Buffer,
    screen: &Screen,
    message: Option<&str>,
) -> io::Result<()> {
    let text = buf.buffer_string();
    let lines: Vec<&str> = text.split('\n').collect();
    let pt = buf.point();

    // Figure out cursor row/col in the buffer
    let mut cursor_row: usize = 0;
    let mut cursor_col: usize = 0;
    let mut char_count: usize = 1; // 1-based position counter
    'outer: for (row, line) in lines.iter().enumerate() {
        for (col, _ch) in line.chars().enumerate() {
            if char_count == pt {
                cursor_row = row;
                cursor_col = col;
                break 'outer;
            }
            char_count += 1;
        }
        // Account for the newline character
        if char_count == pt {
            // Point is at the newline (end of this line)
            cursor_row = row;
            cursor_col = line.chars().count();
            break;
        }
        char_count += 1; // the newline
    }
    // If point is past all text (end of buffer)
    if char_count <= pt {
        cursor_row = if lines.is_empty() { 0 } else { lines.len() - 1 };
        cursor_col = lines.last().map(|l| l.chars().count()).unwrap_or(0);
    }

    // Draw text rows
    execute!(stdout, cursor::MoveTo(0, 0))?;

    for row in 0..screen.text_rows as usize {
        let line_idx = screen.top_line + row;
        execute!(stdout, cursor::MoveTo(0, row as u16))?;
        execute!(stdout, terminal::Clear(ClearType::CurrentLine))?;
        if line_idx < lines.len() {
            let line = lines[line_idx];
            // Truncate to terminal width
            let display: String = line.chars().take(screen.cols as usize).collect();
            execute!(stdout, Print(&display))?;
        } else {
            // Empty line marker, like Emacs's fringe
            execute!(stdout, Print("~"))?;
        }
    }

    // Status line (second to last row)
    let status_row = screen.rows - 2;
    execute!(stdout, cursor::MoveTo(0, status_row))?;
    execute!(stdout, terminal::Clear(ClearType::CurrentLine))?;

    let modified = if buf.is_modified() { "**" } else { "--" };
    let line_num = buf.line_number_at_pos(pt);
    let col_num = buf.current_column();
    let status = format!(
        " {modified}-  {name}    L{line}C{col}  ({size} chars)",
        modified = modified,
        name = buf.name,
        line = line_num,
        col = col_num,
        size = buf.buffer_size(),
    );
    // Pad/truncate to fill the row, render inverted
    let padded: String = format!("{:<width$}", status, width = screen.cols as usize)
        .chars()
        .take(screen.cols as usize)
        .collect();
    execute!(
        stdout,
        crossterm::style::SetAttribute(crossterm::style::Attribute::Reverse),
        Print(&padded),
        crossterm::style::SetAttribute(crossterm::style::Attribute::Reset),
    )?;

    // Echo area (last row)
    let echo_row = screen.rows - 1;
    execute!(stdout, cursor::MoveTo(0, echo_row))?;
    execute!(stdout, terminal::Clear(ClearType::CurrentLine))?;
    if let Some(msg) = message {
        let truncated: String = msg.chars().take(screen.cols as usize).collect();
        execute!(stdout, Print(truncated))?;
    }

    // Place cursor
    let screen_cursor_row = cursor_row.saturating_sub(screen.top_line) as u16;
    let screen_cursor_col = cursor_col as u16;
    execute!(stdout, cursor::MoveTo(screen_cursor_col, screen_cursor_row))?;

    stdout.flush()?;
    Ok(())
}

/// Ensure the cursor is visible by scrolling if needed.
pub fn scroll_to_cursor(buf: &Buffer, screen: &mut Screen) {
    let pt = buf.point();
    let cursor_line = buf.line_number_at_pos(pt) - 1; // 0-based
    let text_rows = screen.text_rows as usize;

    if cursor_line < screen.top_line {
        screen.top_line = cursor_line;
    } else if cursor_line >= screen.top_line + text_rows {
        screen.top_line = cursor_line - text_rows + 1;
    }
}

/// Read a key event from the terminal and convert it to our Key type.
pub fn read_key() -> io::Result<Option<Key>> {
    loop {
        let ev = event::read()?;
        match ev {
            Event::Key(KeyEvent {
                code, modifiers, ..
            }) => {
                let ctrl = modifiers.contains(KeyModifiers::CONTROL);
                let alt = modifiers.contains(KeyModifiers::ALT);

                let key = match code {
                    KeyCode::Char(c) => match (ctrl, alt) {
                        (true, true) => Key::CtrlAlt(c),
                        (true, false) => Key::Ctrl(c),
                        (false, true) => Key::Alt(c),
                        (false, false) => Key::Char(c),
                    },
                    KeyCode::Enter => Key::Enter,
                    KeyCode::Backspace => Key::Backspace,
                    KeyCode::Delete => Key::Delete,
                    KeyCode::Tab => Key::Tab,
                    KeyCode::Esc => Key::Escape,
                    KeyCode::Up => Key::Up,
                    KeyCode::Down => Key::Down,
                    KeyCode::Left => Key::Left,
                    KeyCode::Right => Key::Right,
                    KeyCode::Home => Key::Home,
                    KeyCode::End => Key::End,
                    KeyCode::PageUp => Key::PageUp,
                    KeyCode::PageDown => Key::PageDown,
                    _ => continue, // ignore other keys for now
                };
                return Ok(Some(key));
            }
            Event::Resize(_, _) => {
                // Caller should refresh screen size
                return Ok(None);
            }
            _ => continue,
        }
    }
}

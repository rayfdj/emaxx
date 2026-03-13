use crate::buffer::Buffer;

/// Result of executing a command. Commands can request higher-level
/// actions from the editor (like quitting, opening the minibuffer, etc).
#[derive(Debug)]
pub enum CommandResult {
    /// Command ran fine, nothing special.
    Ok,
    /// Request to quit the editor.
    Quit,
    /// An error message to display.
    Error(String),
    /// Need to read a string from the minibuffer (prompt, callback name).
    NeedInput(String),
}

/// Dispatch a named command on a buffer. Returns what happened.
///
/// This is the rough equivalent of Emacs's command dispatch — keyboard.c
/// reads keys, looks up the command name, and we execute it here.
pub fn execute(buf: &mut Buffer, command: &str, last_char: Option<char>) -> CommandResult {
    match command {
        // -- Movement --
        "forward-char" => match buf.forward_char(1) {
            Ok(_) => CommandResult::Ok,
            Err(e) => CommandResult::Error(e.to_string()),
        },

        "backward-char" => match buf.forward_char(-1) {
            Ok(_) => CommandResult::Ok,
            Err(e) => CommandResult::Error(e.to_string()),
        },

        "beginning-of-line" => {
            buf.beginning_of_line();
            CommandResult::Ok
        }

        "end-of-line" => {
            buf.end_of_line();
            CommandResult::Ok
        }

        "beginning-of-buffer" => {
            buf.goto_char(buf.point_min());
            CommandResult::Ok
        }

        "end-of-buffer" => {
            buf.goto_char(buf.point_max());
            CommandResult::Ok
        }

        "next-line" => {
            buf.forward_line(1);
            CommandResult::Ok
        }

        "previous-line" => {
            buf.forward_line(-1);
            CommandResult::Ok
        }

        // -- Editing --
        "self-insert-command" => {
            if let Some(c) = last_char {
                buf.insert_char(c);
                CommandResult::Ok
            } else {
                CommandResult::Error("No character to insert".into())
            }
        }

        "newline" => {
            buf.insert_char('\n');
            CommandResult::Ok
        }

        "delete-char" => match buf.delete_char(1) {
            Ok(_) => CommandResult::Ok,
            Err(e) => CommandResult::Error(e.to_string()),
        },

        "backward-delete-char" => match buf.delete_char(-1) {
            Ok(_) => CommandResult::Ok,
            Err(e) => CommandResult::Error(e.to_string()),
        },

        "kill-line" => {
            // Kill from point to end of line (or just the newline if at eol)
            let pt = buf.point();
            if buf.eolp() {
                // At end of line: kill the newline
                match buf.delete_char(1) {
                    Ok(_) => CommandResult::Ok,
                    Err(e) => CommandResult::Error(e.to_string()),
                }
            } else {
                // Kill to end of line
                let old_pt = pt;
                buf.end_of_line();
                let eol = buf.point();
                buf.goto_char(old_pt);
                match buf.delete_region(old_pt, eol) {
                    Ok(_text) => {
                        // TODO: push to kill ring
                        CommandResult::Ok
                    }
                    Err(e) => CommandResult::Error(e.to_string()),
                }
            }
        }

        "undo" => match buf.undo() {
            Ok(()) => CommandResult::Ok,
            Err(e) => CommandResult::Error(e.to_string()),
        },

        "set-mark-command" => {
            let pt = buf.point();
            buf.set_mark(pt);
            CommandResult::Ok
        }

        "keyboard-quit" => {
            buf.deactivate_mark();
            CommandResult::Ok
        }

        // -- Buffer/file operations (stubs for now) --
        "save-buffers-kill-emacs" => CommandResult::Quit,

        "find-file" => CommandResult::NeedInput("Find file: ".into()),

        "save-buffer" => {
            // TODO: actual file saving
            CommandResult::Error("save-buffer not yet implemented".into())
        }

        _ => CommandResult::Error(format!("Unknown command: {}", command)),
    }
}

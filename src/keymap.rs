#![allow(dead_code)]

use std::collections::HashMap;

/// A key event — either a regular character or a special key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Key {
    Char(char),
    Ctrl(char),
    Alt(char),
    CtrlAlt(char),
    // Function/special keys
    Backspace,
    Delete,
    Enter,
    Tab,
    Escape,
    Up,
    Down,
    Left,
    Right,
    Home,
    End,
    PageUp,
    PageDown,
}

impl Key {
    /// Parse an Emacs-style key description like "C-x", "M-f", "C-M-s".
    pub fn parse(desc: &str) -> Option<Key> {
        let parts: Vec<&str> = desc.split('-').collect();
        let mut ctrl = false;
        let mut alt = false;

        for part in parts.iter().take(parts.len() - 1) {
            match *part {
                "C" => ctrl = true,
                "M" => alt = true,
                _ => return None,
            }
        }

        let last = *parts.last()?;
        // Special key names
        match last {
            "RET" | "return" => return Some(Key::Enter),
            "TAB" | "tab" => return Some(Key::Tab),
            "ESC" | "escape" => return Some(Key::Escape),
            "DEL" | "backspace" => return Some(Key::Backspace),
            "delete" => return Some(Key::Delete),
            "up" => return Some(Key::Up),
            "down" => return Some(Key::Down),
            "left" => return Some(Key::Left),
            "right" => return Some(Key::Right),
            "home" => return Some(Key::Home),
            "end" => return Some(Key::End),
            "prior" => return Some(Key::PageUp),
            "next" => return Some(Key::PageDown),
            _ => {}
        }

        // Single character
        if last.len() == 1 {
            let c = last.chars().next()?;
            return Some(match (ctrl, alt) {
                (true, true) => Key::CtrlAlt(c),
                (true, false) => Key::Ctrl(c),
                (false, true) => Key::Alt(c),
                (false, false) => Key::Char(c),
            });
        }

        None
    }
}

/// A key sequence (one or more keys pressed in order, like C-x C-f).
pub type KeySeq = Vec<Key>;

/// The name of a command (string for now, will be a function pointer later).
pub type CommandName = String;

/// What a key binding resolves to.
#[derive(Debug, Clone)]
pub enum Binding {
    /// A leaf command.
    Command(CommandName),
    /// A prefix key leading to a sub-keymap.
    Prefix(Keymap),
}

/// A keymap. Emacs keymaps form a tree: each prefix key opens a sub-keymap.
/// Keymaps can also have a parent for inheritance (like minor-mode → major-mode → global).
#[derive(Debug, Clone)]
pub struct Keymap {
    name: Option<String>,
    bindings: HashMap<Key, Binding>,
    parent: Option<Box<Keymap>>,
}

impl Default for Keymap {
    fn default() -> Self {
        Self::new()
    }
}

impl Keymap {
    pub fn new() -> Self {
        Keymap {
            name: None,
            bindings: HashMap::new(),
            parent: None,
        }
    }

    pub fn with_name(name: &str) -> Self {
        Keymap {
            name: Some(name.to_string()),
            bindings: HashMap::new(),
            parent: None,
        }
    }

    pub fn set_parent(&mut self, parent: Keymap) {
        self.parent = Some(Box::new(parent));
    }

    /// Bind a single key to a command.
    pub fn bind(&mut self, key: Key, command: &str) {
        self.bindings
            .insert(key, Binding::Command(command.to_string()));
    }

    /// Bind a key to a sub-keymap (making it a prefix key).
    pub fn bind_prefix(&mut self, key: Key, sub: Keymap) {
        self.bindings.insert(key, Binding::Prefix(sub));
    }

    /// Bind a full key sequence like ["C-x", "C-f"] to a command.
    pub fn bind_seq(&mut self, keys: &[&str], command: &str) {
        let parsed: Vec<Key> = keys.iter().filter_map(|k| Key::parse(k)).collect();
        if parsed.is_empty() {
            return;
        }
        if parsed.len() == 1 {
            self.bind(parsed[0].clone(), command);
            return;
        }

        // For multi-key sequences, create nested keymaps
        let first = parsed[0].clone();
        let rest: Vec<&str> = keys[1..].to_vec();

        let sub = match self.bindings.get_mut(&first) {
            Some(Binding::Prefix(km)) => {
                km.bind_seq(&rest, command);
                return;
            }
            _ => {
                let mut km = Keymap::new();
                km.bind_seq(&rest, command);
                km
            }
        };
        self.bind_prefix(first, sub);
    }

    /// Look up a single key. Returns the binding if found.
    pub fn lookup(&self, key: &Key) -> Option<&Binding> {
        self.bindings
            .get(key)
            .or_else(|| self.parent.as_ref().and_then(|p| p.lookup(key)))
    }

    /// Look up a full key sequence. Returns the command name if it resolves
    /// to a complete binding, or None if the sequence is unbound.
    /// Returns Err(()) if it's a valid prefix (more keys needed).
    #[allow(clippy::result_unit_err)]
    pub fn lookup_seq(&self, keys: &[Key]) -> Result<Option<CommandName>, ()> {
        if keys.is_empty() {
            return Ok(None);
        }

        match self.lookup(&keys[0]) {
            None => Ok(None), // unbound
            Some(Binding::Command(cmd)) => {
                if keys.len() == 1 {
                    Ok(Some(cmd.clone()))
                } else {
                    Ok(None) // extra keys after a command binding = unbound
                }
            }
            Some(Binding::Prefix(sub)) => {
                if keys.len() == 1 {
                    Err(()) // prefix: need more keys
                } else {
                    sub.lookup_seq(&keys[1..])
                }
            }
        }
    }
}

/// Build the default global keymap with basic Emacs bindings.
pub fn default_global_keymap() -> Keymap {
    let mut map = Keymap::with_name("global");

    // Movement
    map.bind(Key::Ctrl('f'), "forward-char");
    map.bind(Key::Ctrl('b'), "backward-char");
    map.bind(Key::Ctrl('n'), "next-line");
    map.bind(Key::Ctrl('p'), "previous-line");
    map.bind(Key::Ctrl('a'), "beginning-of-line");
    map.bind(Key::Ctrl('e'), "end-of-line");
    map.bind(Key::Ctrl('v'), "scroll-down");
    map.bind(Key::Alt('v'), "scroll-up");
    map.bind(Key::Alt('<'), "beginning-of-buffer");
    map.bind(Key::Alt('>'), "end-of-buffer");
    map.bind(Key::Alt('f'), "forward-word");
    map.bind(Key::Alt('b'), "backward-word");
    map.bind(Key::Right, "forward-char");
    map.bind(Key::Left, "backward-char");
    map.bind(Key::Up, "previous-line");
    map.bind(Key::Down, "next-line");
    map.bind(Key::Home, "beginning-of-line");
    map.bind(Key::End, "end-of-line");

    // Editing
    map.bind(Key::Ctrl('d'), "delete-char");
    map.bind(Key::Backspace, "backward-delete-char");
    map.bind(Key::Delete, "delete-char");
    map.bind(Key::Enter, "newline");
    map.bind(Key::Ctrl('k'), "kill-line");
    map.bind(Key::Ctrl('y'), "yank");
    map.bind(Key::Alt('w'), "kill-ring-save");
    map.bind(Key::Ctrl('w'), "kill-region");
    map.bind(Key::Ctrl('/'), "undo");
    map.bind(Key::Tab, "indent");

    // Buffer/file (prefix keys)
    map.bind_seq(&["C-x", "C-f"], "find-file");
    map.bind_seq(&["C-x", "C-s"], "save-buffer");
    map.bind_seq(&["C-x", "C-c"], "save-buffers-kill-emacs");
    map.bind_seq(&["C-x", "b"], "switch-to-buffer");
    map.bind_seq(&["C-x", "k"], "kill-buffer");
    map.bind_seq(&["C-x", "o"], "other-window");
    map.bind_seq(&["C-x", "0"], "delete-window");
    map.bind_seq(&["C-x", "1"], "delete-other-windows");
    map.bind_seq(&["C-x", "2"], "split-window-below");
    map.bind_seq(&["C-x", "3"], "split-window-right");

    // Meta/misc
    map.bind(Key::Ctrl('g'), "keyboard-quit");
    map.bind(Key::Ctrl('l'), "recenter");
    map.bind(Key::Ctrl('s'), "isearch-forward");
    map.bind(Key::Ctrl('r'), "isearch-backward");
    map.bind_seq(&["C-x", "u"], "undo");

    // Mark
    map.bind(Key::Ctrl(' '), "set-mark-command");

    map
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_keys() {
        assert_eq!(Key::parse("a"), Some(Key::Char('a')));
        assert_eq!(Key::parse("C-x"), Some(Key::Ctrl('x')));
        assert_eq!(Key::parse("M-f"), Some(Key::Alt('f')));
        assert_eq!(Key::parse("C-M-s"), Some(Key::CtrlAlt('s')));
        assert_eq!(Key::parse("RET"), Some(Key::Enter));
        assert_eq!(Key::parse("TAB"), Some(Key::Tab));
    }

    #[test]
    fn single_key_lookup() {
        let map = default_global_keymap();
        match map.lookup(&Key::Ctrl('f')) {
            Some(Binding::Command(cmd)) => assert_eq!(cmd, "forward-char"),
            _ => panic!("expected forward-char"),
        }
    }

    #[test]
    fn prefix_key_lookup() {
        let map = default_global_keymap();
        // C-x should be a prefix
        match map.lookup(&Key::Ctrl('x')) {
            Some(Binding::Prefix(_)) => {}
            other => panic!("expected prefix, got {:?}", other),
        }

        // C-x C-f should resolve to find-file
        let keys = vec![Key::Ctrl('x'), Key::Ctrl('f')];
        match map.lookup_seq(&keys) {
            Ok(Some(cmd)) => assert_eq!(cmd, "find-file"),
            other => panic!("expected find-file, got {:?}", other),
        }
    }

    #[test]
    fn unbound_key() {
        let map = default_global_keymap();
        assert!(map.lookup(&Key::Ctrl('z')).is_none());
    }

    #[test]
    fn parent_keymap_inheritance() {
        let mut parent = Keymap::new();
        parent.bind(Key::Ctrl('a'), "parent-cmd");

        let mut child = Keymap::new();
        child.set_parent(parent);
        child.bind(Key::Ctrl('b'), "child-cmd");

        // Child binding works
        match child.lookup(&Key::Ctrl('b')) {
            Some(Binding::Command(cmd)) => assert_eq!(cmd, "child-cmd"),
            _ => panic!("child binding missing"),
        }

        // Parent binding inherited
        match child.lookup(&Key::Ctrl('a')) {
            Some(Binding::Command(cmd)) => assert_eq!(cmd, "parent-cmd"),
            _ => panic!("parent binding not inherited"),
        }
    }
}

use super::*;
use crate::lisp::alloc::RootedVec;

// keyboard.c:READ_KEY_ELTS and keyremap. Each cursor visits the live map
// once per input event, retaining a reached prefix across subsequent reads.
const READ_KEY_ELTS: usize = 30;

#[derive(Clone, Copy, Default)]
struct KeyRemap {
    start: usize,
    end: usize,
}

pub(crate) struct KeySequenceReader {
    keys: RootedVec<Value>,
    raw: RootedVec<Value>,
    // Three (parent, current) pairs, binding, prompt, original case,
    // events for which a non-text mouse prefix has already been generated,
    // and read_key_sequence_cmd.
    roots: RootedVec<Value>,
    remaps: [KeyRemap; 3],
    read: usize,
    first_unbound: usize,
    original_uppercase_position: Option<usize>,
    shift_translated: bool,
    // Positions of events read_char reread from `unread-command-events'
    // after the first key: add_command_key never saw them, until a replay
    // re-adds the whole sequence.
    omitted: Vec<usize>,
    // Whether the event just read is one of those.
    fresh_omitted: bool,
    // read_key_sequence's keys_start and echo_start: this-command-keys'
    // length and the echo's when the sequence began, which a replay
    // returns to.
    keys_start: usize,
    echo_start: usize,
    // read_key_sequence's used_mouse_menu_history: whether the key read
    // into each position came from a mouse menu.
    menu_keys: [bool; READ_KEY_ELTS + 1],
}

impl KeySequenceReader {
    pub(crate) fn new(
        interp: &mut Interpreter,
        prompt: Value,
        env: &mut Env,
    ) -> Result<Self, LispError> {
        let mut roots = RootedVec::with_capacity(11);
        for name in [
            cached_symbol!("input-decode-map"),
            cached_symbol!("local-function-key-map"),
            cached_symbol!("key-translation-map"),
        ] {
            let map = interp.lookup_var_key(name, env).unwrap_or(Value::Nil);
            roots.push(map);
            roots.push(map);
        }
        roots.push(Value::Nil);
        roots.push(prompt);
        roots.push(Value::Nil);
        roots.push(Value::Nil);
        roots.push(Value::Nil);
        let mut reader = Self {
            keys: RootedVec::with_capacity(READ_KEY_ELTS),
            raw: RootedVec::new(),
            roots,
            remaps: [KeyRemap::default(); 3],
            read: 0,
            first_unbound: READ_KEY_ELTS + 1,
            original_uppercase_position: None,
            shift_translated: false,
            omitted: Vec::new(),
            fresh_omitted: false,
            keys_start: 0,
            echo_start: 0,
            menu_keys: [false; READ_KEY_ELTS + 1],
        };
        // read_key_sequence, interactively: PROMPT begins the echo string
        // and is shown at once (echo_now), echoing going on only with
        // `echo-keystrokes'; without one, `cursor-in-echo-area' with
        // `echo-keystrokes' has the dash put after what is echoed.
        let interactive = interactive_p(interp, env);
        if interactive {
            if !prompt.is_nil() {
                super::set_echo_prompt(interp, prompt);
                interp.keyboard_input.immediate_echo = false;
                super::echo_now(interp, env)?;
                if !super::echo_keystrokes_p(interp, env) {
                    interp.keyboard_input.immediate_echo = false;
                }
            } else if interp
                .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
                .is_some_and(|value| value.is_truthy())
                && super::echo_keystrokes_p(interp, env)
            {
                super::echo_dash(interp, env)?;
            }
        }
        // The initial state of the echo area and this_command_keys, which
        // a replayed sequence returns to.
        if interactive {
            reader.echo_start = super::echo_length(interp);
        }
        reader.keys_start = interp.keyboard_input.command_keys.len();
        interp.keyboard_input.single_command_start = reader.keys_start;
        reader.replay(interp, env, false)?;
        Ok(reader)
    }

    /// read_key_sequence's replay_sequence: the active maps again and the
    /// keys read so far (MOCK_INPUT, when there are any) looked up anew,
    /// this-command-keys back to where the sequence began and, with mock
    /// input, the echo too.
    fn replay(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        mock_input: bool,
    ) -> Result<(), LispError> {
        // replay_sequence: from the beginning of keybuf, no key read.
        interp.set_variable_key(cached_symbol!("last-nonmenu-event"), Value::Nil, env);
        // keyboard.c:active_maps: the EVENT_START of a first event with
        // parameters, or of the second when the first is the fake prefix
        // (`mode-line' and the like) its POSN_POSN names.
        let car_safe = |value: Value| value.cons_values().map_or(Value::Nil, |(car, _)| car);
        let cdr_safe = |value: Value| value.cons_values().map_or(Value::Nil, |(_, cdr)| cdr);
        let first = self.keys.first().copied().unwrap_or(Value::Nil);
        let second = self.keys.get(1).copied().unwrap_or(Value::Nil);
        let mut position = if first.is_cons() {
            car_safe(cdr_safe(first))
        } else {
            Value::Nil
        };
        if first.is_symbol() && second.is_cons() {
            let start = car_safe(cdr_safe(second));
            if crate::lisp::types::eq(first, car_safe(cdr_safe(start))) {
                position = start;
            }
        }
        let maps = current_active_maps(interp, env, true, Some(&position))?;
        self.roots[6] = Value::cons(Value::symbol("keymap"), Value::list(maps));
        self.read = 0;
        self.first_unbound = READ_KEY_ELTS + 1;
        self.omitted.clear();
        self.fresh_omitted = false;
        interp.keyboard_input.command_keys.truncate(self.keys_start);
        if mock_input && interactive_p(interp, env) {
            super::echo_truncate(interp, self.echo_start, env)?;
        }
        Ok(())
    }

    fn reset_remap(&mut self, index: usize, position: usize) {
        self.remaps[index] = KeyRemap {
            start: position,
            end: position,
        };
        self.roots[2 * index + 1] = self.roots[2 * index];
    }

    fn step(
        &mut self,
        index: usize,
        translate: bool,
        interp: &mut Interpreter,
        env: &mut Env,
    ) -> Result<Option<isize>, LispError> {
        let KeyRemap { start, end } = self.remaps[index];
        self.remaps[index].end += 1;
        let mut next = if keymap_reference_map(interp, &self.roots[2 * index], env).is_some() {
            keymap_access_event(interp, self.roots[2 * index + 1], self.keys[end], true, env)?
                .unwrap_or(Value::Nil)
        } else {
            Value::Nil
        };
        load_autoloaded_prefix_map(interp, &next, env)?;
        // access_keymap_keyremap accepts symbolic arrays and prefix maps.
        if let Kind::Symbol(name) = next.kind()
            && let Ok(function) = interp.lookup_function_symbol(&name, env)
            && (function.is_string()
                || matches!(function.kind(), Kind::Vector(_))
                || keymap_reference_map(interp, &function, env).is_some())
        {
            next = function;
        }
        if translate && function_value_p(interp, &next, env) {
            self.roots.push(next);
            let count = interp.specpdl_index();
            interp.specbind_symbol(
                &"current-key-remap-sequence".into(),
                Value::vector(self.keys[start..=end].iter().copied()),
                env,
            )?;
            let result = interp.call_function_value(next, None, &[self.roots[7]], env);
            interp.with_lisp_stack_roots(&result, |interp| interp.unbind_to(count, env))?;
            let function = self.roots.pop().expect("translation function root");
            next = result?;
            if !next.is_nil() && !next.is_string() && !matches!(next.kind(), Kind::Vector(_)) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("error"),
                    Value::string("Function returns invalid key sequence"),
                    function,
                ])));
            }
        }
        if translate && (next.is_string() || matches!(next.kind(), Kind::Vector(_))) {
            let events = translated_input_events(&next)?;
            let removed = end + 1 - start;
            let length = self.keys.len() - removed + events.len();
            if length >= READ_KEY_ELTS {
                return Err(LispError::Signal("Key sequence too long".into()));
            }
            let diff = events.len() as isize - removed as isize;
            // No Lisp callback occurs while the buffer is being shifted.
            for _ in 0..removed {
                self.keys.remove(start);
            }
            for (offset, event) in events.into_iter().enumerate() {
                self.keys.insert(start + offset, event);
            }
            self.reset_remap(
                index,
                (end + 1).checked_add_signed(diff).expect("translated end"),
            );
            return Ok(Some(diff));
        }
        if let Some(map) = keymap_reference_map(interp, &next, env) {
            self.roots[2 * index + 1] = map;
        } else {
            self.reset_remap(index, start + 1);
        }
        Ok(None)
    }

    fn translate(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
        while self.remaps[0].end < self.read {
            if self.step(0, true, interp, env)?.is_some() {
                self.replay(interp, env, true)?;
                return Ok(true);
            }
        }
        if keymap_reference_map(interp, &self.roots[6], env).is_none()
            && !self.binding_is_undefined(interp, env)?
            && self.remaps[0].start >= self.read
        {
            self.reset_remap(1, self.read);
        } else {
            while self.remaps[1].end < self.remaps[0].start {
                let doit = self.remaps[1].end + 1 == self.read
                    && self.binding_is_undefined(interp, env)?;
                if let Some(diff) = self.step(1, doit, interp, env)? {
                    self.shift(0, diff);
                    self.replay(interp, env, true)?;
                    return Ok(true);
                }
            }
        }
        while self.remaps[2].end < self.remaps[1].start {
            if let Some(diff) = self.step(2, true, interp, env)? {
                self.shift(0, diff);
                self.shift(1, diff);
                self.replay(interp, env, true)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn binding_is_undefined(
        &self,
        interp: &mut Interpreter,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        let binding = self.roots[6];
        let undefined = Value::symbol("undefined");
        // keyboard.c:test_undefined consults live remapping at each call
        // site. A collecting remap filter can change its answer between
        // deciding to scan function-key-map and actually translating it.
        Ok(binding.is_nil()
            || crate::lisp::types::eq(binding, undefined)
            || (binding.is_symbol()
                && crate::lisp::types::eq(
                    command_remapping(interp, &binding, None, env)?,
                    undefined,
                )))
    }

    fn shift(&mut self, index: usize, diff: isize) {
        self.remaps[index].start = self.remaps[index]
            .start
            .checked_add_signed(diff)
            .expect("remap start");
        self.remaps[index].end = self.remaps[index]
            .end
            .checked_add_signed(diff)
            .expect("remap end");
    }

    fn discard_event(&mut self, position: usize) {
        // keyboard.c:read_key_sequence rewinds pending remaps and drops
        // mock input when an unbound up/down event is discarded. Without
        // this, the next raw event can lie before the translation cursor.
        for index in 0..self.remaps.len() {
            if self.remaps[index].end <= position {
                break;
            }
            self.reset_remap(index, self.remaps[index].start.min(position));
        }
        self.keys.truncate(position);
    }

    fn downcase_current(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        if !self.roots[6].is_nil()
            || self.remaps[2].start < self.read
            || !interp
                .lookup_var_key(cached_symbol!("translate-upper-case-key-bindings"), env)
                .is_some_and(|value| value.is_truthy())
        {
            return Ok(false);
        }
        let position = self.read - 1;
        let original = self.keys[position];
        let (translated, reset_function_keys) = match original.kind() {
            Kind::Integer(code) => {
                let lower = if code & KEY_DESCRIPTION_SHIFT_BIT != 0 {
                    code & !KEY_DESCRIPTION_SHIFT_BIT
                } else {
                    let character = code & !KEY_DESCRIPTION_MODIFIER_MASK;
                    if !(0..=0x3f_ffff).contains(&character) {
                        return Ok(false);
                    }
                    let table = interp.current_case_table_id();
                    let mapped = interp
                        .char_table_get(table, character as u32)
                        .and_then(|value| value.integer_value())
                        .unwrap_or(character);
                    mapped | (code & KEY_DESCRIPTION_MODIFIER_MASK)
                };
                if lower == code {
                    return Ok(false);
                }
                (Value::Integer(lower), false)
            }
            Kind::Symbol(_) => {
                let elements = parse_event_symbol_modifiers(interp, &original)?.to_vec()?;
                let shift = Value::symbol("shift");
                if !elements.iter().skip(1).any(|value| value.eq_value(shift)) {
                    return Ok(false);
                }
                let description = Value::list(
                    elements
                        .iter()
                        .skip(1)
                        .copied()
                        .filter(|value| !value.eq_value(shift))
                        .chain(elements.first().copied()),
                );
                (event_convert_list_value(interp, &description)?, true)
            }
            _ => return Ok(false),
        };
        // keyboard.c retries the actual command maps with the lower-case
        // event. Shifted function keys also restart fkey/keytran, allowing
        // mappings such as S-backspace -> backspace -> DEL.
        self.roots[8] = original;
        self.original_uppercase_position = Some(position);
        self.keys[position] = translated;
        self.shift_translated = true;
        if reset_function_keys {
            self.reset_remap(1, 0);
            self.reset_remap(2, 0);
        }
        self.replay(interp, env, true)?;
        Ok(true)
    }

    pub(crate) fn finish(
        mut self,
        interp: &mut Interpreter,
        dont_downcase_last: bool,
        env: &mut Env,
    ) -> Result<Vec<Value>, LispError> {
        // GNU returns all generated events, even if lookup finished before
        // their end. Only that unread suffix is appended to command keys:
        // restoring the returned last event's case does not rewrite the
        // lower-case event already recorded in this-command-keys.
        interp
            .keyboard_input
            .command_keys
            .extend_from_slice(&self.keys[self.read..]);
        if (dont_downcase_last || self.roots[6].is_nil())
            && self.read > 0
            && self.original_uppercase_position == Some(self.read - 1)
        {
            self.keys[self.read - 1] = self.roots[8];
            self.shift_translated = false;
        }
        if self.shift_translated {
            interp.set_variable_key(
                cached_symbol!("this-command-keys-shift-translated"),
                Value::T,
                env,
            );
        }
        // ...and echo_update.
        super::echo_update(interp, env)?;
        Ok(self.keys.to_vec())
    }

    fn needs_input(&self, interp: &Interpreter, env: &Env) -> bool {
        if !self.roots[6].is_nil() {
            keymap_reference_map(interp, &self.roots[6], env).is_some()
        } else {
            self.remaps[2].start < self.read
        }
    }

    /// read_key_sequence's last_nonmenu_event as read_char is called
    /// for the next key: the last key read not from a mouse menu, nil
    /// before the first.
    pub(crate) fn prev_event(&self, interp: &Interpreter, env: &Env) -> Value {
        interp
            .lookup_var_key(cached_symbol!("last-nonmenu-event"), env)
            .unwrap_or(Value::Nil)
    }

    /// read_key_sequence's current_binding, the keymap read_char is
    /// given for the next key.
    pub(crate) fn current_binding(&self) -> Value {
        self.roots[6]
    }

    /// read_key_sequence_cmd: the binding the sequence ended with, or
    /// `prefix-help-command' for a help character after an unbound prefix.
    pub(crate) fn read_key_sequence_cmd(&self) -> Value {
        self.roots[10]
    }

    /// Advance the same reader used by read-key-sequence by one external
    /// event. Terminal waits, minibuffer recursion and collecting callbacks
    /// may occur between calls; the reached maps and generated input remain
    /// rooted here. No frontend needs to replay an unchanged prefix.
    pub(crate) fn read_event(
        &mut self,
        interp: &mut Interpreter,
        event: Value,
        env: &mut Env,
    ) -> Result<KeyResolution, LispError> {
        let event = match event.cons_values() {
            Some((head, tail))
                if crate::lisp::types::eq(head, Value::T)
                    || crate::lisp::types::eq(head, Value::symbol("no-record")) =>
            {
                tail
            }
            _ => event,
        };
        // read_char's input method step for the sequence's first event,
        // read with PREV_EVENT (last_nonmenu_event) nil; read_char reads
        // again when the input method gives no event.
        let event = if self.keys.is_empty() {
            match super::input_method_for_last_event(
                interp,
                env,
                event,
                super::InputMethodRead::KeySequence,
            )? {
                Some(event) => event,
                None => return Ok(KeyResolution::Prefix),
            }
        } else {
            event
        };
        // read_key_sequence after each read_char, its input method step
        // included: a quit the read reported as the quit character is
        // reported no more (while-no-input's `throw-on-input' has thrown
        // at that step's funcall, or is lost).
        interp.set_c_variable("quit-flag", Value::Nil);
        // read_char past the input method: an unread or macro event not yet
        // recorded is recorded as the input method left it.
        super::record_unrecorded_event(interp, &event, env);
        // read_char added the event to this-command-keys unless it was
        // reread with keys already there.
        let reread = std::mem::take(&mut interp.keyboard_input.last_event_reread);
        let omit = (reread && !interp.keyboard_input.command_keys.is_empty())
            || super::take_read_menu_exit();
        // read_char then lets echoing start at the next pause.
        if !omit && !super::echo::mouse_movement_p(interp, &event) {
            interp.keyboard_input.ok_to_echo_at_next_pause = true;
        }
        self.keys.push(event);
        // used_mouse_menu_history[t] = used_mouse_menu.
        let position = self.keys.len() - 1;
        let used_mouse_menu = super::take_read_used_mouse_menu();
        if position < self.menu_keys.len() {
            self.menu_keys[position] = used_mouse_menu;
        }
        // keyboard.c copies only the event spine before remapping. The
        // unchanged mouse.el translator can mutate its head in place;
        // raw keys retain the original head and share the position data.
        // raw_keybuf gets the key after whatever a read in the middle of
        // the sequence (a menu's) added, until init_raw_keybuf_count.
        let raw = if event.is_cons() {
            copy_sequence_value(interp, &event)?
        } else {
            event
        };
        self.raw.push(raw);
        interp.keyboard_input.raw_keys.push(raw);
        interp.set_variable_key(cached_symbol!("last-input-event"), event, env);
        let mut expanded = false;
        if mouse_event_head(interp, &event).is_none()
            && let Some(position_tail) = event
                .cdr()
                .ok()
                .and_then(|tail| tail.car_if_list())
                .and_then(|position| position.cdr_if_list())
            && let Ok(area) = position_tail.car()
            && ["menu-bar", "tab-bar", "tool-bar"]
                .iter()
                .any(|name| crate::lisp::types::eq(area, Value::symbol(name)))
        {
            // Non-mouse parameterized events receive this prefix only
            // on external input, never when returned by a translation.
            // GNU marks the shared position itself as expanded here.
            if self.keys.len() + 1 >= READ_KEY_ELTS {
                return Err(LispError::Signal("Key sequence too long".into()));
            }
            position_tail.set_car(Value::list([area]))?;
            let index = self.keys.len() - 1;
            self.keys.insert(index, area);
            expanded = true;
        }
        if self.read == 0 || expanded {
            // The sequence's first event looks the maps up again where
            // read_key_sequence had them before reading it; anything else
            // is a replay of mock input.
            self.replay(interp, env, expanded || self.keys.len() > 1)?;
            // A reread first key after CONTINUE-ECHO's keys is not added
            // either.
            self.fresh_omitted = omit && !expanded;
        } else {
            self.fresh_omitted = omit;
        }
        loop {
            // keyboard.c:read_key_sequence stops retrying an unbound
            // prefix once every possible translation has moved past it.
            // Replay the remaining suffix instead of returning the stale
            // prefix with a later translation (or waiting indefinitely).
            if self.first_unbound < self.remaps[2].start {
                let discarded = self.first_unbound + 1;
                self.keys.truncate(self.read);
                for _ in 0..discarded {
                    self.keys.remove(0);
                }
                for index in 0..self.remaps.len() {
                    self.reset_remap(index, self.remaps[index].start - discarded);
                }
                self.replay(interp, env, true)?;
            }
            if self.read >= READ_KEY_ELTS {
                return Err(LispError::Signal("Key sequence too long".into()));
            }
            let Some(event) = self.keys.get(self.read).copied() else {
                return Ok(KeyResolution::Prefix);
            };
            let mut real_start = self.read;
            if mouse_event_head(interp, &event).is_some() {
                let position = event.cdr()?.car()?.cdr()?.car()?;
                let expanded = self.roots[9]
                    .to_vec()?
                    .iter()
                    .any(|previous| previous.eq_value(event));
                if expanded || position.is_cons() {
                    real_start = self.read.saturating_sub(1);
                } else if position.is_symbol() {
                    if self.keys.len() + 1 >= READ_KEY_ELTS {
                        return Err(LispError::Signal("Key sequence too long".into()));
                    }
                    self.roots[9] = Value::cons(event, self.roots[9]);
                    self.keys.insert(self.read, position);
                    continue;
                }
            } else if event
                .cdr()
                .ok()
                .and_then(|tail| tail.car_if_list())
                .and_then(|position| position.cdr_if_list())
                .and_then(|tail| tail.car_if_list())
                .is_some_and(|position| position.is_cons())
            {
                real_start = self.read.saturating_sub(1);
            }
            let binding = if let Some(map) = keymap_reference_map(interp, &self.roots[6], env) {
                keymap_access_event(interp, map, event, true, env)?.unwrap_or(Value::Nil)
            } else {
                Value::Nil
            };
            if binding.is_nil() {
                self.first_unbound = self.first_unbound.min(self.read);
            } else {
                self.first_unbound = self.first_unbound.max(self.read + 1);
            }
            if binding.is_nil() && is_mouse_down_event(&event) {
                self.discard_event(real_start);
                if real_start != self.read {
                    self.replay(interp, env, true)?;
                }
                continue;
            }
            self.roots[6] = binding;
            load_autoloaded_prefix_map(interp, &binding, env)?;
            if self.fresh_omitted && self.read + 1 == self.keys.len() {
                self.omitted.push(self.read);
            }
            self.fresh_omitted = false;
            self.read += 1;
            // keybuf[t++] = key.  Normally, last_nonmenu_event gets the
            // previous key we read.  But when a mouse popup menu is being
            // used, we don't update last_nonmenu_event; it continues to
            // hold the mouse event that preceded the first level of menu.
            if !self.menu_keys.get(self.read - 1).copied().unwrap_or(false) {
                interp.set_variable_key(
                    cached_symbol!("last-nonmenu-event"),
                    self.keys[self.read - 1],
                    env,
                );
            }
            // read_char's add_command_key for a key read, or
            // read_key_sequence's for mock input after a replay (which
            // returned this_command_keys to the sequence's start): the key
            // joins this-command-keys after whatever a read in the middle
            // of the sequence (a menu's, a help character's) recorded.
            if !self.omitted.contains(&(self.read - 1)) {
                interp
                    .keyboard_input
                    .command_keys
                    .push(self.keys[self.read - 1]);
            }
            // "Record what part of this_command_keys is the current key
            // sequence": its last T keys, none before the start.
            interp.keyboard_input.single_command_start = interp
                .keyboard_input
                .command_keys
                .len()
                .saturating_sub(self.read);
            // read_char's add_command_key and echo_update (and, for mock
            // input, read_key_sequence's echo_now while echoing).
            super::echo_update(interp, env)?;
            if self.translate(interp, env)? {
                continue;
            }
            // An unbound key: an upper-case character retried in lower
            // case; else (not_upcase) a help character after a prefix runs
            // `prefix-help-command'; else a shifted function key retried
            // unshifted.
            let character = matches!(self.keys[self.read - 1].kind(), Kind::Integer(_));
            if character && self.downcase_current(interp, env)? {
                continue;
            }
            if self.roots[6].is_nil() && self.read > 1 {
                let key = self.keys[self.read - 1];
                let head = key.cons_values().map_or(key, |(head, _)| head);
                if super::echo::help_char_p(interp, head, env) {
                    let command = interp
                        .lookup_var_key(cached_symbol!("prefix-help-command"), env)
                        .unwrap_or(Value::Nil);
                    self.roots[10] = command;
                    return if command.is_nil() || command.is_symbol() {
                        resolve_read_key_binding(interp, env, command, &self.keys)
                    } else {
                        Ok(KeyResolution::Command(command))
                    };
                }
            }
            if !character && self.downcase_current(interp, env)? {
                continue;
            }
            if !self.needs_input(interp, env) {
                self.roots[10] = self.roots[6];
                return resolve_read_key_binding(interp, env, self.roots[6], &self.keys);
            }
        }
    }
}

/// macros.c:at_end_of_macro_p: a macro executes and has no event left
/// (or is t, an early exit).
fn at_end_of_macro_p(interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
    let macro_value = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .unwrap_or(Value::Nil);
    if macro_value.is_nil() {
        return Ok(false);
    }
    if crate::lisp::types::eq(macro_value, Value::T) {
        return Ok(true);
    }
    let index = interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro-index"), env)
        .unwrap_or(Value::Integer(0));
    let length = super::call(interp, "length", &[macro_value], env)?;
    Ok(index.as_integer()? >= length.as_integer()?)
}

// read_key_sequence's read_char: kbd_buffer_get_event's wait runs the
// timers, idle ones included, until an event arrives.
fn wait_for_sequence_event(
    interp: &mut Interpreter,
    env: &mut Env,
    prev_event: Value,
    map: Value,
) -> Result<Value, LispError> {
    read_key_sequence_event(interp, env, prev_event, map)
}

/// keyboard.c's INTERACTIVE: no keyboard macro executing, and not
/// `noninteractive'.
fn interactive_p(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .is_none_or(|value| value.is_nil())
        && !interp
            .forwarded_c_value("noninteractive", &Env::new())
            .is_some_and(|value| value.is_truthy())
}

/// keyboard.c:read_key_sequence_vs: outside the command loop (CMD_LOOP
/// false) an input method stops at the first character and uses the echo
/// area; without CONTINUE_ECHO the sequence starts this-command-keys
/// afresh; the raw keys start empty.
pub(crate) fn read_key_sequence_vs(
    interp: &mut Interpreter,
    prompt: Value,
    continue_echo: bool,
    dont_downcase_last: bool,
    can_return_switch_frame: bool,
    cmd_loop: bool,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let flag = if cmd_loop { Value::Nil } else { Value::T };
    let count = interp.specpdl_index();
    interp.specbind_symbol(&"input-method-exit-on-first-char".into(), flag, env)?;
    interp.specbind_symbol(&"input-method-use-echo-area".into(), flag, env)?;
    if !continue_echo {
        super::clear_command_keys(interp);
    }
    super::init_raw_keybuf_count(interp);
    let read = read_key_sequence(
        interp,
        prompt,
        dont_downcase_last,
        can_return_switch_frame,
        env,
    );
    interp.unbind_to(count, env)?;
    let read = read?;
    // A rejected menu (read_key_sequence's -1) quits.
    if read.menu_rejected {
        interp.set_c_variable("quit-flag", Value::T);
        interp.maybe_quit(env)?;
    }
    Ok(read.keys)
}

/// keyboard.c:read_key_sequence's result: the keys (none at a keyboard
/// macro's end or the keyboard's), read_key_sequence_cmd and
/// read_key_sequence_remapped.
pub(crate) struct ReadKeySequence {
    pub(crate) keys: Vec<Value>,
    pub(crate) cmd: Value,
    pub(crate) remapped: Value,
    /// read_key_sequence's -1: a menu read_char showed was rejected.
    pub(crate) menu_rejected: bool,
}

/// keyboard.c:read_key_sequence: read_char's events looked up in the
/// active maps until they make a complete sequence.  A switch-frame event
/// after the first key, or as the first when CAN_RETURN_SWITCH_FRAME is
/// false, is delayed until the sequence ends (unread_switch_frame); an
/// end of input (read_char's -1) or of the executing macro ends the read
/// with no keys.
pub(crate) fn read_key_sequence(
    interp: &mut Interpreter,
    prompt: Value,
    dont_downcase_last: bool,
    can_return_switch_frame: bool,
    env: &mut Env,
) -> Result<ReadKeySequence, LispError> {
    // read_key_sequence's read_char decodes the terminal's bytes.
    let _decoded = super::RawEventRead::enter(false);
    let mut reader = KeySequenceReader::new(interp, prompt, env)?;
    let mut delayed_switch_frame = RootedVec::from_vec(vec![Value::Nil]);
    let result: Result<Option<KeyResolution>, LispError> = (|| loop {
        // At the end of an executing macro, with no unread events, the
        // sequence ends empty (t = 0), whatever it has read.
        if at_end_of_macro_p(interp, env)?
            && !super::kbd_input::requeued_events_pending_p(interp, env)
        {
            return Ok(None);
        }
        let prev_event = reader.prev_event(interp, env);
        let event = wait_for_sequence_event(interp, env, prev_event, reader.current_binding())?;
        // read_char returns t when it shows a menu and the user rejects
        // it.  Just return -1.
        if crate::lisp::types::eq(event, Value::T) {
            return Ok(Some(KeyResolution::MenuRejected));
        }
        // read_char returns -1 at the end of input: t = 0.
        if matches!(event.kind(), Kind::Integer(-1)) {
            return Ok(None);
        }
        // EVENT_HAS_PARAMETERS with EVENT_HEAD_KIND switch-frame: a
        // `switch-frame' or a `select-window' event.
        if event.cons_values().is_some_and(|(head, _)| {
            interp
                .get_property_of_symbol_value(head, "event-kind")
                .is_some_and(|kind| crate::lisp::types::eq_symbol_named(kind, "switch-frame"))
        }) && (!reader.keys.is_empty() || !can_return_switch_frame)
        {
            delayed_switch_frame[0] = event;
            continue;
        }
        let resolution = reader.read_event(interp, event, env)?;
        if !matches!(resolution, KeyResolution::Prefix) {
            return Ok(Some(resolution));
        }
    })();
    // done: unread_switch_frame = delayed_switch_frame, on a return (a
    // nonlocal exit leaves it).
    let result = result?;
    interp.keyboard_input.unread_switch_frame =
        (!delayed_switch_frame[0].is_nil()).then_some(delayed_switch_frame[0]);
    let Some(resolution) = result else {
        return Ok(ReadKeySequence {
            keys: Vec::new(),
            cmd: Value::Nil,
            remapped: Value::Nil,
            menu_rejected: false,
        });
    };
    if matches!(resolution, KeyResolution::MenuRejected) {
        return Ok(ReadKeySequence {
            keys: Vec::new(),
            cmd: Value::Nil,
            remapped: Value::Nil,
            menu_rejected: true,
        });
    }
    let cmd = reader.read_key_sequence_cmd();
    let remapped = match resolution {
        KeyResolution::Command(command) if !crate::lisp::types::eq(command, cmd) => command,
        _ => Value::Nil,
    };
    let keys = reader.finish(interp, dont_downcase_last, env)?;
    Ok(ReadKeySequence {
        keys,
        cmd,
        remapped,
        menu_rejected: false,
    })
}

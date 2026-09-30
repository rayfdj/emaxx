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
    // Three (parent, current) pairs, binding, prompt, original case, and
    // events for which a non-text mouse prefix has already been generated.
    roots: RootedVec<Value>,
    remaps: [KeyRemap; 3],
    read: usize,
    first_unbound: usize,
    original_uppercase_position: Option<usize>,
    shift_translated: bool,
}

impl KeySequenceReader {
    pub(crate) fn new(
        interp: &mut Interpreter,
        prompt: Value,
        env: &mut Env,
    ) -> Result<Self, LispError> {
        let mut roots = RootedVec::with_capacity(10);
        for name in [
            "input-decode-map",
            "local-function-key-map",
            "key-translation-map",
        ] {
            let map = interp.lookup_var(name, env).unwrap_or(Value::Nil);
            roots.push(map);
            roots.push(map);
        }
        roots.push(Value::Nil);
        roots.push(prompt);
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
        };
        reader.replay(interp, env)?;
        Ok(reader)
    }

    fn replay(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
        let event = self.keys.first().copied().and_then(|first| {
            if first.is_symbol() && self.keys.len() > 1 {
                self.keys.get(1).copied()
            } else {
                Some(first)
            }
        });
        let position = event.and_then(|event| {
            mouse_event_head(interp, &event)?;
            event.cdr().ok()?.car().ok()
        });
        let maps = current_active_maps(interp, env, true, position.as_ref())?;
        self.roots[6] = Value::cons(Value::symbol("keymap"), Value::list(maps));
        self.read = 0;
        self.first_unbound = READ_KEY_ELTS + 1;
        interp.keyboard_input.command_keys.clear();
        interp.keyboard_input.single_command_start = 0;
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
            && let Ok(function) = interp.lookup_function(&name, env)
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
                self.replay(interp, env)?;
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
                    self.replay(interp, env)?;
                    return Ok(true);
                }
            }
        }
        while self.remaps[2].end < self.remaps[1].start {
            if let Some(diff) = self.step(2, true, interp, env)? {
                self.shift(0, diff);
                self.shift(1, diff);
                self.replay(interp, env)?;
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
            || binding.eq_value(undefined)
            || (binding.is_symbol()
                && command_remapping(interp, &binding, None, env)?.eq_value(undefined)))
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
                .lookup_var("translate-upper-case-key-bindings", env)
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
                        .and_then(|value| value.as_integer().ok())
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
        self.replay(interp, env)?;
        Ok(true)
    }

    pub(crate) fn finish(
        mut self,
        interp: &mut Interpreter,
        dont_downcase_last: bool,
        env: &mut Env,
    ) -> Vec<Value> {
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
            interp.set_variable("this-command-keys-shift-translated", Value::T, env);
        }
        self.keys.to_vec()
    }

    fn needs_input(&self, interp: &Interpreter, env: &Env) -> bool {
        if !self.roots[6].is_nil() {
            keymap_reference_map(interp, &self.roots[6], env).is_some()
        } else {
            self.remaps[2].start < self.read
        }
    }

    pub(crate) fn command_binding(&self) -> Value {
        self.roots[6]
    }

    pub(crate) fn keys(&self) -> &[Value] {
        &self.keys
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
            Some((head, tail)) if head == Value::T || head.eq_value(Value::symbol("no-record")) => {
                tail
            }
            _ => event,
        };
        self.keys.push(event);
        // keyboard.c copies only the event spine before remapping. The
        // unchanged mouse.el translator can mutate its head in place;
        // raw keys retain the original head and share the position data.
        self.raw.push(if event.is_cons() {
            copy_sequence_value(interp, &event)?
        } else {
            event
        });
        interp.set_variable("last-input-event", event, env);
        let mut expanded = false;
        if mouse_event_head(interp, &event).is_none()
            && let Some(position_tail) = event
                .cdr()
                .ok()
                .and_then(|tail| tail.car().ok())
                .and_then(|position| position.cdr().ok())
            && let Ok(area) = position_tail.car()
            && ["menu-bar", "tab-bar", "tool-bar"]
                .iter()
                .any(|name| area.eq_value(Value::symbol(name)))
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
            self.replay(interp, env)?;
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
                self.replay(interp, env)?;
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
                .and_then(|tail| tail.car().ok())
                .and_then(|position| position.cdr().ok())
                .and_then(|tail| tail.car().ok())
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
                    self.replay(interp, env)?;
                }
                continue;
            }
            self.roots[6] = binding;
            load_autoloaded_prefix_map(interp, &binding, env)?;
            self.read += 1;
            set_command_key_state(
                interp,
                self.keys[..self.read].to_vec(),
                self.raw.to_vec(),
                env,
            );
            if self.translate(interp, env)? || self.downcase_current(interp, env)? {
                continue;
            }
            if !self.needs_input(interp, env)
                || (has_tty_menu_executor()
                    && mouse_event_head(interp, &event).is_some()
                    && keymap_reference_map(interp, &self.roots[6], env).is_some())
            {
                return resolve_read_key_binding(interp, env, self.roots[6], &self.keys);
            }
        }
    }
}

// GNU's unchanged read-key owns its idle-timer ambiguity fallback. A batch
// fixture with queued input has no frontend poller, but its timers still
// need the same idle wait and may unwind this reader through a Lisp throw.
fn wait_for_sequence_event(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let queued = interp
        .lookup_var("unread-command-events", env)
        .is_some_and(|v| v.cons_values().is_some());
    let idle_timers = interp
        .lookup_var("timer-idle-list", env)
        .is_some_and(|value| value.is_truthy());
    if queued || has_tty_event_poller() || executing_kbd_macro_p(interp, env) || !idle_timers {
        return pop_unread_command_event_value(interp, env);
    }
    let previous = interp.set_waiting_for_user_input(true);
    tty_note_idle_start(interp, env);
    let start = std::time::Instant::now();
    let result = (|| {
        loop {
            interp.service_async_runtime_events(env, true, Some(start.elapsed().as_secs_f64()))?;
            if interp
                .lookup_var("unread-command-events", env)
                .is_some_and(|v| v.cons_values().is_some())
            {
                return pop_unread_command_event_value(interp, env);
            }
            interp.maybe_quit(env)?;
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    })();
    tty_note_idle_end();
    interp.set_waiting_for_user_input(previous);
    result
}

pub(crate) fn read_key_sequence_events(
    interp: &mut Interpreter,
    prompt: Value,
    dont_downcase_last: bool,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut reader = KeySequenceReader::new(interp, prompt, env)?;
    loop {
        let event = wait_for_sequence_event(interp, env)?;
        if !matches!(
            reader.read_event(interp, event, env)?,
            KeyResolution::Prefix
        ) {
            return Ok(reader.finish(interp, dont_downcase_last, env));
        }
    }
}

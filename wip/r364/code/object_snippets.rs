// print/object.rs: snippets of the first implementation.

thread_local! {
    /// print.c's prstack: the one print stack of every print, as GNU's
    /// static is.  Each print_object works on the entries above the
    /// depth it found (its base_sp), and nothing takes entries off when
    /// a print is left by a throw or an error: entries of a print run
    /// inside another (by a function it prints to, or by
    /// `print-unreadable-function') and left that way are on top when
    /// the outer print goes on, and it finishes them, as GNU's does.
    /// The buffer is scanned for the values the entries hold.
    static PRSTACK: std::cell::RefCell<crate::lisp::alloc::RootedVec<PrintEntry>> =
        const { std::cell::RefCell::new(crate::lisp::alloc::RootedVec::new()) };
}

/// prstack.sp.
fn prstack_sp() -> usize {
    PRSTACK.with_borrow(|stack| stack.len())
}

/// print_stack_push.
fn prstack_push(entry: PrintEntry) {
    PRSTACK.with_borrow_mut(|stack| stack.push(entry));
}

/// `prstack.sp--': the top entry goes, whichever print pushed it.
fn prstack_pop() {
    PRSTACK.with_borrow_mut(|stack| {
        stack.pop();
    });
}

/// The innermost entry above BASE_SP, and where it is.
fn prstack_top(base_sp: usize) -> Option<(usize, PrintEntry)> {
    PRSTACK.with_borrow(|stack| {
        let sp = stack.len();
        (sp > base_sp).then(|| (sp - 1, stack[sp - 1]))
    })
}

/// The entry at INDEX rewritten, as print.c writes through its pointer
/// to the entry.
fn prstack_set(index: usize, entry: PrintEntry) {
    PRSTACK.with_borrow_mut(|stack| {
        if let Some(slot) = stack.get_mut(index) {
            *slot = entry;
        }
    });
}

    /// print.c:strout of a Lisp string's data as it is (a string found in
    /// `print-number-table'): its bytes one block, each a character when
    /// the string has as many characters as bytes (to a function, each as
    /// the C `char' it is); a multibyte string's characters otherwise.
    fn strout_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: crate::lisp::types::StringObjectRef,
    ) -> Result<(), LispError> {
        {
            let state = text.borrow();
            let bytes = state.bytes();
            if state.len() == bytes.len() {
                if matches!(self.sink, PrintSink::Function(_)) {
                    for &byte in bytes {
                        self.out.push_c_function_byte(byte);
                    }
                } else {
                    self.out.push_octets(bytes);
                }
            } else {
                let mut block = PrintOutput::plain();
                let mut rest = bytes;
                while !rest.is_empty() {
                    let (code, width) = crate::lisp::types::string_data::decode_character(rest)
                        .unwrap_or((u32::from(rest[0]), 1));
                    block.push_code(code);
                    rest = &rest[width..];
                }
                self.out.append_strout(&block);
            }
        }
        self.sync(interp, env)
    }

    // print_obj's start (circle detection); `vars' reads lazily.
        self.sync(interp, env)?;
        interp.maybe_quit(env)?;
        self.vars(interp, CIRCLE);
        // With `print-circle': PRINT_CIRCLE_CANDIDATE_P, which reads
        // `print-gensym'.
        let candidate = self.options.circle && {
            self.vars(interp, GENSYM);
            circle_candidate(interp, &obj, self.options)
        };
        if !self.options.circle {
            // Simple but incomplete way.
            let depth = usize::try_from(interp.print_globals.depth).unwrap_or(0);
            if depth >= PRINT_CIRCLE {
                return Err(LispError::Signal(
                    "Apparently circular structure being printed".into(),
                ));
            }
            if let Some(index) = interp.print_globals.being_printed[..depth]
                .iter()
                .position(|outer| outer.word() == obj.word())
            {
                self.strout(interp, env, &format!("#{index}"))?;
                return Ok(None);
            }
            interp.print_globals.being_printed[depth] = obj;
        } else if candidate {
            let num = number_table_get(interp, env, &obj)?;
            match num.kind() {
                // Not printed yet: label it (the label written first),
                // and remember it is printed.
                Kind::Integer(n) if n < 0 => {
                    self.strout(interp, env, &format!("#{}=", -n))?;
                    number_table_put(interp, env, obj, Value::Integer(-n))?;
                }
                // ... Kind::Integer(n): strout "#n#", done; a string:
                // strout_string, done.
            }
        }

    /// print.c:print_stack_push_vector: the bracket, then `print-length'
    /// read, then the entry.
    fn push_vector(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        (lbrac, rbrac): (&str, &'static str),
        obj: Value,
        start: usize,
        size: usize,
    ) -> Result<(), LispError> {
        self.strout(interp, env, lbrac)?;
        self.vars(interp, LENGTH);
        let print_size = self.options.length.map_or(size, |length| length.min(size));
        prstack_push(PrintEntry::Vector {
            obj,
            size: print_size,
            idx: start,
            end: rbrac,
            truncated: print_size < size,
        });
        Ok(())
    }
    // callers: ("[", "]") vector; ("#s(", ")") record; ("#[", "]") closure
    // (public_len); ("#^[", "]") char-table (slot_count); ("", "]")
    // sub-char-table from 1.

    // The prin1 string loop for a function sink (print_string_literal):
            self.printchar(interp, env, '"')?;
            let size_byte = printed.borrow().bytes().len();
            let mut offset = 0;
            let mut need_nonhex = false;
            loop {
                {
                    let printed = printed.borrow();
                    // To the `size_byte' read at the start: a string the
                    // function made shorter is read at its terminating
                    // NUL next; nothing past that is the string's.
                    if offset >= size_byte || offset > printed.bytes().len() {
                        break;
                    }
                    // The escape variables this character's turn reads.
                    let multibyte = printed.is_multibyte();
                    self.vars(
                        interp,
                        match printed.bytes().get(offset).copied().unwrap_or(0) {
                            b'\n' | 0x0c => ESCAPE_NEWLINES | ESCAPE_CONTROL,
                            0..=0x1f | 0x7f => ESCAPE_CONTROL,
                            0x80.. if multibyte => ESCAPE_MULTIBYTE,
                            0x80.. => ESCAPE_NONASCII,
                            _ => 0,
                        },
                    );
                    prin1_string_char(
                        self.escapes,
                        &printed,
                        &mut offset,
                        &mut need_nonhex,
                        &mut self.out,
                    );
                }
                self.flush(interp, env)?;
            }
            self.printchar(interp, env, '"')?;
    // (`printed' is print_prune_string_charset's copy unless `itself';
    // any other sink: self.vars(interp, ESCAPES); render_prin1_string(...);
    // self.sync(...).)  Then `if (string_intervals (obj))' again: the
    // string itself may have lost its properties to Lisp the print ran.

    // print_hash_table's head:
        self.strout(interp, env, "#s(hash-table")?;
        let test = table.test_name();
        // `!BASE_EQ (h->test->name, Qeql)'.
        if !matches!(test.kind(), Kind::Symbol(name) if name.is_named("eql")) {
            self.strout(interp, env, " test ")?;
            self.print_object(interp, env, test, escape)?;
        }
        let weakness = hash_table_weakness_value(table);
        if !weakness.is_nil() {
            self.strout(interp, env, " weakness ")?;
            self.print_object(interp, env, weakness, escape)?;
        }
        if table.purecopy() {
            self.strout(interp, env, " purecopy t")?;
        }
        // `h->count' as it is once all of that is written.

use super::*;

pub(super) fn string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
    update_match_data: bool,
) -> Result<Value, LispError> {
    let max_args = if update_match_data { 4 } else { 3 };
    if args.len() < 2 || args.len() > max_args {
        return Err(LispError::WrongNumberOfArgs(
            if update_match_data {
                "string-match".into()
            } else {
                "string-match-p".into()
            },
            args.len(),
        ));
    }
    super::search::string_match_1(
        interp,
        env,
        &args[0],
        &args[1],
        args.get(2),
        false,
        update_match_data && !args.get(3).is_some_and(Value::is_truthy),
    )
}

pub(super) fn posix_string_match_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "posix-string-match".into(),
            args.len(),
        ));
    }
    super::search::string_match_1(
        interp,
        env,
        &args[0],
        &args[1],
        args.get(2),
        true,
        !args.get(3).is_some_and(Value::is_truthy),
    )
}

pub(super) fn skip_chars_forward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::search::skip_chars(interp, env, true, spec_value, limit_value)
}

pub(super) fn skip_chars_backward_impl(
    interp: &mut Interpreter,
    spec_value: &Value,
    limit_value: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::search::skip_chars(interp, env, false, spec_value, limit_value)
}

pub(super) fn looking_at_impl(
    interp: &mut Interpreter,
    pattern_value: &Value,
    posix: bool,
    update_match_data: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    super::search::looking_at_1(interp, env, pattern_value, posix, update_match_data)
}

pub(super) fn buffer_regex_search(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
    forward: bool,
    posix: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            if posix && forward {
                "posix-search-forward".into()
            } else if posix {
                "posix-search-backward".into()
            } else if forward {
                "re-search-forward".into()
            } else {
                "re-search-backward".into()
            },
            args.len(),
        ));
    }
    super::search::search_command(interp, env, args, if forward { 1 } else { -1 }, true, posix)
}

pub(super) fn slice_string_chars(source: &str, start: usize, end: usize) -> String {
    source
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

/// search.c update_search_regs, which replace_range runs for
/// `replace-match': [OLDSTART, OLDEND) became [OLDSTART, NEWEND).  A
/// start before or at OLDSTART stays (the replaced group is taken to
/// enclose it), one inside goes to OLDSTART and one after shifts; an end
/// at or after OLDEND shifts and one inside goes to OLDSTART.
pub(super) fn update_search_regs(
    match_data: &mut [Option<(usize, usize)>],
    oldstart: usize,
    oldend: usize,
    newend: usize,
) {
    let shift = |position: usize| position + newend - oldend;
    for (start, end) in match_data.iter_mut().flatten() {
        if *start > oldstart {
            *start = if *start >= oldend {
                shift(*start)
            } else {
                oldstart
            };
        }
        if *end >= oldend {
            *end = shift(*end);
        } else if *end > oldstart {
            *end = oldstart;
        }
    }
}

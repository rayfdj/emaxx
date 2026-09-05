use super::{Env, Interpreter, LispError, Value, call, string_like, string_text};

/// xdisp.c:vadd_to_log formats through the C primitive and writes only to
/// the log. It does not dispatch Lisp `message', echo hooks, or batch stderr.
pub(crate) fn add_to_log(
    interp: &mut Interpreter,
    format: &str,
    arguments: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let mut values = Vec::with_capacity(arguments.len() + 1);
    values.push(Value::string(format));
    values.extend_from_slice(arguments);
    let formatted = call(interp, "format-message", &values, env)?;
    append_message_log(interp, &string_text(&formatted)?, env);
    Ok(())
}

/// Shared log-writing portion of the existing C `message' implementation.
/// Keep logging separate from echo/capture policy so internal C diagnostics
/// use the same log without accidentally invoking Lisp message handlers.
pub(super) fn append_message_log(interp: &mut Interpreter, text: &str, env: &Env) {
    let buffer_name = interp
        .lookup_var("messages-buffer-name", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .unwrap_or_else(|| "*Messages*".into());
    let log_max = interp
        .lookup_var("message-log-max", env)
        .unwrap_or(Value::T);
    if !text.is_empty() && !log_max.is_nil() {
        let buffer_id = interp
            .find_buffer(&buffer_name)
            .map(|(id, _)| id)
            .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
        if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
            let end = buffer.point_max();
            buffer.goto_char(end);
            buffer.insert(&(text.to_owned() + "\n"));
            if let Ok(max_lines) = log_max.as_integer()
                && max_lines >= 0
            {
                let contents = buffer.full_buffer_string();
                let lines = contents.matches('\n').count();
                if lines > max_lines as usize {
                    let drop = lines - max_lines as usize;
                    let mut offset = 0usize;
                    for _ in 0..drop {
                        if let Some(next) = contents[offset..].find('\n') {
                            offset += next + 1;
                        }
                    }
                    let char_end = contents[..offset].chars().count();
                    let _ = buffer.delete_region(1, char_end + 1);
                }
            }
        }
    }
}

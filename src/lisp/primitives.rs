use super::eval::Interpreter;
use super::types::{LispError, Value};

/// Check if a name is a known builtin.
pub fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        // Arithmetic
        "+"  | "-"
            | "*"
            | "/"
            | "%"
            | "mod"
            | "1+"
            | "1-"
            | "max"
            | "min"
            | "abs"
            // Comparison
            | "="
            | "<"
            | ">"
            | "<="
            | ">="
            | "/="
            // Equality
            | "eq"
            | "equal"
            | "string="
            | "string-equal"
            // Type predicates
            | "null"
            | "not"
            | "integerp"
            | "numberp"
            | "floatp"
            | "stringp"
            | "symbolp"
            | "consp"
            | "listp"
            | "bufferp"
            | "buffer-live-p"
            | "zerop"
            | "natnump"
            | "atom"
            | "nlistp"
            | "characterp"
            // List operations
            | "cons"
            | "car"
            | "cdr"
            | "list"
            | "append"
            | "nth"
            | "nthcdr"
            | "length"
            | "reverse"
            | "memq"
            | "member"
            | "assq"
            | "assoc"
            | "mapcar"
            | "apply"
            | "funcall"
            | "identity"
            // Allocation
            | "make-string"
            | "make-vector"
            // String operations
            | "concat"
            | "substring"
            | "string-to-multibyte"
            | "string-to-number"
            | "number-to-string"
            | "format"
            | "char-to-string"
            | "string-to-char"
            | "upcase"
            | "downcase"
            // Buffer operations
            | "insert"
            | "point"
            | "point-min"
            | "point-max"
            | "goto-char"
            | "forward-char"
            | "backward-char"
            | "beginning-of-line"
            | "end-of-line"
            | "forward-line"
            | "buffer-string"
            | "buffer-substring"
            | "buffer-substring-no-properties"
            | "buffer-size"
            | "buffer-name"
            | "set-buffer-multibyte"
            | "char-after"
            | "char-before"
            | "bobp"
            | "eobp"
            | "bolp"
            | "eolp"
            | "delete-region"
            | "delete-char"
            | "erase-buffer"
            | "current-column"
            | "line-number-at-pos"
            | "line-beginning-position"
            | "line-end-position"
            | "pos-bol"
            | "pos-eol"
            | "narrow-to-region"
            | "widen"
            | "buffer-modified-p"
            | "set-buffer-modified-p"
            | "get-pos-property"
            | "get-char-property"
            | "current-buffer"
            | "generate-new-buffer"
            | "get-buffer"
            | "get-buffer-create"
            | "generate-new-buffer-name"
            | "rename-buffer"
            | "other-buffer"
            | "buffer-base-buffer"
            | "buffer-list"
            | "set-buffer"
            | "switch-to-buffer"
            | "kill-buffer"
            | "set-mark"
            | "mark"
            | "region-beginning"
            | "region-end"
            // Output
            | "message"
            | "prin1-to-string"
            | "princ"
            | "print"
            // Reader
            | "read"
            // More string/char ops
            | "char-equal"
            | "number-sequence"
            // More buffer ops
            | "following-char"
            | "preceding-char"
            | "buffer-last-name"
            // Stubs for terminal/display
            | "display-graphic-p"
            | "frame-parameter"
            | "selected-window"
            | "selected-frame"
            // Overlay operations
            | "make-overlay"
            | "overlayp"
            | "overlay-buffer"
            | "overlay-start"
            | "overlay-end"
            | "move-overlay"
            | "delete-overlay"
            | "delete-all-overlays"
            | "overlay-put"
            | "overlay-get"
            | "overlay-properties"
            | "overlays-at"
            | "overlays-in"
            | "next-overlay-change"
            | "previous-overlay-change"
            | "overlay-lists"
            | "overlay-recenter"
            | "remove-overlays"
            // Plist operations
            | "plist-get"
            | "plist-put"
            | "plist-member"
            // Sorting
            | "sort"
            // Misc
            | "error"
            | "signal"
            | "throw"
            | "add-hook"
            | "describe-function"
            | "ignore"
            | "intern"
            | "symbol-function"
            | "symbol-name"
            | "type-of"
            | "file-truename"
            | "random"
            | "make-hash-table"
            | "vector"
            | "aref"
            | "aset"
            | "seq-every-p"
            | "nreverse"
            | "copy-sequence"
            | "delete"
            | "delq"
            | "make-list"
            | "insert-before-markers"
    ) || is_composed_accessor_name(name)
}

/// Dispatch a builtin function call.
pub fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &super::types::Env,
) -> Result<Value, LispError> {
    // Helper: check if any argument is a float
    let has_float = |args: &[Value]| args.iter().any(|a| matches!(a, Value::Float(_)));
    // Helper: get numeric value as f64
    let as_num = |a: &Value| -> Result<f64, LispError> {
        match a {
            Value::Integer(n) => Ok(*n as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(LispError::TypeError("number".into(), a.type_name())),
        }
    };

    match name {
        // ── Arithmetic ──
        "+" => {
            if has_float(args) {
                let mut sum = 0.0;
                for a in args {
                    sum += as_num(a)?;
                }
                Ok(Value::Float(sum))
            } else {
                let mut sum: i64 = 0;
                for a in args {
                    sum = sum.wrapping_add(a.as_integer()?);
                }
                Ok(Value::Integer(sum))
            }
        }
        "-" => {
            if args.is_empty() {
                return Ok(Value::Integer(0));
            }
            if has_float(args) {
                if args.len() == 1 {
                    return Ok(Value::Float(-as_num(&args[0])?));
                }
                let mut result = as_num(&args[0])?;
                for a in &args[1..] {
                    result -= as_num(a)?;
                }
                Ok(Value::Float(result))
            } else {
                if args.len() == 1 {
                    return Ok(Value::Integer(args[0].as_integer()?.wrapping_neg()));
                }
                let mut result = args[0].as_integer()?;
                for a in &args[1..] {
                    result = result.wrapping_sub(a.as_integer()?);
                }
                Ok(Value::Integer(result))
            }
        }
        "*" => {
            if has_float(args) {
                let mut product = 1.0;
                for a in args {
                    product *= as_num(a)?;
                }
                Ok(Value::Float(product))
            } else {
                let mut product: i64 = 1;
                for a in args {
                    product = product.wrapping_mul(a.as_integer()?);
                }
                Ok(Value::Integer(product))
            }
        }
        "/" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
            }
            if has_float(args) {
                let mut result = as_num(&args[0])?;
                for a in &args[1..] {
                    let divisor = as_num(a)?;
                    if divisor == 0.0 {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(Value::Float(result))
            } else {
                let mut result = args[0].as_integer()?;
                for a in &args[1..] {
                    let divisor = a.as_integer()?;
                    if divisor == 0 {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(Value::Integer(result))
            }
        }
        "%" | "mod" => {
            need_args(name, args, 2)?;
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            if b == 0 {
                return Err(LispError::Signal("Division by zero".into()));
            }
            Ok(Value::Integer(a.rem_euclid(b)))
        }
        "1+" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(args[0].as_integer()?.wrapping_add(1)))
        }
        "1-" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(args[0].as_integer()?.wrapping_sub(1)))
        }
        "max" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("max".into(), 0));
            }
            let mut result = args[0].as_integer()?;
            for a in &args[1..] {
                result = result.max(a.as_integer()?);
            }
            Ok(Value::Integer(result))
        }
        "min" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("min".into(), 0));
            }
            let mut result = args[0].as_integer()?;
            for a in &args[1..] {
                result = result.min(a.as_integer()?);
            }
            Ok(Value::Integer(result))
        }
        "abs" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(args[0].as_integer()?.abs()))
        }

        // ── Comparison ──
        "=" => {
            need_args(name, args, 2)?;
            let a = as_num(&args[0])?;
            let b = as_num(&args[1])?;
            Ok(if a == b { Value::T } else { Value::Nil })
        }
        "<" => {
            need_args(name, args, 2)?;
            Ok(if as_num(&args[0])? < as_num(&args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">" => {
            need_args(name, args, 2)?;
            Ok(if as_num(&args[0])? > as_num(&args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "<=" => {
            need_args(name, args, 2)?;
            Ok(if as_num(&args[0])? <= as_num(&args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">=" => {
            need_args(name, args, 2)?;
            Ok(if as_num(&args[0])? >= as_num(&args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "/=" => {
            need_args(name, args, 2)?;
            Ok(if as_num(&args[0])? != as_num(&args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }

        // ── Equality ──
        "eq" => {
            need_args(name, args, 2)?;
            // eq tests identity — for our purposes, value equality on atoms
            Ok(if args[0] == args[1] {
                Value::T
            } else {
                Value::Nil
            })
        }
        "equal" => {
            need_args(name, args, 2)?;
            Ok(if values_equal(interp, &args[0], &args[1]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string=" | "string-equal" => {
            need_args(name, args, 2)?;
            let a = args[0].as_string()?;
            let b = args[1].as_string()?;
            Ok(if a == b { Value::T } else { Value::Nil })
        }

        // ── Type predicates ──
        "null" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "not" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "integerp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_integer() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "numberp" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_integer() || matches!(args[0], Value::Float(_)) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "floatp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Float(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "stringp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_string() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "symbolp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_symbol() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "consp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_cons() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "listp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_list() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "bufferp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Buffer(_, _)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "buffer-live-p" => {
            need_args(name, args, 1)?;
            Ok(if matches!(&args[0], Value::Buffer(id, _) if interp.has_buffer_id(*id)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        "zerop" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(0) => Value::T,
                Value::Float(f) if *f == 0.0 => Value::T,
                _ => Value::Nil,
            })
        }

        "natnump" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 => Value::T,
                _ => Value::Nil,
            })
        }

        "atom" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_cons() { Value::Nil } else { Value::T })
        }

        "nlistp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_list() { Value::Nil } else { Value::T })
        }

        "characterp" => {
            need_args(name, args, 1)?;
            // In Emacs, characters are integers 0..#x3FFFFF
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 && *n <= 0x3F_FFFF => Value::T,
                _ => Value::Nil,
            })
        }

        // ── List operations ──
        "cons" => {
            need_args(name, args, 2)?;
            Ok(Value::cons(args[0].clone(), args[1].clone()))
        }
        "car" => {
            need_args(name, args, 1)?;
            args[0].car()
        }
        "cdr" => {
            need_args(name, args, 1)?;
            args[0].cdr()
        }
        "identity" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "list" => Ok(Value::list(args.iter().cloned())),
        "append" => {
            let mut items: Vec<Value> = Vec::new();
            for (i, a) in args.iter().enumerate() {
                if i == args.len() - 1 {
                    // Last arg can be a non-list (dotted)
                    if a.is_list() {
                        items.extend(a.to_vec()?);
                    } else {
                        // Build the list so far and cons the last as cdr
                        let mut result = a.clone();
                        for item in items.into_iter().rev() {
                            result = Value::cons(item, result);
                        }
                        return Ok(result);
                    }
                } else {
                    items.extend(a.to_vec()?);
                }
            }
            Ok(Value::list(items))
        }
        "nth" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            let list = args[1].to_vec()?;
            Ok(list.get(n).cloned().unwrap_or(Value::Nil))
        }
        "nthcdr" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            let mut current = args[1].clone();
            for _ in 0..n {
                current = current.cdr()?;
            }
            Ok(current)
        }
        "length" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::Integer(s.chars().count() as i64)),
                Value::Nil => Ok(Value::Integer(0)),
                Value::Cons(_, _) => Ok(Value::Integer(args[0].to_vec()?.len() as i64)),
                _ => Err(LispError::TypeError("sequence".into(), args[0].type_name())),
            }
        }
        "reverse" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.reverse();
            Ok(Value::list(items))
        }
        "memq" | "member" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            for (i, item) in items.iter().enumerate() {
                if *item == args[0] {
                    return Ok(Value::list(items[i..].iter().cloned()));
                }
            }
            Ok(Value::Nil)
        }
        "assq" | "assoc" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            for item in &items {
                if item.car()? == args[0] {
                    return Ok(item.clone());
                }
            }
            Ok(Value::Nil)
        }
        "mapcar" => {
            need_args(name, args, 2)?;
            let list = args[1].to_vec()?;
            let mut results = Vec::new();
            for item in list {
                let call_expr =
                    Value::list([args[0].clone(), Value::list([Value::symbol("quote"), item])]);
                let result = interp.eval(&call_expr, &mut Vec::new())?;
                results.push(result);
            }
            Ok(Value::list(results))
        }
        "apply" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("apply".into(), args.len()));
            }
            let func = &args[0];
            let last = &args[args.len() - 1];
            let mut all_args: Vec<Value> = args[1..args.len() - 1].to_vec();
            all_args.extend(last.to_vec()?);

            // Build a call expression
            let mut call_items = vec![func.clone()];
            for a in &all_args {
                call_items.push(Value::list([Value::symbol("quote"), a.clone()]));
            }
            interp.eval(&Value::list(call_items), &mut Vec::new())
        }
        "funcall" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("funcall".into(), 0));
            }
            let func = &args[0];
            let mut call_items = vec![func.clone()];
            for a in &args[1..] {
                call_items.push(Value::list([Value::symbol("quote"), a.clone()]));
            }
            interp.eval(&Value::list(call_items), &mut Vec::new())
        }

        // ── Allocation ──
        "make-string" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "make-string".into(),
                    args.len(),
                ));
            }
            let length = args[0].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            let init = args[1].as_integer()?;
            let c = char::from_u32(init as u32).unwrap_or('\0');
            let s: String = std::iter::repeat_n(c, length as usize).collect();
            Ok(Value::String(s))
        }
        "make-vector" => {
            need_args(name, args, 2)?;
            let length = args[0].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            let init = args[1].clone();
            let items: Vec<Value> = std::iter::repeat_n(init, length as usize).collect();
            // Represent as (vector el1 el2 ...) like the reader does for #(...)
            let mut result = vec![Value::symbol("vector")];
            result.extend(items);
            Ok(Value::list(result))
        }

        // ── String operations ──
        "concat" => {
            let mut result = String::new();
            for a in args {
                match a {
                    Value::String(s) => result.push_str(s),
                    Value::Nil => {} // (concat nil) is ""
                    _ => result.push_str(&a.to_string()),
                }
            }
            Ok(Value::String(result))
        }
        "substring" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs("substring".into(), args.len()));
            }
            let s = args[0].as_string()?;
            let chars: Vec<char> = s.chars().collect();
            let from = if args.len() > 1 {
                args[1].as_integer()? as usize
            } else {
                0
            };
            let to = if args.len() > 2 {
                args[2].as_integer()? as usize
            } else {
                chars.len()
            };
            let to = to.min(chars.len());
            let from = from.min(to);
            Ok(Value::String(chars[from..to].iter().collect()))
        }
        "string-to-multibyte" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_string()))
        }
        "string-to-number" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            let n = s.parse::<i64>().unwrap_or(0);
            Ok(Value::Integer(n))
        }
        "number-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_integer()?.to_string()))
        }
        "format" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("format".into(), 0));
            }
            let fmt = args[0].as_string()?;
            let mut result = String::new();
            let mut arg_idx = 1;
            let chars: Vec<char> = fmt.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] != '%' || i + 1 >= chars.len() {
                    result.push(chars[i]);
                    i += 1;
                    continue;
                }
                i += 1; // skip '%'

                if chars[i] == '%' {
                    result.push('%');
                    i += 1;
                    continue;
                }

                // Parse optional N$ positional arg
                let mut positional: Option<usize> = None;
                if chars[i].is_ascii_digit() {
                    let mut n = 0u64;
                    let digit_start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        n = n
                            .saturating_mul(10)
                            .saturating_add(chars[i] as u64 - '0' as u64);
                        i += 1;
                    }
                    if i < chars.len() && chars[i] == '$' {
                        if n == 0 || n > args.len() as u64 {
                            return Err(LispError::Signal(
                                "Not enough arguments for format string".into(),
                            ));
                        }
                        positional = Some(n as usize);
                        i += 1;
                    } else {
                        i = digit_start; // not positional, rewind
                    }
                } else if chars[i] == '$' {
                    return Err(LispError::Signal("Invalid format operation %$".into()));
                } else if chars[i] == '-' {
                    // Check for %-N$s which is invalid
                    let save = i;
                    i += 1;
                    let mut has_digits = false;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        has_digits = true;
                        i += 1;
                    }
                    if has_digits && i < chars.len() && chars[i] == '$' {
                        return Err(LispError::Signal("Invalid format operation %$".into()));
                    }
                    i = save; // rewind, handle as flag below
                }

                // Parse flags
                let mut flag_hash = false;
                let mut flag_zero = false;
                let mut flag_minus = false;
                let mut flag_plus = false;
                let mut flag_space = false;
                while i < chars.len() {
                    match chars[i] {
                        '#' => flag_hash = true,
                        '0' => flag_zero = true,
                        '-' => flag_minus = true,
                        '+' => flag_plus = true,
                        ' ' => flag_space = true,
                        _ => break,
                    }
                    i += 1;
                }

                // Parse width
                let mut width: usize = 0;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    width = width * 10 + (chars[i] as usize - '0' as usize);
                    i += 1;
                }

                // Skip precision (e.g., .2)
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }

                if i >= chars.len() {
                    break;
                }

                let conv = chars[i];
                i += 1;

                // Get the argument
                let aidx = if let Some(n) = positional {
                    n
                } else {
                    let idx = arg_idx;
                    arg_idx += 1;
                    idx
                };
                if aidx >= args.len() {
                    return Err(LispError::Signal(
                        "Not enough arguments for format string".into(),
                    ));
                }
                let arg = &args[aidx];

                // Convert to integer, requiring integer type for some conversions
                let require_int = |a: &Value| -> Result<i64, LispError> {
                    match a {
                        Value::Integer(n) => Ok(*n),
                        Value::Float(f) => Ok(*f as i64),
                        _ => Err(LispError::TypeError("integer".into(), a.type_name())),
                    }
                };

                let formatted = match conv {
                    's' => match arg {
                        Value::String(s) => s.clone(),
                        Value::Integer(n) => n.to_string(),
                        other => other.to_string(),
                    },
                    'S' => arg.to_string(),
                    'd' => {
                        let n = require_int(arg)?;
                        n.to_string()
                    }
                    'o' => {
                        let n = require_int(arg)?;
                        let abs_bits = if n < 0 {
                            format!("{:o}", n as u64)
                        } else {
                            format!("{:o}", n)
                        };
                        let sign = if n < 0 {
                            "-"
                        } else if flag_plus {
                            "+"
                        } else {
                            ""
                        };
                        if n < 0 {
                            abs_bits // Emacs uses unsigned repr for negative octal
                        } else {
                            format!("{}{}", sign, abs_bits)
                        }
                    }
                    'x' => {
                        let n = require_int(arg)?;
                        let prefix = if flag_hash && n != 0 { "0x" } else { "" };
                        let abs_bits = if n < 0 {
                            format!("{:x}", n as u64)
                        } else {
                            format!("{:x}", n)
                        };
                        format!("{}{}", prefix, abs_bits)
                    }
                    'X' => {
                        let n = require_int(arg)?;
                        let prefix = if flag_hash && n != 0 { "0X" } else { "" };
                        let abs_bits = if n < 0 {
                            format!("{:X}", n as u64)
                        } else {
                            format!("{:X}", n)
                        };
                        format!("{}{}", prefix, abs_bits)
                    }
                    'b' | 'B' => {
                        let n = require_int(arg)?;
                        let upper = conv == 'B';
                        let abs_n = n.unsigned_abs();
                        let bits = format!("{:b}", abs_n);
                        let sign_str = if n < 0 {
                            "-"
                        } else if flag_plus {
                            "+"
                        } else {
                            ""
                        };
                        let prefix = if flag_hash && n != 0 {
                            if upper { "0B" } else { "0b" }
                        } else {
                            ""
                        };
                        format!("{}{}{}", sign_str, prefix, bits)
                    }
                    'c' => {
                        let n = match arg {
                            Value::Integer(n) => *n,
                            Value::Float(_) => {
                                return Err(LispError::TypeError("integer".into(), "float".into()));
                            }
                            _ => {
                                return Err(LispError::TypeError(
                                    "integer".into(),
                                    arg.type_name(),
                                ));
                            }
                        };
                        char::from_u32(n as u32)
                            .map(|c| c.to_string())
                            .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))?
                    }
                    _ => {
                        // Unknown conversion, pass through
                        if let Some(pos) = positional {
                            format!("%{}${}", pos, conv)
                        } else {
                            format!("%{}", conv)
                        }
                    }
                };

                // Apply width/padding
                if width > 0 && formatted.len() < width {
                    let padding = width - formatted.len();
                    if flag_minus {
                        // Left-align: content then spaces
                        result.push_str(&formatted);
                        for _ in 0..padding {
                            result.push(' ');
                        }
                    } else if flag_zero && !flag_minus {
                        // Zero-pad: put zeros after sign/prefix, before digits
                        // Find the split point: sign + prefix
                        let s = &formatted;
                        let mut prefix_end = 0;
                        if s.starts_with('-') || s.starts_with('+') {
                            prefix_end = 1;
                        }
                        if s[prefix_end..].starts_with("0x")
                            || s[prefix_end..].starts_with("0X")
                            || s[prefix_end..].starts_with("0b")
                            || s[prefix_end..].starts_with("0B")
                        {
                            prefix_end += 2;
                        }
                        result.push_str(&s[..prefix_end]);
                        for _ in 0..padding {
                            result.push('0');
                        }
                        result.push_str(&s[prefix_end..]);
                    } else if flag_plus && matches!(conv, 'b' | 'B' | 'd' | 'o' | 'x' | 'X') {
                        // Right-align with + flag: spaces then sign+digits
                        for _ in 0..padding {
                            result.push(' ');
                        }
                        result.push_str(&formatted);
                    } else if flag_space
                        && matches!(conv, 'b' | 'B' | 'd' | 'o' | 'x' | 'X')
                        && !formatted.starts_with('-')
                    {
                        // Space flag: like right-align but ensure at least one space for sign
                        for _ in 0..padding {
                            result.push(' ');
                        }
                        result.push_str(&formatted);
                    } else {
                        // Right-align with spaces
                        for _ in 0..padding {
                            result.push(' ');
                        }
                        result.push_str(&formatted);
                    }
                } else {
                    result.push_str(&formatted);
                }
            }
            Ok(Value::String(result))
        }
        "char-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-to-char" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            Ok(Value::Integer(
                s.chars().next().map(|c| c as i64).unwrap_or(0),
            ))
        }
        "upcase" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_uppercase()))
        }
        "downcase" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_lowercase()))
        }

        // ── Buffer operations ──
        "insert" => {
            for a in args {
                let s = match a {
                    Value::String(s) => s.clone(),
                    Value::Integer(n) => char::from_u32(*n as u32)
                        .map(|c| c.to_string())
                        .unwrap_or_default(),
                    _ => a.to_string(),
                };
                interp.buffer.insert(&s);
            }
            Ok(Value::Nil)
        }
        "point" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "point-min" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "point-max" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "goto-char" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(interp.buffer.point() as i64))
        }
        "forward-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(n as isize) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "backward-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.forward_char(-(n as isize)) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "beginning-of-line" => {
            interp.buffer.beginning_of_line();
            Ok(Value::Nil)
        }
        "end-of-line" => {
            interp.buffer.end_of_line();
            Ok(Value::Nil)
        }
        "forward-line" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let remaining = interp.buffer.forward_line(n as isize);
            Ok(Value::Integer(remaining as i64))
        }
        "buffer-string" => Ok(Value::String(interp.buffer.buffer_string())),
        "buffer-substring" | "buffer-substring-no-properties" => {
            need_args(name, args, 2)?;
            let from = args[0].as_integer()? as usize;
            let to = args[1].as_integer()? as usize;
            match interp.buffer.buffer_substring(from, to) {
                Ok(s) => Ok(Value::String(s)),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "buffer-size" => Ok(Value::Integer(interp.buffer.buffer_size() as i64)),
        "buffer-name" => {
            if !args.is_empty()
                && let Value::Buffer(_, name) = &args[0]
            {
                return Ok(Value::String(name.clone()));
            }
            Ok(Value::String(interp.buffer.name.clone()))
        }
        "set-buffer-multibyte" => Ok(args.first().cloned().unwrap_or(Value::Nil)),
        "char-after" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            match interp.buffer.char_at(pos) {
                Some(c) => Ok(Value::Integer(c as i64)),
                None => Ok(Value::Nil),
            }
        }
        "char-before" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            if pos <= interp.buffer.point_min() {
                Ok(Value::Nil)
            } else {
                match interp.buffer.char_at(pos - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Nil),
                }
            }
        }
        "bobp" => Ok(if interp.buffer.bobp() {
            Value::T
        } else {
            Value::Nil
        }),
        "eobp" => Ok(if interp.buffer.eobp() {
            Value::T
        } else {
            Value::Nil
        }),
        "bolp" => Ok(if interp.buffer.bolp() {
            Value::T
        } else {
            Value::Nil
        }),
        "eolp" => Ok(if interp.buffer.eolp() {
            Value::T
        } else {
            Value::Nil
        }),
        "delete-region" => {
            need_args(name, args, 2)?;
            let from = args[0].as_integer()? as usize;
            let to = args[1].as_integer()? as usize;
            match interp.buffer.delete_region(from, to) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "delete-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            match interp.buffer.delete_char(n as isize) {
                Ok(_) => Ok(Value::Nil),
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "erase-buffer" => {
            let size = interp.buffer.buffer_size();
            if size > 0 {
                let min = interp.buffer.point_min();
                let max = interp.buffer.point_max();
                interp.buffer.delete_region(min, max)?;
            }
            Ok(Value::Nil)
        }
        "current-column" => {
            // Look up tab-width from environment (default 8)
            let tab_width = interp
                .lookup_var("tab-width", env)
                .and_then(|v| v.as_integer().ok())
                .unwrap_or(8) as usize;
            let tab_width = tab_width.max(1);

            let pt = interp.buffer.point();
            let bol = {
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let bol = interp.buffer.point();
                interp.buffer.goto_char(saved);
                bol
            };

            let mut col: usize = 0;
            for pos in bol..pt {
                match interp.buffer.char_at(pos) {
                    Some('\t') => col = (col / tab_width + 1) * tab_width,
                    Some(_) => col += 1,
                    None => break,
                }
            }
            Ok(Value::Integer(col as i64))
        }
        "line-number-at-pos" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            Ok(Value::Integer(interp.buffer.line_number_at_pos(pos) as i64))
        }
        "line-beginning-position" | "pos-bol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            let shortage = if count != 0 {
                interp.buffer.forward_line(count)
            } else {
                0
            };
            // If forward_line overshot (couldn't find enough lines),
            // point is already at point-max/point-min — don't move it back.
            if shortage == 0 || (count > 0 && interp.buffer.point() < interp.buffer.point_max()) {
                interp.buffer.beginning_of_line();
            }
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "line-end-position" | "pos-eol" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let saved = interp.buffer.point();
            let count = (n - 1) as isize;
            if count != 0 {
                interp.buffer.forward_line(count);
            }
            interp.buffer.end_of_line();
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "narrow-to-region" => {
            need_args(name, args, 2)?;
            let start = args[0].as_integer()? as usize;
            let end = args[1].as_integer()? as usize;
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }
        "widen" => {
            interp.buffer.widen();
            Ok(Value::Nil)
        }
        "buffer-modified-p" => Ok(if interp.buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        }),
        "set-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
            }
            // Setting to t: just leave the modified state as-is (it's already modified if changed)
            Ok(Value::Nil)
        }
        "get-pos-property" | "get-char-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()? as usize;
            let prop = args[1].as_symbol()?.to_string();
            let buffer_id = if let Some(object) = args.get(2) {
                if object.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(object)?
                }
            } else {
                interp.current_buffer_id()
            };
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            Ok(highest_priority_overlay_property(
                buffer,
                pos,
                &prop,
                name == "get-pos-property",
            )
            .unwrap_or(Value::Nil))
        }
        "current-buffer" => {
            Ok(Value::Buffer(
                interp.current_buffer_id(),
                interp.buffer.name.clone(),
            ))
        }
        "generate-new-buffer" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let buf_name = if interp.has_buffer(base) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                base.to_string()
            };
            let (id, _) = interp.create_buffer(&buf_name);
            Ok(Value::Buffer(id, buf_name))
        }
        "get-buffer" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Buffer(id, _) if interp.has_buffer_id(*id) => Ok(args[0].clone()),
                Value::Buffer(_, _) => Ok(Value::Nil),
                Value::String(s) => match interp.find_buffer(s) {
                    Some((id, name)) => Ok(Value::Buffer(id, name)),
                    None => Ok(Value::Nil),
                },
                _ => Err(LispError::TypeError(
                    "string-or-buffer".into(),
                    args[0].type_name(),
                )),
            }
        }
        "get-buffer-create" => {
            need_args(name, args, 1)?;
            let buf_name = match &args[0] {
                Value::Buffer(_, n) => n.clone(),
                Value::String(s) => s.clone(),
                _ => {
                    return Err(LispError::TypeError(
                        "string-or-buffer".into(),
                        args[0].type_name(),
                    ));
                }
            };
            if let Some((id, name)) = interp.find_buffer(&buf_name) {
                Ok(Value::Buffer(id, name))
            } else {
                let (id, _) = interp.create_buffer(&buf_name);
                Ok(Value::Buffer(id, buf_name))
            }
        }
        "generate-new-buffer-name" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let ignore = if args.len() > 1 {
                args[1].as_string().ok().map(|s| s.to_string())
            } else {
                None
            };
            if !interp.has_buffer(base) || ignore.as_deref() == Some(base) {
                Ok(Value::String(base.to_string()))
            } else {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) || ignore.as_deref() == Some(&candidate) {
                        break Ok(Value::String(candidate));
                    }
                    n += 1;
                }
            }
        }
        "rename-buffer" => {
            need_args(name, args, 1)?;
            let new_name = args[0].as_string()?;
            if new_name.is_empty() {
                return Err(LispError::Signal("Empty string for buffer name".into()));
            }
            let old_name = interp.buffer.name.clone();
            let unique = args.len() > 1 && args[1].is_truthy();
            let final_name = if interp.has_buffer(new_name) && new_name != old_name {
                if unique {
                    let mut n = 2;
                    loop {
                        let candidate = format!("{}<{}>", new_name, n);
                        if !interp.has_buffer(&candidate) {
                            break candidate;
                        }
                        n += 1;
                    }
                } else {
                    return Err(LispError::Signal(format!(
                        "Buffer name `{}' is in use",
                        new_name
                    )));
                }
            } else {
                new_name.to_string()
            };
            if let Some(pos) = interp.buffer_list.iter().position(|(_, n)| *n == old_name) {
                interp.buffer_list[pos].1 = final_name.clone();
            }
            interp.buffer.last_name = Some(old_name);
            interp.buffer.name = final_name.clone();
            Ok(Value::String(final_name))
        }
        "other-buffer" => {
            let exclude = if !args.is_empty() {
                match &args[0] {
                    Value::Buffer(_, n) => n.clone(),
                    _ => interp.buffer.name.clone(),
                }
            } else {
                interp.buffer.name.clone()
            };
            for (id, buf_name) in &interp.buffer_list {
                if *buf_name != exclude && !buf_name.starts_with(' ') {
                    return Ok(Value::Buffer(*id, buf_name.clone()));
                }
            }
            Ok(Value::Buffer(0, "*scratch*".into()))
        }
        "buffer-base-buffer" => {
            // No indirect buffers supported yet
            Ok(Value::Nil)
        }
        "buffer-list" => {
            let bufs: Vec<Value> = interp
                .buffer_list
                .iter()
                .map(|(id, n)| Value::Buffer(*id, n.clone()))
                .collect();
            Ok(Value::list(bufs))
        }
        "set-buffer" => {
            need_args(name, args, 1)?;
            let id = interp.resolve_buffer_id(&args[0])?;
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "switch-to-buffer" => {
            need_args(name, args, 1)?;
            let id = interp.resolve_buffer_id(&args[0])?;
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "kill-buffer" => {
            let id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            interp.kill_buffer_id(id);
            Ok(Value::T)
        }
        "set-mark" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            interp.buffer.set_mark(pos);
            Ok(Value::Nil)
        }
        "mark" => Ok(match interp.buffer.mark() {
            Some(m) => Value::Integer(m as i64),
            None => Value::Nil,
        }),
        "region-beginning" => match interp.buffer.region() {
            Some((beg, _)) => Ok(Value::Integer(beg as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },
        "region-end" => match interp.buffer.region() {
            Some((_, end)) => Ok(Value::Integer(end as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },

        // ── Output ──
        "message" | "princ" | "print" => {
            // Stub: just return the first arg
            if args.is_empty() {
                Ok(Value::Nil)
            } else {
                Ok(args[0].clone())
            }
        }
        "prin1-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].to_string()))
        }

        // ── More string/char ops ──
        "char-equal" => {
            need_args(name, args, 2)?;
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            let case_fold = interp
                .lookup_var("case-fold-search", env)
                .map(|v| v.is_truthy())
                .unwrap_or(false);
            let eq = if case_fold {
                a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
            } else {
                a == b
            };
            Ok(if eq { Value::T } else { Value::Nil })
        }
        "number-sequence" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "number-sequence".into(),
                    args.len(),
                ));
            }
            let from = args[0].as_integer()?;
            let to = if args.len() > 1 {
                args[1].as_integer()?
            } else {
                from
            };
            let step = if args.len() > 2 {
                args[2].as_integer()?
            } else if from <= to {
                1
            } else {
                -1
            };
            if step == 0 {
                return Err(LispError::Signal(
                    "number-sequence: step must not be 0".into(),
                ));
            }
            let mut result = Vec::new();
            let mut i = from;
            if step > 0 {
                while i <= to {
                    result.push(Value::Integer(i));
                    i += step;
                }
            } else {
                while i >= to {
                    result.push(Value::Integer(i));
                    i += step;
                }
            }
            Ok(Value::list(result))
        }

        // ── More buffer ops ──
        "following-char" => match interp.buffer.char_at(interp.buffer.point()) {
            Some(c) => Ok(Value::Integer(c as i64)),
            None => Ok(Value::Integer(0)),
        },
        "preceding-char" => {
            let pt = interp.buffer.point();
            if pt <= interp.buffer.point_min() {
                Ok(Value::Integer(0))
            } else {
                match interp.buffer.char_at(pt - 1) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Integer(0)),
                }
            }
        }
        "buffer-last-name" => Ok(Value::String(
            interp
                .buffer
                .last_name
                .clone()
                .unwrap_or_else(|| interp.buffer.name.clone()),
        )),

        // ── Display stubs ──
        "display-graphic-p" | "frame-parameter" => Ok(Value::Nil),
        "selected-window" => Ok(Value::Symbol("window".into())),
        "selected-frame" => Ok(Value::Symbol("frame".into())),

        // ── Reader ──
        "read" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            let mut reader = super::reader::Reader::new(s);
            match reader.read()? {
                Some(val) => Ok(val),
                None => Err(LispError::EndOfInput),
            }
        }

        // ── Misc ──
        "error" => {
            let msg = if args.is_empty() {
                "error".to_string()
            } else if let Ok(fmt) = args[0].as_string() {
                // Simple format
                fmt.to_string()
            } else {
                args[0].to_string()
            };
            Err(LispError::Signal(msg))
        }
        "signal" | "throw" => {
            let msg = if args.len() >= 2 {
                format!("{}: {}", args[0], args[1])
            } else if !args.is_empty() {
                args[0].to_string()
            } else {
                "signal".into()
            };
            Err(LispError::Signal(msg))
        }
        "intern" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            Ok(Value::Symbol(s.to_string()))
        }
        "ignore" => Ok(Value::Nil),
        "describe-function" => {
            let _ = get_or_create_buffer(interp, "*Help*");
            Ok(Value::Nil)
        }
        "add-hook" => Ok(Value::Nil),
        "symbol-function" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if is_builtin(symbol) {
                Value::BuiltinFunc(symbol.to_string())
            } else {
                Value::String(format!("#<function {}>", symbol))
            })
        }
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(s.to_string()))
        }
        "file-truename" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_string()))
        }
        "make-hash-table" => Ok(Value::String("#<hash-table>".into())),
        "type-of" => {
            need_args(name, args, 1)?;
            let name = match &args[0] {
                Value::Nil => "symbol",
                Value::T => "symbol",
                Value::Integer(_) => "integer",
                Value::Float(_) => "float",
                Value::String(_) => "string",
                Value::Symbol(_) => "symbol",
                Value::Cons(_, _) => "cons",
                Value::BuiltinFunc(_) => "subr",
                Value::Lambda(_, _, _) => "cons", // Emacs closures are cons cells
                Value::Buffer(_, _) => "buffer",
                Value::Overlay(_) => "overlay",
            };
            Ok(Value::Symbol(name.into()))
        }

        // ── Overlay operations ──

        "make-overlay" => {
            // (make-overlay BEG END &optional BUFFER FRONT-ADVANCE REAR-ADVANCE)
            if !(2..=5).contains(&args.len()) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let beg = args[0].as_integer()?;
            let end = args[1].as_integer()?;
            let buffer_id = if let Some(buffer_arg) = args.get(2) {
                if buffer_arg.is_nil() {
                    interp.current_buffer_id()
                } else if matches!(buffer_arg, Value::Buffer(_, _)) {
                    interp.resolve_buffer_id(buffer_arg)?
                } else {
                    return Err(LispError::TypeError(
                        "buffer".into(),
                        buffer_arg.type_name(),
                    ));
                }
            } else {
                interp.current_buffer_id()
            };
            let front_advance = args.get(3).is_some_and(|v| v.is_truthy());
            let rear_advance = args.get(4).is_some_and(|v| v.is_truthy());
            let ov_id = interp.alloc_overlay_id();
            let (beg, end) = {
                let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                    LispError::Signal(format!("No buffer with id {}", buffer_id))
                })?;
                clamp_overlay_range(buffer, beg, end)
            };
            let ov = crate::overlay::Overlay::new(
                ov_id,
                beg,
                end,
                buffer_id,
                front_advance,
                rear_advance,
            );
            interp
                .get_buffer_by_id_mut(buffer_id)
                .expect("resolved live buffer id")
                .overlays
                .push(ov);
            Ok(Value::Overlay(ov_id))
        }

        "overlayp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(&args[0], Value::Overlay(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        "overlay-buffer" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => {
                    let buf_id = ov.buffer_id.unwrap_or(0);
                    let buf_name = interp.buffer_list.iter()
                        .find(|(id, _)| *id == buf_id)
                        .map_or("*unknown*".to_string(), |(_, n)| n.clone());
                    Ok(Value::Buffer(buf_id, buf_name))
                }
                _ => Ok(Value::Nil),
            }
        }

        "overlay-start" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => Ok(Value::Integer(ov.beg as i64)),
                _ => Ok(Value::Nil),
            }
        }

        "overlay-end" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) if !ov.is_dead() => Ok(Value::Integer(ov.end as i64)),
                _ => Ok(Value::Nil),
            }
        }

        "move-overlay" => {
            // (move-overlay OVERLAY BEG END &optional BUFFER)
            if !(3..=4).contains(&args.len()) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let target_buffer_id = if let Some(buffer_arg) = args.get(3) {
                if buffer_arg.is_nil() {
                    interp.current_buffer_id()
                } else if matches!(buffer_arg, Value::Buffer(_, _)) {
                    interp.resolve_buffer_id(buffer_arg)?
                } else {
                    return Err(LispError::TypeError("buffer".into(), buffer_arg.type_name()));
                }
            } else {
                interp.current_buffer_id()
            };
            let beg = args[1].as_integer()?;
            let end = args[2].as_integer()?;
            let (beg, end) = {
                let buffer = interp.get_buffer_by_id(target_buffer_id).ok_or_else(|| {
                    LispError::Signal(format!("No buffer with id {}", target_buffer_id))
                })?;
                clamp_overlay_range(buffer, beg, end)
            };
            let mut overlay = take_overlay(interp, ov_id).unwrap_or_else(|| {
                crate::overlay::Overlay::new(ov_id, beg, end, target_buffer_id, false, false)
            });
            overlay.beg = beg;
            overlay.end = end;
            overlay.buffer_id = Some(target_buffer_id);
            interp
                .get_buffer_by_id_mut(target_buffer_id)
                .expect("resolved live buffer id")
                .overlays
                .push(overlay);
            Ok(Value::Overlay(ov_id))
        }

        "delete-overlay" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            if let Some(ov) = interp.find_overlay_mut(ov_id) {
                ov.buffer_id = None;
            }
            Ok(Value::Nil)
        }

        "delete-all-overlays" => {
            // Remove all overlays (or mark them dead)
            interp.buffer.overlays.clear();
            Ok(Value::Nil)
        }

        "overlay-put" => {
            need_args(name, args, 3)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let key = match &args[1] {
                Value::Symbol(s) => s.clone(),
                _ => return Err(LispError::TypeError("symbol".into(), args[1].type_name())),
            };
            let value = args[2].clone();
            if let Some(ov) = interp.find_overlay_mut(ov_id) {
                ov.put_prop(&key, value.clone());
            }
            Ok(value)
        }

        "overlay-get" => {
            need_args(name, args, 2)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            let key = match &args[1] {
                Value::Symbol(s) => s.clone(),
                _ => return Err(LispError::TypeError("symbol".into(), args[1].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) => Ok(ov.get_prop(&key).cloned().unwrap_or(Value::Nil)),
                None => Ok(Value::Nil),
            }
        }

        "overlay-properties" => {
            need_args(name, args, 1)?;
            let ov_id = match &args[0] {
                Value::Overlay(id) => *id,
                _ => return Err(LispError::TypeError("overlay".into(), args[0].type_name())),
            };
            match interp.find_overlay(ov_id) {
                Some(ov) => {
                    let mut items = Vec::new();
                    for (k, v) in &ov.plist {
                        items.push(Value::Symbol(k.clone()));
                        items.push(v.clone());
                    }
                    Ok(Value::list(items))
                }
                None => Ok(Value::Nil),
            }
        }

        "overlays-at" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let result: Vec<Value> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| !ov.is_dead() && ov.beg <= pos && pos < ov.end)
                .map(|ov| Value::Overlay(ov.id))
                .collect();
            Ok(Value::list(result))
        }

        "overlays-in" => {
            need_args(name, args, 2)?;
            let beg = args[0].as_integer()? as usize;
            let end = args[1].as_integer()? as usize;
            // Z is the un-narrowed buffer end (1-based).
            let z = interp.buffer.size_total() + 1;
            let result: Vec<Value> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| {
                    if ov.is_dead() {
                        return false;
                    }
                    if ov.beg == ov.end {
                        // Zero-length overlay at pos P:
                        // Include if P is in [beg, end), or if beg==end and P==beg,
                        // or if P==end and end >= Z (at the real buffer end).
                        return (ov.beg >= beg && ov.beg < end)
                            || (beg == end && ov.beg == beg)
                            || (ov.beg == end && end >= z);
                    }
                    // Non-empty overlay: include if it overlaps [beg, end)
                    ov.beg < end && ov.end > beg
                })
                .map(|ov| Value::Overlay(ov.id))
                .collect();
            Ok(Value::list(result))
        }

        "next-overlay-change" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let zv = interp.buffer.point_max();
            let mut next = zv;
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.beg > pos && ov.beg < next {
                    next = ov.beg;
                }
                if ov.end > pos && ov.end < next {
                    next = ov.end;
                }
            }
            Ok(Value::Integer(next as i64))
        }

        "previous-overlay-change" => {
            need_args(name, args, 1)?;
            let pos = args[0].as_integer()? as usize;
            let begv = interp.buffer.point_min();
            let mut prev = begv;
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.beg < pos && ov.beg > prev {
                    prev = ov.beg;
                }
                if ov.end < pos && ov.end > prev {
                    prev = ov.end;
                }
            }
            Ok(Value::Integer(prev as i64))
        }

        "overlay-lists" => {
            // Returns (BEFORE-LIST . AFTER-LIST) relative to point.
            let pt = interp.buffer.point();
            let mut before = Vec::new();
            let mut after = Vec::new();
            for ov in &interp.buffer.overlays {
                if ov.is_dead() {
                    continue;
                }
                if ov.end <= pt {
                    before.push(Value::Overlay(ov.id));
                } else {
                    after.push(Value::Overlay(ov.id));
                }
            }
            Ok(Value::cons(Value::list(before), Value::list(after)))
        }

        "overlay-recenter" => {
            // In real Emacs this recenters the overlay cache. We're a no-op.
            Ok(Value::Nil)
        }

        "remove-overlays" => {
            // (remove-overlays &optional BEG END NAME VAL)
            let beg = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point_min()
            } else {
                args[0].as_integer()? as usize
            };
            let end = if args.len() < 2 || args[1].is_nil() {
                interp.buffer.point_max()
            } else {
                args[1].as_integer()? as usize
            };
            let filter_name = if args.len() >= 3 {
                args[2].as_symbol().ok().map(|s| s.to_string())
            } else {
                None
            };
            let filter_val = args.get(3).cloned();
            let zv = interp.buffer.point_max();

            // Collect IDs to delete (fully contained or matching)
            let ids_to_delete: Vec<u64> = interp
                .buffer
                .overlays
                .iter()
                .filter(|ov| {
                    if ov.is_dead() {
                        return false;
                    }
                    // Check property filter
                    if let Some(ref fname) = filter_name {
                        let val = ov.get_prop(fname).cloned().unwrap_or(Value::Nil);
                        if let Some(ref fval) = filter_val
                            && val != *fval
                        {
                            return false;
                        }
                    }
                    // Check containment
                    if ov.beg == ov.end {
                        // Zero-length: include if within range
                        ov.beg >= beg && (ov.beg < end || (ov.beg == end && end == zv))
                    } else {
                        ov.beg >= beg && ov.end <= end
                    }
                })
                .map(|ov| ov.id)
                .collect();

            for id in &ids_to_delete {
                if let Some(ov) = interp.find_overlay_mut(*id) {
                    ov.buffer_id = None;
                }
            }
            interp.buffer.overlays.retain(|ov| !ids_to_delete.contains(&ov.id));
            Ok(Value::Nil)
        }

        // ── Plist operations ──

        "plist-get" => {
            need_args(name, args, 2)?;
            let plist = args[0].to_vec()?;
            let key = &args[1];
            let mut i = 0;
            while i + 1 < plist.len() {
                if plist[i] == *key {
                    return Ok(plist[i + 1].clone());
                }
                i += 2;
            }
            Ok(Value::Nil)
        }

        "plist-put" => {
            need_args(name, args, 3)?;
            let mut plist = args[0].to_vec()?;
            let key = &args[1];
            let val = &args[2];
            let mut i = 0;
            let mut found = false;
            while i + 1 < plist.len() {
                if plist[i] == *key {
                    plist[i + 1] = val.clone();
                    found = true;
                    break;
                }
                i += 2;
            }
            if !found {
                plist.push(key.clone());
                plist.push(val.clone());
            }
            Ok(Value::list(plist))
        }

        "plist-member" => {
            need_args(name, args, 2)?;
            let key = &args[1];
            let mut current = args[0].clone();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        if *car == *key {
                            return Ok(Value::Cons(car, cdr));
                        }
                        // Skip the value
                        match *cdr {
                            Value::Cons(_, next_cdr) => current = *next_cdr,
                            _ => return Ok(Value::Nil),
                        }
                    }
                    _ => return Ok(Value::Nil),
                }
            }
        }

        // ── Sort ──

        "sort" => {
            need_args(name, args, 2)?;
            let mut items = args[0].to_vec()?;
            let pred = args[1].clone();
            // Sort using the predicate. We need to call back into the interpreter.
            // Use a simple insertion sort to avoid issues with the borrow checker
            // and Rust's sort requiring Fn (not FnMut with &mut self).
            let len = items.len();
            for i in 1..len {
                let mut j = i;
                while j > 0 {
                    let pred_args = [items[j - 1].clone(), items[j].clone()];
                    let result = match &pred {
                        Value::BuiltinFunc(fname) => {
                            call(interp, fname, &pred_args, env)?
                        }
                        Value::Lambda(params, body, captured_env) => {
                            let mut call_env = captured_env.clone();
                            let mut frame = Vec::new();
                            for (k, param) in params.iter().enumerate() {
                                frame.push((
                                    param.clone(),
                                    pred_args.get(k).cloned().unwrap_or(Value::Nil),
                                ));
                            }
                            call_env.push(frame);
                            let mut r = Value::Nil;
                            for expr in body {
                                r = interp.eval(expr, &mut call_env)?;
                            }
                            r
                        }
                        _ => {
                            return Err(LispError::TypeError(
                                "function".into(),
                                pred.type_name(),
                            ))
                        }
                    };
                    // If pred(items[j-1], items[j]) is nil, swap
                    if result.is_nil() {
                        items.swap(j - 1, j);
                        j -= 1;
                    } else {
                        break;
                    }
                }
            }
            Ok(Value::list(items))
        }

        "random" => {
            if args.is_empty() {
                Ok(Value::Integer(rand_simple()))
            } else {
                let limit = args[0].as_integer()?;
                if limit <= 0 {
                    Ok(Value::Integer(0))
                } else {
                    Ok(Value::Integer(rand_simple().unsigned_abs() as i64 % limit))
                }
            }
        }

        "vector" => {
            // (vector &rest OBJECTS) — creates a "vector" as a list for now
            Ok(Value::list(args.to_vec()))
        }

        "aref" => {
            need_args(name, args, 2)?;
            let idx = args[1].as_integer()? as usize;
            // Support both list-vectors and strings
            match &args[0] {
                Value::String(s) => {
                    match s.chars().nth(idx) {
                        Some(c) => Ok(Value::Integer(c as i64)),
                        None => Err(LispError::Signal("Args out of range".into())),
                    }
                }
                _ => {
                    // Treat as list-vector
                    let items = args[0].to_vec()?;
                    items.get(idx).cloned().ok_or_else(|| {
                        LispError::Signal("Args out of range".into())
                    })
                }
            }
        }

        "aset" => {
            need_args(name, args, 3)?;
            // Aset on vectors: we can't mutate easily, just return the value
            Ok(args[2].clone())
        }

        "seq-every-p" => {
            need_args(name, args, 2)?;
            let pred = args[0].clone();
            let seq = args[1].to_vec()?;
            for item in &seq {
                let result = match &pred {
                    Value::BuiltinFunc(fname) => {
                        call(interp, fname, std::slice::from_ref(item), env)?
                    }
                    Value::Lambda(params, body, captured_env) => {
                        let mut call_env = captured_env.clone();
                        let mut frame = Vec::new();
                        if let Some(p) = params.first() {
                            frame.push((p.clone(), item.clone()));
                        }
                        call_env.push(frame);
                        let mut r = Value::Nil;
                        for expr in body {
                            r = interp.eval(expr, &mut call_env)?;
                        }
                        r
                    }
                    _ => {
                        return Err(LispError::TypeError(
                            "function".into(),
                            pred.type_name(),
                        ))
                    }
                };
                if result.is_nil() {
                    return Ok(Value::Nil);
                }
            }
            Ok(Value::T)
        }

        "nreverse" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.reverse();
            Ok(Value::list(items))
        }

        "copy-sequence" => {
            need_args(name, args, 1)?;
            // Lisp values are already cloned, so this is identity
            Ok(args[0].clone())
        }

        "delete" | "delq" => {
            need_args(name, args, 2)?;
            let elt = &args[0];
            let items = args[1].to_vec()?;
            let filtered: Vec<Value> = items.into_iter().filter(|x| x != elt).collect();
            Ok(Value::list(filtered))
        }

        "make-list" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()?;
            let val = args[1].clone();
            let items: Vec<Value> = (0..n).map(|_| val.clone()).collect();
            Ok(Value::list(items))
        }

        "insert-before-markers" => {
            for arg in args {
                let insert_at = interp.buffer.point();
                let s = match arg {
                    Value::String(s) => s.clone(),
                    Value::Integer(n) => {
                        char::from_u32(*n as u32)
                            .map(|c| c.to_string())
                            .unwrap_or_default()
                    }
                    _ => format!("{}", arg),
                };
                let nchars = s.chars().count();
                interp.buffer.insert(&s);
                for overlay in &mut interp.buffer.overlays {
                    if overlay.is_dead() {
                        continue;
                    }
                    if overlay.beg == insert_at {
                        overlay.beg += nchars;
                    }
                    if overlay.end == insert_at {
                        overlay.end += nchars;
                    }
                }
            }
            Ok(Value::Nil)
        }

        _ if is_composed_accessor_name(name) => call_composed_accessor(name, args),

        _ => Err(LispError::Signal(format!("Unknown function: {}", name))),
    }
}

fn get_or_create_buffer(interp: &mut Interpreter, name: &str) -> (u64, String) {
    interp
        .find_buffer(name)
        .unwrap_or_else(|| interp.create_buffer(name))
}

fn clamp_overlay_range(buffer: &crate::buffer::Buffer, beg: i64, end: i64) -> (usize, usize) {
    let min = buffer.point_min() as i64;
    let max = buffer.point_max() as i64;
    let clamp = |pos: i64| pos.clamp(min, max) as usize;
    let beg = clamp(beg);
    let end = clamp(end);
    if beg > end { (end, beg) } else { (beg, end) }
}

fn take_overlay(interp: &mut Interpreter, overlay_id: u64) -> Option<crate::overlay::Overlay> {
    interp.take_overlay(overlay_id)
}

fn highest_priority_overlay_property(
    buffer: &crate::buffer::Buffer,
    pos: usize,
    prop: &str,
    include_empty_rear_advance: bool,
) -> Option<Value> {
    let mut overlays: Vec<&crate::overlay::Overlay> = buffer
        .overlays
        .iter()
        .filter(|overlay| {
            !overlay.is_dead() && overlay_covers_position(overlay, pos, include_empty_rear_advance)
        })
        .collect();
    overlays.sort_by(|a, b| {
        a.priority()
            .cmp(&b.priority())
            .then_with(|| a.id.cmp(&b.id))
    });
    overlays
        .last()
        .and_then(|overlay| overlay.get_prop(prop).cloned())
}

fn overlay_covers_position(
    overlay: &crate::overlay::Overlay,
    pos: usize,
    include_empty_rear_advance: bool,
) -> bool {
    if overlay.beg == overlay.end {
        include_empty_rear_advance && overlay.rear_advance && overlay.beg == pos
    } else {
        overlay.beg <= pos && pos < overlay.end
    }
}

fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Buffer(a, _), Value::Buffer(b, _)) => a == b,
        (Value::Overlay(a), Value::Overlay(b)) => overlays_equal(interp, *a, *b),
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            values_equal(interp, a_car, b_car) && values_equal(interp, a_cdr, b_cdr)
        }
        _ => false,
    }
}

fn overlays_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_overlay(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_overlay(right_id) else {
        return false;
    };
    left.beg == right.beg
        && left.end == right.end
        && left.buffer_id == right.buffer_id
        && left.plist.len() == right.plist.len()
        && left
            .plist
            .iter()
            .zip(&right.plist)
            .all(|((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && values_equal(interp, left_value, right_value)
            })
}

fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

fn is_composed_accessor_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 3
        && bytes.first() == Some(&b'c')
        && bytes.last() == Some(&b'r')
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| matches!(byte, b'a' | b'd'))
}

fn call_composed_accessor(name: &str, args: &[Value]) -> Result<Value, LispError> {
    need_args(name, args, 1)?;
    let mut value = args[0].clone();
    for op in name[1..name.len() - 1].bytes().rev() {
        value = match op {
            b'a' => value.car()?,
            b'd' => value.cdr()?,
            _ => unreachable!("validated by is_composed_accessor_name"),
        };
    }
    Ok(value)
}

/// Simple pseudo-random number (xorshift64).
fn rand_simple() -> i64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
    let mut s = STATE.load(Ordering::Relaxed);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    STATE.store(s, Ordering::Relaxed);
    s as i64
}

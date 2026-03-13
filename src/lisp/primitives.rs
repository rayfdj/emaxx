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
            // Allocation
            | "make-string"
            | "make-vector"
            // String operations
            | "concat"
            | "substring"
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
            // Misc
            | "error"
            | "signal"
            | "throw"
            | "intern"
            | "symbol-name"
            | "type-of"
    )
}

/// Dispatch a builtin function call.
pub fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &super::types::Env,
) -> Result<Value, LispError> {
    match name {
        // ── Arithmetic ──
        "+" => {
            let mut sum: i64 = 0;
            for a in args {
                sum = sum.wrapping_add(a.as_integer()?);
            }
            Ok(Value::Integer(sum))
        }
        "-" => {
            if args.is_empty() {
                return Ok(Value::Integer(0));
            }
            if args.len() == 1 {
                return Ok(Value::Integer(args[0].as_integer()?.wrapping_neg()));
            }
            let mut result = args[0].as_integer()?;
            for a in &args[1..] {
                result = result.wrapping_sub(a.as_integer()?);
            }
            Ok(Value::Integer(result))
        }
        "*" => {
            let mut product: i64 = 1;
            for a in args {
                product = product.wrapping_mul(a.as_integer()?);
            }
            Ok(Value::Integer(product))
        }
        "/" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
            }
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
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            Ok(if a == b { Value::T } else { Value::Nil })
        }
        "<" => {
            need_args(name, args, 2)?;
            Ok(if args[0].as_integer()? < args[1].as_integer()? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">" => {
            need_args(name, args, 2)?;
            Ok(if args[0].as_integer()? > args[1].as_integer()? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "<=" => {
            need_args(name, args, 2)?;
            Ok(if args[0].as_integer()? <= args[1].as_integer()? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">=" => {
            need_args(name, args, 2)?;
            Ok(if args[0].as_integer()? >= args[1].as_integer()? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "/=" => {
            need_args(name, args, 2)?;
            Ok(if args[0].as_integer()? != args[1].as_integer()? {
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
            Ok(if args[0] == args[1] {
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
            Ok(if matches!(args[0], Value::Buffer(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "buffer-live-p" => {
            need_args(name, args, 1)?;
            // For now, all buffer values we hand out are live
            Ok(if matches!(args[0], Value::Buffer(_)) {
                Value::T
            } else {
                Value::Nil
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
                    let mut n = 0usize;
                    let digit_start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        n = n * 10 + (chars[i] as usize - '0' as usize);
                        i += 1;
                    }
                    if i < chars.len() && chars[i] == '$' {
                        positional = Some(n);
                        i += 1;
                    } else {
                        i = digit_start; // not positional, rewind
                    }
                }

                // Parse flags
                let mut flag_hash = false;
                let mut flag_zero = false;
                let mut flag_minus = false;
                while i < chars.len() {
                    match chars[i] {
                        '#' => flag_hash = true,
                        '0' => flag_zero = true,
                        '-' => flag_minus = true,
                        '+' | ' ' => {} // ignored for now
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
                    continue;
                }
                let arg = &args[aidx];

                // Convert to integer, handling floats
                let as_int = || -> i64 {
                    match arg {
                        Value::Integer(n) => *n,
                        Value::Float(f) => *f as i64,
                        _ => 0,
                    }
                };

                let formatted = match conv {
                    's' => match arg {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    },
                    'S' => arg.to_string(),
                    'd' => {
                        let n = as_int();
                        n.to_string()
                    }
                    'o' => {
                        let n = as_int();
                        if n < 0 {
                            // Emacs uses unsigned representation for negative
                            format!("{:o}", n as u64)
                        } else {
                            format!("{:o}", n)
                        }
                    }
                    'x' => {
                        let n = as_int();
                        if flag_hash && n != 0 {
                            format!("0x{:x}", n)
                        } else if n < 0 {
                            format!("{:x}", n as u64)
                        } else {
                            format!("{:x}", n)
                        }
                    }
                    'X' => {
                        let n = as_int();
                        if flag_hash && n != 0 {
                            format!("0X{:X}", n)
                        } else {
                            format!("{:X}", n)
                        }
                    }
                    'b' => {
                        let n = as_int();
                        if n < 0 {
                            format!("{:b}", n as u64)
                        } else {
                            format!("{:b}", n)
                        }
                    }
                    'c' => {
                        let n = as_int();
                        char::from_u32(n as u32)
                            .map(|c| c.to_string())
                            .unwrap_or_default()
                    }
                    _ => {
                        // Unknown, just pass through
                        if let Some(pos) = positional {
                            format!("%{}${}", pos, conv)
                        } else {
                            format!("%{}", conv)
                        }
                    }
                };

                // Apply width/padding
                if width > 0 && formatted.len() < width {
                    let pad_char = if flag_zero && !flag_minus { '0' } else { ' ' };
                    let padding = width - formatted.len();
                    if flag_minus {
                        result.push_str(&formatted);
                        for _ in 0..padding {
                            result.push(pad_char);
                        }
                    } else if flag_zero && (conv == 'x' || conv == 'X') && flag_hash {
                        // For %#08x, pad zeros after the 0x prefix
                        if let Some(rest) = formatted.strip_prefix("0x") {
                            result.push_str("0x");
                            for _ in 0..padding {
                                result.push('0');
                            }
                            result.push_str(rest);
                        } else if let Some(rest) = formatted.strip_prefix("0X") {
                            result.push_str("0X");
                            for _ in 0..padding {
                                result.push('0');
                            }
                            result.push_str(rest);
                        } else {
                            for _ in 0..padding {
                                result.push('0');
                            }
                            result.push_str(&formatted);
                        }
                    } else {
                        for _ in 0..padding {
                            result.push(pad_char);
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
                && let Value::Buffer(name) = &args[0]
            {
                return Ok(Value::String(name.clone()));
            }
            Ok(Value::String(interp.buffer.name.clone()))
        }
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
        "current-buffer" => Ok(Value::Buffer(interp.buffer.name.clone())),
        "generate-new-buffer" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let buf_name = if interp.buffer_list.contains(&base.to_string()) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.buffer_list.contains(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                base.to_string()
            };
            interp.buffer_list.push(buf_name.clone());
            Ok(Value::Buffer(buf_name))
        }
        "get-buffer" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Buffer(_) => Ok(args[0].clone()),
                Value::String(s) => {
                    if interp.buffer_list.contains(s) {
                        Ok(Value::Buffer(s.clone()))
                    } else {
                        Ok(Value::Nil)
                    }
                }
                _ => Err(LispError::TypeError(
                    "string-or-buffer".into(),
                    args[0].type_name(),
                )),
            }
        }
        "get-buffer-create" => {
            need_args(name, args, 1)?;
            let buf_name = match &args[0] {
                Value::Buffer(n) => n.clone(),
                Value::String(s) => s.clone(),
                _ => {
                    return Err(LispError::TypeError(
                        "string-or-buffer".into(),
                        args[0].type_name(),
                    ));
                }
            };
            if !interp.buffer_list.contains(&buf_name) {
                interp.buffer_list.push(buf_name.clone());
            }
            Ok(Value::Buffer(buf_name))
        }
        "generate-new-buffer-name" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let ignore = if args.len() > 1 {
                args[1].as_string().ok().map(|s| s.to_string())
            } else {
                None
            };
            if !interp.buffer_list.contains(&base.to_string()) || ignore.as_deref() == Some(base) {
                Ok(Value::String(base.to_string()))
            } else {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.buffer_list.contains(&candidate)
                        || ignore.as_deref() == Some(&candidate)
                    {
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
            let final_name =
                if interp.buffer_list.contains(&new_name.to_string()) && new_name != old_name {
                    if unique {
                        let mut n = 2;
                        loop {
                            let candidate = format!("{}<{}>", new_name, n);
                            if !interp.buffer_list.contains(&candidate) {
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
            if let Some(pos) = interp.buffer_list.iter().position(|n| *n == old_name) {
                interp.buffer_list[pos] = final_name.clone();
            }
            interp.buffer.name = final_name.clone();
            Ok(Value::String(final_name))
        }
        "other-buffer" => {
            let exclude = if !args.is_empty() {
                match &args[0] {
                    Value::Buffer(n) => n.clone(),
                    _ => interp.buffer.name.clone(),
                }
            } else {
                interp.buffer.name.clone()
            };
            for buf_name in &interp.buffer_list {
                if *buf_name != exclude && !buf_name.starts_with(' ') {
                    return Ok(Value::Buffer(buf_name.clone()));
                }
            }
            Ok(Value::Buffer("*scratch*".into()))
        }
        "buffer-base-buffer" => {
            // No indirect buffers supported yet
            Ok(Value::Nil)
        }
        "buffer-list" => {
            let bufs: Vec<Value> = interp
                .buffer_list
                .iter()
                .map(|n| Value::Buffer(n.clone()))
                .collect();
            Ok(Value::list(bufs))
        }
        "set-buffer" => {
            need_args(name, args, 1)?;
            // We only have one actual buffer, but return the buffer value
            match &args[0] {
                Value::Buffer(n) => Ok(Value::Buffer(n.clone())),
                Value::String(s) => Ok(Value::Buffer(s.clone())),
                _ => Err(LispError::TypeError(
                    "string-or-buffer".into(),
                    args[0].type_name(),
                )),
            }
        }
        "kill-buffer" => {
            // Remove from buffer list
            if let Some(buf_name) = match &args.first() {
                Some(Value::Buffer(n)) => Some(n.clone()),
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            } {
                interp.buffer_list.retain(|n| *n != buf_name);
            }
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
            // case-insensitive comparison (simplified: just lowercase ASCII)
            let eq = a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char));
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
        "buffer-last-name" => {
            // We don't track last-name, return current name
            Ok(Value::String(interp.buffer.name.clone()))
        }

        // ── Display stubs ──
        "display-graphic-p" | "frame-parameter" => Ok(Value::Nil),

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
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(s.to_string()))
        }
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
                Value::Buffer(_) => "buffer",
            };
            Ok(Value::Symbol(name.into()))
        }

        _ => Err(LispError::Signal(format!("Unknown function: {}", name))),
    }
}

fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

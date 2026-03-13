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
            | "get-buffer-create"
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
pub fn call(interp: &mut Interpreter, name: &str, args: &[Value]) -> Result<Value, LispError> {
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
        "bufferp" | "buffer-live-p" => {
            // Stub: we don't have first-class buffer objects yet
            need_args(name, args, 1)?;
            Ok(Value::Nil)
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
            // Simple %-substitution
            let mut result = String::new();
            let mut arg_idx = 1;
            let chars: Vec<char> = fmt.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] == '%' && i + 1 < chars.len() {
                    i += 1;
                    match chars[i] {
                        's' => {
                            if arg_idx < args.len() {
                                match &args[arg_idx] {
                                    Value::String(s) => result.push_str(s),
                                    other => result.push_str(&other.to_string()),
                                }
                                arg_idx += 1;
                            }
                        }
                        'd' => {
                            if arg_idx < args.len() {
                                if let Ok(n) = args[arg_idx].as_integer() {
                                    result.push_str(&n.to_string());
                                }
                                arg_idx += 1;
                            }
                        }
                        'S' => {
                            // prin1 format
                            if arg_idx < args.len() {
                                result.push_str(&args[arg_idx].to_string());
                                arg_idx += 1;
                            }
                        }
                        '%' => result.push('%'),
                        'c' => {
                            if arg_idx < args.len()
                                && let Ok(n) = args[arg_idx].as_integer()
                                && let Some(c) = char::from_u32(n as u32)
                            {
                                result.push(c);
                            }
                            if arg_idx < args.len() {
                                arg_idx += 1;
                            }
                        }
                        _ => {
                            result.push('%');
                            result.push(chars[i]);
                        }
                    }
                } else {
                    result.push(chars[i]);
                }
                i += 1;
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
        "buffer-name" => Ok(Value::String(interp.buffer.name.clone())),
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
        "current-column" => Ok(Value::Integer(interp.buffer.current_column() as i64)),
        "line-number-at-pos" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                args[0].as_integer()? as usize
            };
            Ok(Value::Integer(interp.buffer.line_number_at_pos(pos) as i64))
        }
        "line-beginning-position" | "pos-bol" => {
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let result = interp.buffer.point();
            interp.buffer.goto_char(saved);
            Ok(Value::Integer(result as i64))
        }
        "line-end-position" | "pos-eol" => {
            let saved = interp.buffer.point();
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
        "current-buffer" | "generate-new-buffer" | "get-buffer-create" => {
            // Stubs: return a symbol for now
            Ok(Value::Symbol("*buffer*".into()))
        }
        "kill-buffer" => Ok(Value::T),
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

use crate::buffer::TextPropertySpan;
use super::eval::Interpreter;
use super::types::{LispError, Value};
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;
use std::fs;
use std::process::Command;

const RAW_CHAR_SENTINEL: char = '\u{F8FF}';

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
            | "equal-including-properties"
            | "string="
            | "string-equal"
            | "xor"
            // Type predicates
            | "null"
            | "not"
            | "integerp"
            | "numberp"
            | "floatp"
            | "stringp"
            | "symbolp"
            | "boundp"
            | "consp"
            | "listp"
            | "bufferp"
            | "buffer-live-p"
            | "zerop"
            | "natnump"
            | "atom"
            | "nlistp"
            | "characterp"
            | "markerp"
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
            | "mapc"
            | "apply"
            | "funcall"
            | "funcall-interactively"
            | "call-interactively"
            | "identity"
            | "mapconcat"
            | "seq-take"
            // Allocation
            | "make-string"
            | "make-vector"
            // String operations
            | "concat"
            | "string"
            | "substring"
            | "string-to-multibyte"
            | "string-to-number"
            | "number-to-string"
            | "format"
            | "char-to-string"
            | "string-to-char"
            | "byte-to-string"
            | "multibyte-string-p"
            | "unibyte-char-to-multibyte"
            | "upcase"
            | "downcase"
            // Buffer operations
            | "insert"
            | "insert-and-inherit"
            | "insert-char"
            | "insert-byte"
            | "insert-buffer-substring"
            | "insert-before-markers-and-inherit"
            | "point"
            | "point-min"
            | "point-max"
            | "goto-char"
            | "forward-char"
            | "backward-char"
            | "forward-word"
            | "beginning-of-line"
            | "end-of-line"
            | "forward-line"
            | "search-forward"
            | "search-backward"
            | "re-search-forward"
            | "search-forward-regexp"
            | "match-beginning"
            | "match-end"
            | "buffer-string"
            | "buffer-substring"
            | "buffer-substring-no-properties"
            | "buffer-size"
            | "buffer-name"
            | "set-buffer-multibyte"
            | "buffer-enable-undo"
            | "char-after"
            | "char-before"
            | "bobp"
            | "eobp"
            | "bolp"
            | "eolp"
            | "delete-region"
            | "delete-char"
            | "delete-forward-char"
            | "kill-word"
            | "kill-region"
            | "erase-buffer"
            | "current-column"
            | "move-to-column"
            | "line-number-at-pos"
            | "line-beginning-position"
            | "line-end-position"
            | "pos-bol"
            | "pos-eol"
            | "narrow-to-region"
            | "widen"
            | "buffer-modified-p"
            | "set-buffer-modified-p"
            | "buffer-chars-modified-tick"
            | "buffer-modified-tick"
            | "restore-buffer-modified-p"
            | "gap-position"
            | "gap-size"
            | "position-bytes"
            | "byte-to-position"
            | "get-pos-property"
            | "get-char-property"
            | "get-text-property"
            | "text-properties-at"
            | "put-text-property"
            | "add-text-properties"
            | "remove-list-of-text-properties"
            | "compare-buffer-substrings"
            | "field-beginning"
            | "field-end"
            | "field-string-no-properties"
            | "delete-field"
            | "constrain-to-field"
            | "current-buffer"
            | "generate-new-buffer"
            | "get-buffer"
            | "get-buffer-create"
            | "generate-new-buffer-name"
            | "make-indirect-buffer"
            | "rename-buffer"
            | "other-buffer"
            | "buffer-base-buffer"
            | "buffer-swap-text"
            | "buffer-local-value"
            | "buffer-local-variables"
            | "buffer-list"
            | "set-buffer"
            | "switch-to-buffer"
            | "buffer-file-name"
            | "find-file"
            | "find-file-noselect"
            | "file-exists-p"
            | "delete-file"
            | "insert-file-contents"
            | "write-region"
            | "kill-buffer"
            | "set-mark"
            | "push-mark"
            | "mark"
            | "region-beginning"
            | "region-end"
            | "make-marker"
            | "copy-marker"
            | "point-marker"
            | "mark-marker"
            | "point-min-marker"
            | "point-max-marker"
            | "marker-buffer"
            | "marker-position"
            | "marker-last-position"
            | "marker-insertion-type"
            | "set-marker-insertion-type"
            | "set-marker"
            | "move-marker"
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
            | "transient-mark-mode"
            | "facemenu-add-face"
            | "get-buffer-window"
            | "set-window-start"
            | "set-window-point"
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
            | "take"
            | "add-hook"
            | "describe-function"
            | "executable-find"
            | "ignore"
            | "intern"
            | "symbol-function"
            | "symbol-name"
            | "char-from-name"
            | "evenp"
            | "type-of"
            | "file-truename"
            | "save-buffer"
            | "auto-save-mode"
            | "do-auto-save"
            | "group-gid"
            | "group-name"
            | "random"
            | "make-hash-table"
            | "make-char-table"
            | "translate-region-internal"
            | "propertize"
            | "regexp-quote"
            | "vector"
            | "aref"
            | "aset"
            | "seq-every-p"
            | "nreverse"
            | "copy-sequence"
            | "delete"
            | "delq"
            | "make-list"
            | "looking-at"
            | "replace-match"
            | "replace-region-contents"
            | "transpose-regions"
            | "subst-char-in-region"
            | "flush-lines"
            | "insert-before-markers"
            | "internal--labeled-narrow-to-region"
            | "internal--labeled-widen"
            | "dabbrev-expand"
            | "encode-coding-region"
            | "decode-coding-region"
            | "encode-coding-string"
            | "decode-coding-string"
            | "garbage-collect"
            | "undo-boundary"
            | "undo"
            | "undo-more"
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
    let has_big_integer = |args: &[Value]| args.iter().any(|a| matches!(a, Value::BigInteger(_)));

    match name {
        // ── Arithmetic ──
        "+" => {
            if has_float(args) {
                let mut sum = 0.0;
                for a in args {
                    sum += numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(sum))
            } else {
                let mut sum = BigInt::zero();
                for a in args {
                    sum += integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(sum))
            }
        }
        "-" => {
            if args.is_empty() {
                return Ok(Value::Integer(0));
            }
            if has_float(args) {
                if args.len() == 1 {
                    return Ok(Value::Float(-numeric_to_f64(interp, &args[0])?));
                }
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result -= numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(result))
            } else {
                if args.len() == 1 {
                    return Ok(normalize_bigint_value(-integer_like_bigint(interp, &args[0])?));
                }
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result -= integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(result))
            }
        }
        "*" => {
            if has_float(args) {
                let mut product = 1.0;
                for a in args {
                    product *= numeric_to_f64(interp, a)?;
                }
                Ok(Value::Float(product))
            } else {
                let mut product = BigInt::from(1u8);
                for a in args {
                    product *= integer_like_bigint(interp, a)?;
                }
                Ok(normalize_bigint_value(product))
            }
        }
        "/" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("/".into(), args.len()));
            }
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    let divisor = numeric_to_f64(interp, a)?;
                    if divisor == 0.0 {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(Value::Float(result))
            } else if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    let divisor = integer_like_bigint(interp, a)?;
                    if divisor.is_zero() {
                        return Err(LispError::Signal("Division by zero".into()));
                    }
                    result /= divisor;
                }
                Ok(normalize_bigint_value(result))
            } else {
                let mut result = integer_like_i64(interp, &args[0])?;
                for a in &args[1..] {
                    let divisor = integer_like_i64(interp, a)?;
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
            if has_big_integer(args) {
                let a = integer_like_bigint(interp, &args[0])?;
                let b = integer_like_bigint(interp, &args[1])?;
                if b.is_zero() {
                    return Err(LispError::Signal("Division by zero".into()));
                }
                let r = ((&a % &b) + &b) % &b;
                return Ok(normalize_bigint_value(r));
            }
            let a = integer_like_i64(interp, &args[0])?;
            let b = integer_like_i64(interp, &args[1])?;
            if b == 0 {
                return Err(LispError::Signal("Division by zero".into()));
            }
            Ok(Value::Integer(a.rem_euclid(b)))
        }
        "1+" => {
            need_args(name, args, 1)?;
            Ok(normalize_bigint_value(integer_like_bigint(interp, &args[0])? + 1))
        }
        "1-" => {
            need_args(name, args, 1)?;
            Ok(normalize_bigint_value(integer_like_bigint(interp, &args[0])? - 1))
        }
        "max" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("max".into(), 0));
            }
            if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.max(integer_like_bigint(interp, a)?);
                }
                return Ok(normalize_bigint_value(result));
            }
            let mut result = integer_like_i64(interp, &args[0])?;
            for a in &args[1..] {
                result = result.max(integer_like_i64(interp, a)?);
            }
            Ok(Value::Integer(result))
        }
        "min" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("min".into(), 0));
            }
            if has_big_integer(args) {
                let mut result = integer_like_bigint(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.min(integer_like_bigint(interp, a)?);
                }
                return Ok(normalize_bigint_value(result));
            }
            let mut result = integer_like_i64(interp, &args[0])?;
            for a in &args[1..] {
                result = result.min(integer_like_i64(interp, a)?);
            }
            Ok(Value::Integer(result))
        }
        "abs" => {
            need_args(name, args, 1)?;
            if matches!(args[0], Value::BigInteger(_)) {
                Ok(normalize_bigint_value(integer_like_bigint(interp, &args[0])?.abs()))
            } else {
                Ok(Value::Integer(integer_like_i64(interp, &args[0])?.abs()))
            }
        }

        // ── Comparison ──
        "=" => {
            need_args(name, args, 2)?;
            if has_float(args) {
                let a = numeric_to_f64(interp, &args[0])?;
                let b = numeric_to_f64(interp, &args[1])?;
                Ok(if a == b { Value::T } else { Value::Nil })
            } else if has_big_integer(args) {
                Ok(if integer_like_bigint(interp, &args[0])?
                    == integer_like_bigint(interp, &args[1])?
                {
                    Value::T
                } else {
                    Value::Nil
                })
            } else {
                Ok(if integer_like_i64(interp, &args[0])? == integer_like_i64(interp, &args[1])? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
        }
        "<" => {
            need_args(name, args, 2)?;
            Ok(if numeric_lt(interp, &args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">" => {
            need_args(name, args, 2)?;
            Ok(if numeric_gt(interp, &args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "<=" => {
            need_args(name, args, 2)?;
            Ok(if !numeric_gt(interp, &args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">=" => {
            need_args(name, args, 2)?;
            Ok(if !numeric_lt(interp, &args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "/=" => {
            need_args(name, args, 2)?;
            Ok(if !matches!(call(interp, "=", args, env)?, Value::T) {
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
        "equal-including-properties" => {
            need_args(name, args, 2)?;
            Ok(if values_equal_including_properties(&args[0], &args[1]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string=" | "string-equal" => {
            need_args(name, args, 2)?;
            let a = string_text(&args[0])?;
            let b = string_text(&args[1])?;
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
        "xor" => {
            need_args(name, args, 2)?;
            Ok(if args[0].is_truthy() ^ args[1].is_truthy() {
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
            Ok(if string_like(&args[0]).is_some() {
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
        "boundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.lookup_var(symbol, env).is_some()
                || matches!(symbol, "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum")
                || symbol == "buffer-undo-list"
            {
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
                Value::BigInteger(n) if n.is_zero() => Value::T,
                Value::Float(f) if *f == 0.0 => Value::T,
                _ => Value::Nil,
            })
        }

        "natnump" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 => Value::T,
                Value::BigInteger(n) if n.sign() != Sign::Minus => Value::T,
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
                Value::BigInteger(n)
                    if n.sign() != Sign::Minus
                        && n <= &BigInt::from(0x3F_FFFFu32) =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }
        "markerp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Marker(_)) {
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
                value if string_like(value).is_some() => {
                    Ok(Value::Integer(string_text(value)?.chars().count() as i64))
                }
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
        "mapc" => {
            need_args(name, args, 2)?;
            let list = args[1].to_vec()?;
            for item in &list {
                let call_expr = Value::list([
                    args[0].clone(),
                    Value::list([Value::symbol("quote"), item.clone()]),
                ]);
                let _ = interp.eval(&call_expr, &mut Vec::new())?;
            }
            Ok(args[1].clone())
        }
        "mapconcat" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let list = call(interp, "mapcar", &args[..2], env)?.to_vec()?;
            let sep = if args.len() == 3 {
                string_like(&args[2]).unwrap_or(StringLike {
                    text: string_text(&args[2])?,
                    props: Vec::new(),
                })
            } else {
                StringLike {
                    text: String::new(),
                    props: Vec::new(),
                }
            };
            let mut result = String::new();
            let mut props = Vec::new();
            for (index, item) in list.iter().enumerate() {
                if index > 0 {
                    let offset = result.chars().count();
                    result.push_str(&sep.text);
                    props.extend(shift_string_props(&sep.props, offset));
                }
                if let Some(string) = string_like(item) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                } else {
                    result.push_str(&item.to_string());
                }
            }
            Ok(string_like_value(result, merge_string_props(props)))
        }
        "seq-take" => {
            need_args(name, args, 2)?;
            let count = args[1].as_integer()?.max(0) as usize;
            if let Ok(items) = args[0].to_vec() {
                Ok(Value::list(items.into_iter().take(count)))
            } else if let Some(string) = string_like(&args[0]) {
                let text: String = string.text.chars().take(count).collect();
                let props = slice_string_props(&string.props, 0, text.chars().count());
                Ok(string_like_value(text, props))
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
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
        "funcall-interactively" | "call-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = &args[0];
            let tail = if name == "call-interactively" {
                &args[1..1]
            } else {
                &args[1..]
            };
            let mut call_items = vec![func.clone()];
            for a in tail {
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
            let mut props = Vec::new();
            for a in args {
                if let Some(string) = string_like(a) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                } else {
                    match a {
                        Value::Nil => {}
                        _ => result.push_str(&a.to_string()),
                    }
                }
            }
            Ok(string_like_value(result, merge_string_props(props)))
        }
        "string" => {
            let mut result = String::new();
            for arg in args {
                let ch = arg.as_integer()?;
                let ch = char::from_u32(ch as u32)
                    .ok_or_else(|| LispError::Signal(format!("Invalid character: {ch}")))?;
                result.push(ch);
            }
            Ok(Value::String(result))
        }
        "substring" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs("substring".into(), args.len()));
            }
            let string = string_like(&args[0])
                .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
            let s = string.text;
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
            Ok(string_like_value(
                chars[from..to].iter().collect(),
                slice_string_props(&string.props, from, to),
            ))
        }
        "string-to-multibyte" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?))
        }
        "string-to-number" => {
            need_args(name, args, 1)?;
            let s = string_text(&args[0])?;
            if let Ok(n) = s.parse::<i64>() {
                Ok(Value::Integer(n))
            } else if let Ok(n) = s.parse::<BigInt>() {
                Ok(normalize_bigint_value(n))
            } else {
                Ok(Value::Integer(0))
            }
        }
        "number-to-string" => {
            need_args(name, args, 1)?;
            Ok(Value::String(number_to_string(&args[0])?))
        }
        "format" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("format".into(), 0));
            }
            let fmt_value = &args[0];
            let fmt = string_text(fmt_value)?;
            let mut result = String::new();
            let mut result_props = Vec::new();
            let mut arg_idx = 1;
            let chars: Vec<char> = fmt.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                if chars[i] != '%' || i + 1 >= chars.len() {
                    let start = result.chars().count();
                    result.push(chars[i]);
                    if let Some(props) = format_source_props(fmt_value, i, i + 1) {
                        result_props.push(TextPropertySpan {
                            start,
                            end: start + 1,
                            props,
                        });
                    }
                    i += 1;
                    continue;
                }
                let spec_start = i;
                i += 1; // skip '%'

                if chars[i] == '%' {
                    let start = result.chars().count();
                    result.push('%');
                    if let Some(props) = format_source_props(fmt_value, spec_start, i + 1) {
                        result_props.push(TextPropertySpan {
                            start,
                            end: start + 1,
                            props,
                        });
                    }
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

                let mut precision = None;
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                    let mut parsed_precision = 0usize;
                    let mut saw_precision = false;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        parsed_precision =
                            parsed_precision * 10 + (chars[i] as usize - '0' as usize);
                        saw_precision = true;
                        i += 1;
                    }
                    precision = Some(if saw_precision { parsed_precision } else { 0 });
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

                let (mut formatted, mut formatted_props) = match conv {
                    's' => format_s_conversion(arg, precision)?,
                    'S' => (render_prin1(interp, arg, env)?, Vec::new()),
                    'd' | 'o' | 'x' | 'X' | 'b' | 'B' => (
                        format_numeric_conversion(
                            interp,
                            arg,
                            conv,
                            flag_hash,
                            flag_plus,
                            flag_space,
                            precision,
                        )?,
                        Vec::new(),
                    ),
                    'c' => (format_char_conversion(arg)?, Vec::new()),
                    _ => {
                        // Unknown conversion, pass through
                        if let Some(pos) = positional {
                            (format!("%{}${}", pos, conv), Vec::new())
                        } else {
                            (format!("%{}", conv), Vec::new())
                        }
                    }
                };

                // Apply width/padding
                let formatted_len = formatted.chars().count();
                if width > 0 && formatted_len < width {
                    let padding = width - formatted_len;
                    if flag_minus {
                        // Left-align: content then spaces
                        if formatted_len > 0 {
                            let trailing_props =
                                props_at_string_offset(&formatted_props, formatted_len - 1);
                            if !trailing_props.is_empty() {
                                formatted_props.push(TextPropertySpan {
                                    start: formatted_len,
                                    end: formatted_len + padding,
                                    props: trailing_props,
                                });
                            }
                        }
                        formatted.push_str(&" ".repeat(padding));
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
                        formatted = format!(
                            "{}{}{}",
                            &s[..prefix_end],
                            "0".repeat(padding),
                            &s[prefix_end..]
                        );
                    } else {
                        formatted = format!("{}{}", " ".repeat(padding), formatted);
                        formatted_props = shift_string_props(&formatted_props, padding);
                    }
                }
                if let Some(props) = format_source_props(fmt_value, spec_start, i) {
                    formatted_props.push(TextPropertySpan {
                        start: 0,
                        end: formatted.chars().count(),
                        props,
                    });
                }
                let start = result.chars().count();
                result.push_str(&formatted);
                result_props.extend(shift_string_props(&merge_string_props(formatted_props), start));
            }
            Ok(string_like_value(result, merge_string_props(result_props)))
        }
        "char-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "byte-to-string" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            if !(0..=255).contains(&n) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            let c = char::from_u32(n as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", n)))?;
            Ok(Value::String(c.to_string()))
        }
        "string-to-char" => {
            need_args(name, args, 1)?;
            let s = string_text(&args[0])?;
            Ok(Value::Integer(
                s.chars().next().map(|c| c as i64).unwrap_or(0),
            ))
        }
        "multibyte-string-p" => {
            need_args(name, args, 1)?;
            let s = string_text(&args[0])?;
            Ok(if s.chars().any(|ch| (ch as u32) > 0x7F) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "unibyte-char-to-multibyte" => {
            need_args(name, args, 1)?;
            let n = args[0].as_integer()?;
            if !(0..=255).contains(&n) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            Ok(Value::Integer(n))
        }
        "upcase" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?.to_uppercase()))
        }
        "downcase" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?.to_lowercase()))
        }

        // ── Buffer operations ──
        "insert" => {
            let combined = combine_insert_args(args)?;
            insert_text_with_hooks(interp, &combined.text, &combined.props, false, false, env)?;
            Ok(Value::Nil)
        }
        "insert-and-inherit" => {
            let combined = combine_insert_args(args)?;
            insert_text_with_hooks(interp, &combined.text, &combined.props, true, false, env)?;
            Ok(Value::Nil)
        }
        "insert-char" => {
            need_args(name, args, 1)?;
            let ch = args[0].as_integer()?;
            let count = if args.len() > 1 {
                args[1].as_integer()?.max(0) as usize
            } else {
                1
            };
            if let Some(c) = char::from_u32(ch as u32) {
                let text: String = std::iter::repeat_n(c, count).collect();
                insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            } else if (0..=0x3F_FFFF).contains(&ch) {
                let text: String = std::iter::repeat_n(RAW_CHAR_SENTINEL, count).collect();
                let props = vec![TextPropertySpan {
                    start: 0,
                    end: count,
                    props: vec![("emaxx-raw-char".into(), Value::Integer(ch))],
                }];
                insert_text_with_hooks(interp, &text, &props, false, false, env)?;
            } else {
                return Err(LispError::Signal(format!("Invalid character: {}", ch)));
            }
            Ok(Value::Nil)
        }
        "insert-byte" => {
            need_args(name, args, 2)?;
            let byte = args[0].as_integer()?;
            if !(0..=255).contains(&byte) {
                return Err(LispError::Signal("Byte value out of range".into()));
            }
            let count = args[1].as_integer()?.max(0) as usize;
            let c = char::from_u32(byte as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid byte: {}", byte)))?;
            let text: String = std::iter::repeat_n(c, count).collect();
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "insert-buffer-substring" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let buffer_id = interp.resolve_buffer_id(&args[0])?;
            let source = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            let start = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                source.point_min()
            };
            let end = if args.len() > 2 {
                position_from_value(interp, &args[2])?
            } else {
                source.point_max()
            };
            let text = source
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let props = source.substring_property_spans(start, end);
            insert_text_with_hooks(interp, &text, &props, false, false, env)?;
            Ok(Value::Nil)
        }
        "point" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "point-min" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "point-max" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "goto-char" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
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
        "forward-word" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let forward = n >= 0;
            let mut remaining = n.unsigned_abs();
            while remaining > 0 {
                if forward {
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if ch.is_alphanumeric() || ch == '_' {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                    while let Some(ch) = interp.buffer.char_at(interp.buffer.point()) {
                        if !(ch.is_alphanumeric() || ch == '_') {
                            break;
                        }
                        let _ = interp.buffer.forward_char(1);
                    }
                } else {
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if matches!(interp.buffer.char_before(), Some(ch) if ch.is_alphanumeric() || ch == '_') {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if !matches!(interp.buffer.char_before(), Some(ch) if ch.is_alphanumeric() || ch == '_') {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                }
                remaining -= 1;
            }
            Ok(Value::Nil)
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
                BigInt::from(1u8)
            } else {
                integer_like_bigint(interp, &args[0])?
            };
            Ok(normalize_bigint_value(forward_line_bigint(
                &mut interp.buffer,
                n,
            )))
        }
        "search-forward" | "search-backward" => {
            need_args(name, args, 1)?;
            let needle = string_text(&args[0])?;
            let haystack = interp.buffer.buffer_string();
            let point = interp.buffer.point();
            let result = if name == "search-forward" {
                let offset = haystack
                    .chars()
                    .take(point.saturating_sub(1))
                    .map(|ch| ch.len_utf8())
                    .sum::<usize>();
                haystack[offset..].find(&needle).map(|found| {
                    let chars = haystack[offset..offset + found + needle.len()].chars().count();
                    point + chars
                })
            } else {
                let prefix: String = haystack.chars().take(point.saturating_sub(1)).collect();
                prefix.rfind(&needle).map(|found| prefix[..found].chars().count() + 1)
            };
            match result {
                Some(pos) => {
                    interp.buffer.goto_char(pos);
                    Ok(Value::Integer(pos as i64))
                }
                None => Ok(Value::Nil),
            }
        }
        "re-search-forward" | "search-forward-regexp" => {
            need_args(name, args, 1)?;
            let pattern = string_text(&args[0])?;
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let start = interp.buffer.point();
            let tail = interp
                .buffer
                .buffer_substring(start, interp.buffer.point_max())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if let Some(captures) = regex.captures(&tail)
                && let Some(matched) = captures.get(0)
            {
                let pos = start + tail[..matched.end()].chars().count();
                set_match_data(interp, start, &tail, &captures);
                interp.buffer.goto_char(pos);
                Ok(Value::Integer(pos as i64))
            } else {
                interp.last_match_data = None;
                Ok(Value::Nil)
            }
        }
        "buffer-string" => Ok(string_like_value(
            interp.buffer.buffer_string(),
            interp
                .buffer
                .substring_property_spans(interp.buffer.point_min(), interp.buffer.point_max()),
        )),
        "buffer-substring" | "buffer-substring-no-properties" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            match interp.buffer.buffer_substring(from, to) {
                Ok(s) => {
                    if name == "buffer-substring" {
                        Ok(string_like_value(
                            s,
                            interp.buffer.substring_property_spans(from, to),
                        ))
                    } else {
                        Ok(Value::String(s))
                    }
                }
                Err(e) => Err(LispError::Signal(e.to_string())),
            }
        }
        "buffer-size" => Ok(Value::Integer(interp.buffer.buffer_size() as i64)),
        "buffer-enable-undo" => {
            interp.buffer.enable_undo();
            Ok(Value::Nil)
        }
        "gap-position" => Ok(Value::Integer(interp.buffer.point() as i64)),
        "gap-size" => Ok(Value::Integer(0)),
        "position-bytes" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            Ok(position_bytes(interp, pos)
                .map(|byte_pos| Value::Integer(byte_pos as i64))
                .unwrap_or(Value::Nil))
        }
        "byte-to-position" => {
            need_args(name, args, 1)?;
            let byte = args[0].as_integer()?;
            if byte <= 0 {
                return Ok(Value::Nil);
            }
            Ok(byte_to_position(interp, byte as usize)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
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
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            delete_region_with_hooks(interp, from, to, env)?;
            Ok(Value::Nil)
        }
        "kill-region" => call(interp, "delete-region", args, env),
        "delete-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let point = interp.buffer.point();
            if n >= 0 {
                let to = point + n as usize;
                if to > interp.buffer.point_max() {
                    Err(LispError::Signal("End of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point, to, env)?;
                    Ok(Value::Nil)
                }
            } else {
                let count = (-n) as usize;
                if point < interp.buffer.point_min() + count {
                    Err(LispError::Signal("Beginning of buffer".into()))
                } else {
                    delete_region_with_hooks(interp, point - count, point, env)?;
                    Ok(Value::Nil)
                }
            }
        }
        "delete-forward-char" => {
            let n = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            call(interp, "delete-char", &[Value::Integer(n)], env)
        }
        "kill-word" => {
            let count = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            let start = interp.buffer.point();
            call(interp, "forward-word", &[Value::Integer(count)], env)?;
            let end = interp.buffer.point();
            interp.buffer.goto_char(start);
            call(
                interp,
                "delete-region",
                &[Value::Integer(start as i64), Value::Integer(end as i64)],
                env,
            )
        }
        "erase-buffer" => {
            let size = interp.buffer.buffer_size();
            if size > 0 {
                let min = interp.buffer.point_min();
                let max = interp.buffer.point_max();
                delete_region_with_hooks(interp, min, max, env)?;
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
        "move-to-column" => {
            need_args(name, args, 1)?;
            let target = args[0].as_integer()?.max(0) as usize;
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let start = interp.buffer.point();
            interp.buffer.goto_char(saved);
            let mut pos = start;
            while pos < interp.buffer.point_max() && column_at(interp, start, pos) < target {
                if matches!(interp.buffer.char_at(pos), Some('\n') | None) {
                    break;
                }
                pos += 1;
            }
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(interp.buffer.current_column() as i64))
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
            let mut start = args[0].as_integer()? as usize;
            let mut end = args[1].as_integer()? as usize;
            if let Some((clamp_start, clamp_end)) =
                interp.effective_labeled_restriction(interp.current_buffer_id(), None)
            {
                start = start.max(clamp_start);
                end = end.min(clamp_end);
            } else if let Some(active) =
                interp.lookup_var("__emaxx-active-labeled-restriction", env)
            {
                let values = active.to_vec()?;
                let clamp_start = values.first().and_then(|v| v.as_integer().ok()).unwrap_or(1)
                    as usize;
                let clamp_end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
                start = start.max(clamp_start);
                end = end.min(clamp_end);
            }
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }
        "widen" => {
            if let Some((start, end)) =
                interp.effective_labeled_restriction(interp.current_buffer_id(), None)
            {
                interp.buffer.narrow_to_region(start, end);
            } else {
                interp.buffer.widen();
            }
            Ok(Value::Nil)
        }
        "buffer-modified-p" => Ok(if interp.buffer.is_autosaved() {
            Value::Symbol("autosaved".into())
        } else if interp.buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        }),
        "buffer-chars-modified-tick" | "buffer-modified-tick" => {
            Ok(Value::Integer(interp.buffer.modified_tick() as i64))
        }
        "set-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
            } else {
                interp.buffer.set_modified();
            }
            Ok(Value::Nil)
        }
        "restore-buffer-modified-p" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                interp.buffer.set_unmodified();
            } else if matches!(&args[0], Value::Symbol(symbol) if symbol == "autosaved") {
                interp.buffer.set_modified();
                interp.buffer.set_autosaved();
            } else {
                interp.buffer.set_modified();
            }
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
            .or_else(|| buffer.text_property_at(pos, &prop))
            .unwrap_or(Value::Nil))
        }
        "get-text-property" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args[0].as_integer()? as usize;
            let prop = args[1].as_symbol()?.to_string();
            if let Some(object) = args.get(2) {
                if string_like(object).is_some() {
                    Ok(string_property_at(object, pos, &prop).unwrap_or(Value::Nil))
                } else {
                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
                    Ok(buffer.text_property_at(pos, &prop).unwrap_or(Value::Nil))
                }
            } else {
                Ok(interp.buffer.text_property_at(pos, &prop).unwrap_or(Value::Nil))
            }
        }
        "text-properties-at" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args[0].as_integer()? as usize;
            let props = if let Some(object) = args.get(1) {
                if string_like(object).is_some() {
                    string_properties_at(object, pos)
                } else {
                    let buffer_id = if object.is_nil() {
                        interp.current_buffer_id()
                    } else {
                        interp.resolve_buffer_id(object)?
                    };
                    interp
                        .get_buffer_by_id(buffer_id)
                        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?
                        .text_properties_at(pos)
                }
            } else {
                interp.buffer.text_properties_at(pos)
            };
            Ok(plist_value(&props))
        }
        "put-text-property" => {
            need_args(name, args, 4)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let prop = args[2].as_symbol()?.to_string();
            interp
                .buffer
                .put_text_property(start, end, &prop, args[3].clone());
            Ok(Value::T)
        }
        "add-text-properties" => {
            need_args(name, args, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let props = plist_pairs(&args[2])?;
            interp.buffer.add_text_properties(start, end, &props);
            Ok(Value::T)
        }
        "remove-list-of-text-properties" => {
            need_args(name, args, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let names = args[2]
                .to_vec()?
                .into_iter()
                .map(|value| value.as_symbol().map(|s| s.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            interp
                .buffer
                .remove_list_of_text_properties(start, end, &names);
            Ok(Value::T)
        }
        "compare-buffer-substrings" => {
            need_args(name, args, 6)?;
            let left_id = interp.resolve_buffer_id(&args[0])?;
            let left_start = position_from_value(interp, &args[1])?;
            let left_end = position_from_value(interp, &args[2])?;
            let right_id = interp.resolve_buffer_id(&args[3])?;
            let right_start = position_from_value(interp, &args[4])?;
            let right_end = position_from_value(interp, &args[5])?;
            let left = interp
                .get_buffer_by_id(left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?
                .buffer_substring(left_start, left_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let right = interp
                .get_buffer_by_id(right_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?
                .buffer_substring(right_start, right_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Integer(compare_buffer_substrings(&left, &right)))
        }
        "field-beginning" | "field-end" => {
            let pos = if args.is_empty() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let field = interp.buffer.text_property_at(pos, "field");
            let mut cursor = pos;
            if name == "field-beginning" {
                while cursor > interp.buffer.point_min()
                    && interp.buffer.text_property_at(cursor - 1, "field") == field
                {
                    cursor -= 1;
                }
            } else {
                while cursor < interp.buffer.point_max()
                    && interp.buffer.text_property_at(cursor, "field") == field
                {
                    cursor += 1;
                }
            }
            Ok(Value::Integer(cursor as i64))
        }
        "field-string-no-properties" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            let start = call(interp, "field-beginning", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            Ok(Value::String(
                interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?,
            ))
        }
        "delete-field" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            let start = call(interp, "field-beginning", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Nil)
        }
        "constrain-to-field" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let new_pos = if args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let old_pos = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                interp.buffer.point()
            };
            let inhibit_motion = interp
                .lookup_var("inhibit-field-text-motion", env)
                .is_some_and(|value| value.is_truthy());
            let mut constrained = new_pos;
            if !inhibit_motion
                && interp.buffer.text_property_at(new_pos, "field")
                    != interp.buffer.text_property_at(old_pos, "field")
            {
                constrained = if new_pos < old_pos {
                    call(
                        interp,
                        "field-beginning",
                        &[Value::Integer(old_pos as i64)],
                        env,
                    )?
                    .as_integer()? as usize
                } else {
                    call(interp, "field-end", &[Value::Integer(old_pos as i64)], env)?
                        .as_integer()? as usize
                };
            }
            if args[0].is_nil() {
                interp.buffer.goto_char(constrained);
            }
            Ok(Value::Integer(constrained as i64))
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
        "make-indirect-buffer" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let base_id = interp.resolve_buffer_id(&args[0])?;
            let new_name = string_text(&args[1])?;
            let clone = args.get(2).is_some_and(|value| value.is_truthy());
            let (new_id, _) = interp.create_buffer(&new_name);
            let (text, props, point, mark, file, base_overlays) = {
                let base = interp
                    .get_buffer_by_id(base_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", base_id)))?;
                (
                    base.buffer_string(),
                    base.substring_property_spans(base.point_min(), base.point_max()),
                    base.point(),
                    base.mark(),
                    base.file.clone(),
                    base.overlays.clone(),
                )
            };
            let overlays = if clone {
                base_overlays
                    .into_iter()
                    .map(|mut overlay| {
                        overlay.id = interp.alloc_overlay_id();
                        overlay.buffer_id = Some(new_id);
                        overlay
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if let Some(buffer) = interp.get_buffer_by_id_mut(new_id) {
                *buffer = crate::buffer::Buffer::from_text(&new_name, &text);
                buffer.file = file;
                buffer.goto_char(point);
                if let Some(mark) = mark {
                    buffer.set_mark(mark);
                }
                for span in props {
                    buffer.add_text_properties(span.start + 1, span.end + 1, &span.props);
                }
                buffer.overlays = overlays;
            }
            interp.register_indirect_buffer(new_id, base_id);
            Ok(Value::Buffer(new_id, new_name))
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
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            Ok(interp
                .buffer_base_id(buffer_id)
                .and_then(|base_id| interp.get_buffer_by_id(base_id).map(|buffer| Value::Buffer(base_id, buffer.name.clone())))
                .unwrap_or(Value::Nil))
        }
        "buffer-swap-text" => {
            need_args(name, args, 1)?;
            let other_id = interp.resolve_buffer_id(&args[0])?;
            let current_id = interp.current_buffer_id();
            interp.swap_buffer_text_state(current_id, other_id)?;
            Ok(Value::Nil)
        }
        "buffer-local-value" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?.to_string();
            let buffer_id = interp.resolve_buffer_id(&args[1])?;
            Ok(interp
                .buffer_local_value(buffer_id, &symbol)
                .or_else(|| interp.lookup_var(&symbol, env))
                .unwrap_or(Value::Nil))
        }
        "buffer-local-variables" => {
            let mut vars = interp
                .buffer_local_variables(interp.current_buffer_id())
                .into_iter()
                .map(|(name, value)| Value::cons(Value::Symbol(name), value))
                .collect::<Vec<_>>();
            vars.push(Value::cons(
                Value::Symbol("buffer-undo-list".into()),
                buffer_undo_list_value(&interp.buffer),
            ));
            Ok(Value::list(vars))
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
            let id = match &args[0] {
                Value::String(name) => interp
                    .find_buffer(name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(name).0),
                _ => interp.resolve_buffer_id(&args[0])?,
            };
            interp.switch_to_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "buffer-file-name" => Ok(interp
            .buffer
            .file
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Nil)),
        "find-file-noselect" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            if let Some((id, name)) = interp.find_buffer(&path) {
                return Ok(Value::Buffer(id, name));
            }
            let (id, _) = interp.create_buffer(&path);
            let contents = std::fs::read_to_string(&path).unwrap_or_default();
            if let Some(buffer) = interp.get_buffer_by_id_mut(id) {
                *buffer = crate::buffer::Buffer::from_text(&path, &contents);
                buffer.file = Some(path.clone());
                buffer.set_unmodified();
            }
            Ok(Value::Buffer(id, path))
        }
        "find-file" => {
            need_args(name, args, 1)?;
            let buffer = call(interp, "find-file-noselect", args, env)?;
            let id = interp.resolve_buffer_id(&buffer)?;
            interp.switch_to_buffer_id(id)?;
            Ok(buffer)
        }
        "file-exists-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if fs::metadata(path).is_ok() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "delete-file" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            fs::remove_file(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "write-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[2])?;
            let text = if string_like(&args[0]).is_some() && args.get(1).is_none_or(Value::is_nil) {
                string_text(&args[0])?
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?
            };
            fs::write(&path, text).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::String(path.to_string()))
        }
        "insert-file-contents" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            let text =
                fs::read_to_string(&path).map_err(|error| LispError::Signal(error.to_string()))?;
            let count = text.chars().count();
            interp.insert_current_buffer(&text);
            Ok(Value::list([
                Value::String(path.to_string()),
                Value::Integer(count as i64),
            ]))
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
            let pos = position_from_value(interp, &args[0])?;
            interp.buffer.set_mark(pos);
            Ok(Value::Nil)
        }
        "push-mark" => {
            let pos = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            interp.buffer.set_mark(pos);
            Ok(Value::Nil)
        }
        "mark" => Ok(match interp.buffer.mark() {
            Some(m) => Value::Integer(m as i64),
            None => Value::Nil,
        }),
        "make-marker" => Ok(interp.make_marker()),
        "copy-marker" => {
            need_args(name, args, 1)?;
            let insertion_type = args.get(1).is_some_and(Value::is_truthy);
            interp.copy_marker_value(&args[0], insertion_type)
        }
        "point-marker" => interp.copy_marker_value(
            &Value::Integer(interp.buffer.point() as i64),
            false,
        ),
        "mark-marker" => match interp.buffer.mark() {
            Some(pos) => interp.copy_marker_value(&Value::Integer(pos as i64), false),
            None => interp.copy_marker_value(&Value::Nil, false),
        },
        "point-min-marker" => interp.copy_marker_value(
            &Value::Integer(interp.buffer.point_min() as i64),
            false,
        ),
        "point-max-marker" => interp.copy_marker_value(
            &Value::Integer(interp.buffer.point_max() as i64),
            false,
        ),
        "marker-buffer" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            match interp.marker_buffer_id(marker_id) {
                Some(buffer_id) => {
                    let buffer_name = interp
                        .buffer_list
                        .iter()
                        .find(|(id, _)| *id == buffer_id)
                        .map(|(_, name)| name.clone())
                        .unwrap_or_else(|| "*unknown*".to_string());
                    Ok(Value::Buffer(buffer_id, buffer_name))
                }
                None => Ok(Value::Nil),
            }
        }
        "marker-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-last-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_last_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-insertion-type" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(if interp.marker_insertion_type(marker_id).unwrap_or(false) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "set-marker-insertion-type" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let insertion_type = args[1].is_truthy();
            interp.set_marker_insertion_type(marker_id, insertion_type);
            Ok(if insertion_type { Value::T } else { Value::Nil })
        }
        "set-marker" | "move-marker" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let (position, buffer_id) = marker_target(interp, &args[1], args.get(2))?;
            interp.set_marker(marker_id, position, buffer_id)?;
            Ok(args[0].clone())
        }
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
            Ok(Value::String(render_prin1(interp, &args[0], env)?))
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
        "display-graphic-p" | "frame-parameter" | "transient-mark-mode" => Ok(Value::Nil),
        "selected-window" => Ok(Value::Symbol("window".into())),
        "selected-frame" => Ok(Value::Symbol("frame".into())),
        "get-buffer-window" => Ok(Value::Symbol("window".into())),
        "set-window-start" | "set-window-point" => Ok(Value::T),
        "facemenu-add-face" => {
            need_args(name, args, 3)?;
            let face = args[0].clone();
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            interp.buffer.put_text_property(start, end, "face", face);
            Ok(Value::Nil)
        }

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
        "executable-find" => {
            need_args(name, args, 1)?;
            let executable = args[0].as_string()?;
            Ok(find_executable(executable).map(Value::String).unwrap_or(Value::Nil))
        }
        "add-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
            let append = args.get(2).is_some_and(|value| value.is_truthy());
            let local = args.get(3).is_some_and(|value| value.is_truthy());
            let mut hooks = if local {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), &hook_name)
                    .unwrap_or_default()
            } else {
                interp
                    .lookup_var(&hook_name, env)
                    .map(|value| value.to_vec().unwrap_or_default())
                    .unwrap_or_default()
            };
            if !hooks.contains(&function) {
                if append {
                    hooks.push(function);
                } else {
                    hooks.insert(0, function);
                }
            }
            if local {
                interp.set_buffer_local_hook(interp.current_buffer_id(), &hook_name, hooks);
            } else {
                interp.set_variable(&hook_name, Value::list(hooks), &mut Vec::new());
            }
            Ok(Value::Nil)
        }
        "symbol-function" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(
                interp
                    .lookup_function(symbol, env)
                    .unwrap_or_else(|_| Value::String(format!("#<function {}>", symbol))),
            )
        }
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(s.to_string()))
        }
        "char-from-name" => {
            need_args(name, args, 1)?;
            let name = args[0].as_string()?;
            let ch = match name {
                "SMILE" => 0x263A,
                _ => return Ok(Value::Nil),
            };
            Ok(Value::Integer(ch))
        }
        "evenp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].as_integer()? % 2 == 0 {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-truename" => {
            need_args(name, args, 1)?;
            Ok(Value::String(args[0].as_string()?.to_string()))
        }
        "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
        "group-name" => {
            need_args(name, args, 1)?;
            let gid = match &args[0] {
                Value::Integer(value) => *value,
                Value::Float(value) => *value as i64,
                _ => return Err(LispError::Signal("Invalid GID specification".into())),
            };
            Ok(group_name_from_gid(gid)?.map(Value::String).unwrap_or(Value::Nil))
        }
        "save-buffer" => {
            let Some(path) = interp.buffer.file.clone() else {
                return Ok(Value::Nil);
            };
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.set_unmodified();
            Ok(Value::Nil)
        }
        "auto-save-mode" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            if enabled {
                let path = auto_save_path_for_buffer(&interp.buffer);
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::String(path),
                );
                Ok(Value::T)
            } else {
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::Nil,
                );
                Ok(Value::Nil)
            }
        }
        "do-auto-save" => {
            let path = interp
                .buffer_local_value(interp.current_buffer_id(), "buffer-auto-save-file-name")
                .and_then(|value| string_text(&value).ok())
                .unwrap_or_else(|| auto_save_path_for_buffer(&interp.buffer));
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-auto-save-file-name",
                Value::String(path),
            );
            interp.buffer.set_autosaved();
            Ok(Value::Nil)
        }
        "make-hash-table" => Ok(Value::String("#<hash-table>".into())),
        "regexp-quote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(regexp_quote_elisp(&string_text(&args[0])?)))
        }
        "garbage-collect" => Ok(Value::Nil),
        "type-of" => {
            need_args(name, args, 1)?;
            let name = match &args[0] {
                Value::Nil => "symbol",
                Value::T => "symbol",
                Value::Integer(_) => "integer",
                Value::BigInteger(_) => "integer",
                Value::Float(_) => "float",
                Value::String(_) => "string",
                Value::Symbol(_) => "symbol",
                Value::Cons(_, _) => "cons",
                Value::BuiltinFunc(_) => "subr",
                Value::Lambda(_, _, _) => "cons", // Emacs closures are cons cells
                Value::Buffer(_, _) => "buffer",
                Value::Marker(_) => "marker",
                Value::Overlay(_) => "overlay",
                Value::CharTable(_) => "char-table",
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
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut items = args[0].to_vec()?;
            let pred = args.get(1).cloned();
            // Sort using the predicate. We need to call back into the interpreter.
            // Use a simple insertion sort to avoid issues with the borrow checker
            // and Rust's sort requiring Fn (not FnMut with &mut self).
            let len = items.len();
            for i in 1..len {
                let mut j = i;
                while j > 0 {
                    let result = if let Some(pred) = &pred {
                        let pred_args = [items[j - 1].clone(), items[j].clone()];
                        match pred {
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
                        }
                    } else if default_sort_lt(interp, &items[j - 1], &items[j])? {
                        Value::T
                    } else {
                        Value::Nil
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
            let mut items = vec![Value::symbol("vector")];
            items.extend(args.iter().cloned());
            Ok(Value::list(items))
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
                    let items = vector_items(&args[0])?;
                    items.get(idx).cloned().ok_or_else(|| {
                        LispError::Signal("Args out of range".into())
                    })
                }
            }
        }

        "aset" => {
            need_args(name, args, 3)?;
            match &args[0] {
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
                    interp.char_table_set(*id, key, args[2].clone())?;
                    Ok(args[2].clone())
                }
                _ => Ok(args[2].clone()),
            }
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

        "propertize" => {
            if args.is_empty() || args.len().is_multiple_of(2) {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let text = string_text(&args[0])?;
            let props = args[1..]
                .chunks(2)
                .map(|pair| Ok((pair[0].as_symbol()?.to_string(), pair[1].clone())))
                .collect::<Result<Vec<_>, LispError>>()?;
            let len = text.chars().count();
            Ok(string_like_value(
                text,
                vec![TextPropertySpan {
                    start: 0,
                    end: len,
                    props,
                }],
            ))
        }

        "make-char-table" => {
            need_args(name, args, 1)?;
            let purpose = args[0].as_symbol()?;
            Ok(interp.make_char_table(purpose))
        }

        "translate-region-internal" => {
            need_args(name, args, 3)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let table_id = match &args[2] {
                Value::CharTable(id) => *id,
                _ => {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[2].type_name(),
                    ))
                }
            };
            if interp.char_table_purpose(table_id) != Some("translation-table") {
                return Err(LispError::Signal("Not a translation table".into()));
            }
            let mut changed = 0i64;
            let mut translated = String::new();
            for pos in from..to {
                let source_char = interp
                    .buffer
                    .text_property_at(pos, "emaxx-raw-char")
                    .and_then(|value| value.as_integer().ok())
                    .map(|value| value as u32)
                    .or_else(|| interp.buffer.char_at(pos).map(|ch| ch as u32))
                    .unwrap_or_default();
                let mapped = interp
                    .char_table_get(table_id, source_char)
                    .and_then(|value| value.as_integer().ok())
                    .map(|value| value as u32)
                    .unwrap_or(source_char);
                if mapped != source_char {
                    changed += 1;
                }
                if let Some(mapped_char) = char::from_u32(mapped) {
                    translated.push(mapped_char);
                }
            }
            interp
                .delete_region_current_buffer(from, to)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(from);
            interp.insert_current_buffer(&translated);
            Ok(Value::Integer(changed))
        }

        "undo-boundary" => {
            interp.buffer.push_undo_boundary();
            Ok(Value::Nil)
        }

        "undo" => {
            interp
                .buffer
                .undo()
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Nil)
        }

        "undo-more" => {
            let count = if args.is_empty() {
                1
            } else {
                args[0].as_integer()?
            };
            for _ in 0..count.max(0) {
                interp
                    .buffer
                    .undo()
                    .map_err(|e| LispError::Signal(e.to_string()))?;
            }
            Ok(Value::Nil)
        }

        "take" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()?.max(0) as usize;
            let items = args[1].to_vec()?;
            Ok(Value::list(items.into_iter().take(n)))
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

        "match-beginning" | "match-end" => {
            need_args(name, args, 1)?;
            let index = args[0].as_integer()?;
            if index < 0 {
                return Err(LispError::Signal("Args out of range".into()));
            }
            let match_data = interp
                .last_match_data
                .as_ref()
                .ok_or_else(|| LispError::Signal("No match data, because no search succeeded".into()))?;
            let result = match_data
                .get(index as usize)
                .and_then(|entry| *entry)
                .map(|(start, end)| {
                    if name == "match-beginning" {
                        Value::Integer(start as i64)
                    } else {
                        Value::Integer(end as i64)
                    }
                })
                .unwrap_or(Value::Nil);
            Ok(result)
        }

        "looking-at" => {
            need_args(name, args, 1)?;
            let pattern = string_text(&args[0])?;
            interp.set_variable(
                "last-looking-at-pattern",
                Value::String(pattern.clone()),
                &mut env.clone(),
            );
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let pos = interp.buffer.point();
            let tail = interp
                .buffer
                .buffer_substring(pos, interp.buffer.point_max())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if let Some(captures) = regex.captures(&tail)
                && let Some(matched) = captures.get(0)
                && matched.start() == 0
            {
                set_match_data(interp, pos, &tail, &captures);
                Ok(Value::T)
            } else {
                interp.last_match_data = None;
                Ok(Value::Nil)
            }
        }

        "replace-match" => {
            need_args(name, args, 1)?;
            let replacement = string_text(&args[0])?;
            let (start, end) = interp
                .last_match_data
                .as_ref()
                .and_then(|entries| entries.first().and_then(|entry| *entry))
                .ok_or_else(|| LispError::Signal("No previous search".into()))?;
            delete_region_with_hooks(interp, start, end, env)?;
            interp.buffer.goto_char(start);
            insert_text_with_hooks(interp, &replacement, &[], false, false, env)?;
            interp.last_match_data = None;
            Ok(Value::Nil)
        }

        "replace-region-contents" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let replacement = replacement_content(interp, &args[2])?;
            let saved_point = interp.buffer.point();
            let saved_markers =
                interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let removed_len = to.saturating_sub(from);
            let inserted_len = replacement.text.chars().count();
            delete_region_with_hooks(interp, from, to, env)?;
            interp.buffer.goto_char(from);
            insert_text_with_hooks(interp, &replacement.text, &replacement.props, false, false, env)?;
            for (marker_id, original) in saved_markers {
                let Some(original_pos) = original else {
                    continue;
                };
                let insertion_type = interp.marker_insertion_type(marker_id).unwrap_or(false);
                let new_pos = if original_pos < from {
                    original_pos
                } else if original_pos == from {
                    from
                } else if original_pos < to {
                    if insertion_type {
                        from + inserted_len
                    } else {
                        from
                    }
                } else {
                    ((original_pos as isize) + inserted_len as isize - removed_len as isize)
                        .max(from as isize) as usize
                };
                let _ = interp.set_marker(
                    marker_id,
                    Some(new_pos),
                    Some(interp.current_buffer_id()),
                );
            }
            if saved_point > to {
                let target = ((saved_point as isize) + inserted_len as isize - removed_len as isize)
                    .max(from as isize) as usize;
                interp.buffer.goto_char(target);
            } else if (from..=to).contains(&saved_point) {
                let trailing = to.saturating_sub(saved_point);
                let target = from + inserted_len.saturating_sub(trailing);
                interp.buffer.goto_char(target);
            }
            Ok(Value::Nil)
        }
        "flush-lines" => {
            need_args(name, args, 3)?;
            let pattern = string_text(&args[0])?;
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let filtered = text
                .split_inclusive('\n')
                .filter(|line| !regex.is_match(&line.to_lowercase()))
                .collect::<String>();
            delete_region_with_hooks(interp, start, end, env)?;
            insert_text_with_hooks(interp, &filtered, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "subst-char-in-region" => {
            need_args(name, args, 4)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            let old = args[2].as_integer()? as u32;
            let new = args[3].as_integer()? as u32;
            let old = char::from_u32(old)
                .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let new = char::from_u32(new)
                .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let text = interp
                .buffer
                .buffer_substring(from, to)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let replaced: String = text
                .chars()
                .map(|ch| if ch == old { new } else { ch })
                .collect();
            delete_region_with_hooks(interp, from, to, env)?;
            insert_text_with_hooks(interp, &replaced, &[], false, false, env)?;
            Ok(Value::Nil)
        }

        "internal--labeled-narrow-to-region" => {
            need_args(name, args, 3)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let label = args[2].as_symbol()?.to_string();
            let state = Value::list([
                Value::Integer(interp.buffer.point_min() as i64),
                Value::Integer(interp.buffer.point_max() as i64),
            ]);
            interp.set_variable(
                &format!("__emaxx-labeled-restriction-{label}"),
                state,
                &mut env.clone(),
            );
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::list([Value::Integer(start as i64), Value::Integer(end as i64)]),
                &mut env.clone(),
            );
            interp.buffer.narrow_to_region(start, end);
            Ok(Value::Nil)
        }

        "internal--labeled-widen" => {
            need_args(name, args, 1)?;
            let label = args[0].as_symbol()?.to_string();
            interp.set_variable(
                "__emaxx-active-labeled-restriction",
                Value::Nil,
                &mut env.clone(),
            );
            if let Some(state) =
                interp.lookup_var(&format!("__emaxx-labeled-restriction-{label}"), env)
            {
                let values = state.to_vec()?;
                let start = values.first().and_then(|v| v.as_integer().ok()).unwrap_or(1) as usize;
                let end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64) as usize;
                interp.buffer.narrow_to_region(start, end);
            } else {
                interp.buffer.widen();
            }
            Ok(Value::Nil)
        }

        "transpose-regions" => {
            if args.len() < 4 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let mut start1 = position_from_value(interp, &args[0])?;
            let mut end1 = position_from_value(interp, &args[1])?;
            let mut start2 = position_from_value(interp, &args[2])?;
            let mut end2 = position_from_value(interp, &args[3])?;
            if start1 > end1 {
                std::mem::swap(&mut start1, &mut end1);
            }
            if start2 > end2 {
                std::mem::swap(&mut start2, &mut end2);
            }
            if start2 < end1 {
                std::mem::swap(&mut start1, &mut start2);
                std::mem::swap(&mut end1, &mut end2);
            }
            if start2 < end1 {
                return Err(LispError::Signal("Transposed regions overlap".into()));
            }
            if (start1 == end1 || start2 == end2) && end1 == start2 {
                return Ok(Value::Nil);
            }
            let leave_markers = args.get(4).is_some_and(|value| value.is_truthy());
            let saved_markers =
                interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let region1_text = interp
                .buffer
                .buffer_substring(start1, end1)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let region1_props = interp.buffer.substring_property_spans(start1, end1);
            let region2_text = interp
                .buffer
                .buffer_substring(start2, end2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let region2_props = interp.buffer.substring_property_spans(start2, end2);
            let gap = interp
                .buffer
                .buffer_substring(end1, start2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let gap_len = gap.chars().count();
            interp
                .delete_region_current_buffer(start2, end2)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp
                .delete_region_current_buffer(start1, end1)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(start1);
            interp.insert_current_buffer(&region2_text);
            for span in &region2_props {
                interp.buffer.add_text_properties(
                    start1 + span.start,
                    start1 + span.end,
                    &span.props,
                );
            }
            let insert_region1_at = start1 + region2_text.chars().count() + gap_len;
            interp.buffer.goto_char(insert_region1_at);
            interp.insert_current_buffer(&region1_text);
            for span in &region1_props {
                interp.buffer.add_text_properties(
                    insert_region1_at + span.start,
                    insert_region1_at + span.end,
                    &span.props,
                );
            }
            let len1 = end1 - start1;
            let len2 = end2 - start2;
            let diff = len2 as isize - len1 as isize;
            let amt1 = len2 + (start2 - end1);
            let amt2 = len1 + (start2 - end1);
            for (marker_id, original) in saved_markers {
                let Some(original_pos) = original else {
                    continue;
                };
                let new_pos = if leave_markers || original_pos < start1 || original_pos >= end2 {
                    original_pos
                } else if original_pos < end1 {
                    original_pos + amt1
                } else if original_pos < start2 {
                    ((original_pos as isize) + diff) as usize
                } else {
                    original_pos - amt2
                };
                let _ = interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
            }
            Ok(Value::Nil)
        }

        "dabbrev-expand" => {
            let point = interp.buffer.point();
            let mut start = point;
            while start > interp.buffer.point_min() {
                let Some(ch) = interp.buffer.char_at(start - 1) else {
                    break;
                };
                if !(ch.is_alphanumeric() || ch == '-' || ch == '_') {
                    break;
                }
                start -= 1;
            }
            let prefix = interp
                .buffer
                .buffer_substring(start, point)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if prefix.is_empty() {
                return Ok(Value::Nil);
            }
            let haystack = interp.buffer.buffer_string();
            let prefix_start = haystack
                .chars()
                .take(start.saturating_sub(1))
                .map(char::len_utf8)
                .sum::<usize>();
            if let Some(found) = haystack[..prefix_start].rfind(&prefix)
                && let Some(expansion) = expand_symbol_at(&haystack, found, &prefix)
                && expansion != prefix
            {
                delete_region_with_hooks(interp, start, point, env)?;
                interp.buffer.goto_char(start);
                insert_text_with_hooks(interp, &expansion, &[], false, false, env)?;
            }
            Ok(Value::Nil)
        }

        "encode-coding-region" | "decode-coding-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let _ = position_from_value(interp, &args[0])?;
            let _ = position_from_value(interp, &args[1])?;
            let _ = args[2].as_symbol()?;
            Ok(Value::Nil)
        }

        "encode-coding-string" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }

        "decode-coding-string" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let decoded = decode_coding_text(&string_text(&args[0])?, args[1].as_symbol()?)?;
            if let Some(buffer) = args.get(3)
                && !buffer.is_nil()
            {
                let buffer_id = interp.resolve_buffer_id(buffer)?;
                let saved_buffer_id = interp.current_buffer_id();
                interp.switch_to_buffer_id(buffer_id)?;
                let insert_at = interp.buffer.point();
                insert_text_with_hooks(interp, &decoded, &[], false, false, env)?;
                interp.buffer.goto_char(insert_at);
                let _ = interp.switch_to_buffer_id(saved_buffer_id);
            }
            Ok(Value::String(decoded))
        }

        "insert-before-markers" => {
            let insert_at = interp.buffer.point();
            let combined = combine_insert_args(args)?;
            let nchars = combined.text.chars().count();
            insert_text_with_hooks(interp, &combined.text, &combined.props, false, true, env)?;
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
            Ok(Value::Nil)
        }
        "insert-before-markers-and-inherit" => {
            let insert_at = interp.buffer.point();
            let combined = combine_insert_args(args)?;
            let nchars = combined.text.chars().count();
            insert_text_with_hooks(interp, &combined.text, &combined.props, true, true, env)?;
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

fn position_from_value(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    match value {
        Value::Integer(pos) if *pos >= 0 => Ok(*pos as usize),
        Value::Marker(id) => interp
            .marker_position(*id)
            .ok_or_else(|| LispError::TypeError("integer-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError(
            "integer-or-marker-p".into(),
            value.type_name(),
        )),
    }
}

fn marker_id_from_value(value: &Value) -> Result<u64, LispError> {
    match value {
        Value::Marker(id) => Ok(*id),
        _ => Err(LispError::TypeError("marker".into(), value.type_name())),
    }
}

fn marker_target(
    interp: &Interpreter,
    value: &Value,
    buffer: Option<&Value>,
) -> Result<(Option<usize>, Option<u64>), LispError> {
    match value {
        Value::Nil => Ok((None, None)),
        Value::Marker(marker_id) => Ok((
            interp.marker_position(*marker_id),
            interp.marker_buffer_id(*marker_id),
        )),
        _ => {
            let position = position_from_value(interp, value)?;
            let buffer_id = if let Some(buffer) = buffer {
                if buffer.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(buffer)?
                }
            } else {
                interp.current_buffer_id()
            };
            Ok((Some(position), Some(buffer_id)))
        }
    }
}

fn vector_items(value: &Value) -> Result<Vec<Value>, LispError> {
    let items = value.to_vec()?;
    if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal") {
        Ok(items.into_iter().skip(1).collect())
    } else {
        Ok(items)
    }
}

fn position_bytes(interp: &Interpreter, pos: usize) -> Option<usize> {
    let text = interp.buffer.buffer_string();
    let char_len = text.chars().count();
    if pos == 0 || pos > char_len + 1 {
        return None;
    }
    Some(1 + text.chars().take(pos - 1).map(char::len_utf8).sum::<usize>())
}

fn byte_to_position(interp: &Interpreter, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = interp.buffer.buffer_string();
    let total_bytes = text.len();
    if byte > total_bytes + 1 {
        return None;
    }
    if byte == total_bytes + 1 {
        return Some(text.chars().count() + 1);
    }
    let mut current_byte = 1usize;
    for (index, ch) in text.chars().enumerate() {
        let next = current_byte + ch.len_utf8();
        if byte < next {
            return Some(index + 1);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

fn column_at(interp: &Interpreter, line_start: usize, pos: usize) -> usize {
    let tab_width = interp
        .lookup_var("tab-width", &Vec::new())
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(8)
        .max(1) as usize;
    let mut col = 0usize;
    for p in line_start..pos {
        match interp.buffer.char_at(p) {
            Some('\t') => col = (col / tab_width + 1) * tab_width,
            Some(_) => col += 1,
            None => break,
        }
    }
    col
}

fn compare_buffer_substrings(left: &str, right: &str) -> i64 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();
    let min_len = left_chars.len().min(right_chars.len());
    for index in 0..min_len {
        if left_chars[index] != right_chars[index] {
            let offset = (index + 1) as i64;
            return if left_chars[index] < right_chars[index] {
                -offset
            } else {
                offset
            };
        }
    }
    if left_chars.len() == right_chars.len() {
        0
    } else {
        let offset = (min_len + 1) as i64;
        if left_chars.len() < right_chars.len() {
            -offset
        } else {
            offset
        }
    }
}

fn translate_elisp_regex(pattern: &str) -> String {
    let mut translated = String::new();
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('\'') => translated.push('$'),
                Some('(') => translated.push('('),
                Some(')') => translated.push(')'),
                Some('|') => translated.push('|'),
                Some(other) => {
                    translated.push('\\');
                    translated.push(other);
                }
                None => translated.push('\\'),
            }
        } else {
            match ch {
                '(' | ')' | '{' | '}' => {
                    translated.push('\\');
                    translated.push(ch);
                }
                _ => translated.push(ch),
            }
        }
    }
    translated
}

fn regexp_quote_elisp(pattern: &str) -> String {
    let mut quoted = String::new();
    for ch in pattern.chars() {
        match ch {
            '(' => quoted.push_str("[(]"),
            ')' => quoted.push_str("[)]"),
            '[' => quoted.push_str("\\["),
            ']' => quoted.push_str("\\]"),
            '{' => quoted.push_str("\\{"),
            '}' => quoted.push_str("\\}"),
            '\\' => quoted.push_str("[\\\\]"),
            '.' | '*' | '+' | '?' | '^' | '$' | '|' => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }
    quoted
}

fn set_match_data(
    interp: &mut Interpreter,
    start_pos: usize,
    haystack: &str,
    captures: &regex::Captures<'_>,
) {
    interp.last_match_data = Some(
        captures
            .iter()
            .map(|matched| {
                matched.map(|matched| {
                    let start = start_pos + haystack[..matched.start()].chars().count();
                    let end = start_pos + haystack[..matched.end()].chars().count();
                    (start, end)
                })
            })
            .collect(),
    );
}

fn expand_symbol_at(haystack: &str, found: usize, prefix: &str) -> Option<String> {
    let tail = &haystack[found..];
    let end = tail
        .char_indices()
        .take_while(|(_, ch)| ch.is_alphanumeric() || *ch == '-' || *ch == '_')
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(prefix.len());
    let expansion = &tail[..end];
    if expansion.starts_with(prefix) {
        Some(expansion.to_string())
    } else {
        None
    }
}

fn decode_coding_text(text: &str, coding: &str) -> Result<String, LispError> {
    match coding {
        "utf-8" | "utf-8-unix" => {
            let bytes = text
                .chars()
                .map(|ch| {
                    u8::try_from(ch as u32)
                        .map_err(|_| LispError::Signal("Invalid byte in utf-8 text".into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        }
        "windows-1252" | "iso-latin-1" | "raw-text" | "undecided" => Ok(text.to_string()),
        _ => Ok(text.to_string()),
    }
}

fn current_group_id() -> Result<u32, LispError> {
    let output = Command::new("id")
        .arg("-g")
        .output()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if !output.status.success() {
        return Err(LispError::Signal("Failed to determine current gid".into()));
    }
    let value = String::from_utf8_lossy(&output.stdout);
    value
        .trim()
        .parse::<u32>()
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn group_name_from_gid(gid: i64) -> Result<Option<String>, LispError> {
    if cfg!(target_os = "macos") {
        let output = Command::new("dscacheutil")
            .args(["-q", "group", "-a", "gid", &gid.to_string()])
            .output();
        if let Ok(output) = output
            && output.status.success()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                if let Some(name) = line.strip_prefix("name:") {
                    return Ok(Some(name.trim().to_string()));
                }
            }
        }
    }

    let output = Command::new("getent")
        .args(["group", &gid.to_string()])
        .output();
    if let Ok(output) = output
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        if let Some(name) = text.split(':').next()
            && !name.is_empty()
        {
            return Ok(Some(name.to_string()));
        }
    }

    if let Ok(groups) = std::fs::read_to_string("/etc/group") {
        for line in groups.lines() {
            let mut parts = line.split(':');
            let Some(name) = parts.next() else { continue };
            let _ = parts.next();
            let Some(entry_gid) = parts.next() else {
                continue;
            };
            if entry_gid.parse::<i64>().ok() == Some(gid) {
                return Ok(Some(name.to_string()));
            }
        }
    }

    Ok(None)
}

fn find_executable(name: &str) -> Option<String> {
    if name.contains(std::path::MAIN_SEPARATOR) && std::path::Path::new(name).exists() {
        return Some(name.to_string());
    }
    let path = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path) {
        let candidate = entry.join(name);
        if candidate.exists() {
            return Some(candidate.display().to_string());
        }
    }
    None
}

fn default_sort_lt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    let left_marker = if let Value::Marker(id) = left {
        interp.marker_position(*id).or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    let right_marker = if let Value::Marker(id) = right {
        interp.marker_position(*id).or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    if let (Some(left), Some(right)) = (left_marker, right_marker) {
        return Ok(left < right);
    }
    if let (Ok(left), Ok(right)) = (left.as_integer(), right.as_integer()) {
        return Ok(left < right);
    }
    if let (Some(left), Some(right)) = (string_like(left), string_like(right)) {
        return Ok(left.text < right.text);
    }
    Ok(left.to_string() < right.to_string())
}

fn replacement_content(interp: &Interpreter, source: &Value) -> Result<StringLike, LispError> {
    if let Some(string) = string_like(source) {
        return Ok(string);
    }
    match source {
        Value::Buffer(id, _) => {
            let buffer = interp
                .get_buffer_by_id(*id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))?;
            Ok(StringLike {
                text: buffer
                    .buffer_substring(buffer.point_min(), buffer.point_max())
                    .map_err(|e| LispError::Signal(e.to_string()))?,
                props: buffer.substring_property_spans(buffer.point_min(), buffer.point_max()),
            })
        }
        _ => {
            let items = vector_items(source)?;
            if items.len() >= 3 {
                let buffer_id = interp.resolve_buffer_id(&items[0])?;
                let start = position_from_value(interp, &items[1])?;
                let end = position_from_value(interp, &items[2])?;
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                Ok(StringLike {
                    text: buffer
                        .buffer_substring(start, end)
                        .map_err(|e| LispError::Signal(e.to_string()))?,
                    props: buffer.substring_property_spans(start, end),
                })
            } else {
                Err(LispError::TypeError(
                    "string-or-buffer".into(),
                    source.type_name(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct StringLike {
    text: String,
    props: Vec<TextPropertySpan>,
}

fn string_like(value: &Value) -> Option<StringLike> {
    match value {
        Value::String(text) => Some(StringLike {
            text: text.clone(),
            props: Vec::new(),
        }),
        Value::Cons(_, _) => {
            let items = vector_items(value).ok()?;
            let Value::String(text) = items.first()?.clone() else {
                return None;
            };
            let mut props = Vec::new();
            let mut i = 1;
            while i + 2 < items.len() {
                let start = items[i].as_integer().ok()? as usize;
                let end = items[i + 1].as_integer().ok()? as usize;
                let plist = plist_pairs(&items[i + 2]).ok()?;
                props.push(TextPropertySpan {
                    start,
                    end,
                    props: plist,
                });
                i += 3;
            }
            Some(StringLike { text, props })
        }
        _ => None,
    }
}

fn string_text(value: &Value) -> Result<String, LispError> {
    string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::TypeError("string".into(), value.type_name()))
}

fn string_like_value(text: String, props: Vec<TextPropertySpan>) -> Value {
    if props.is_empty() {
        Value::String(text)
    } else {
        let mut items = vec![Value::symbol("vector"), Value::String(text)];
        for span in props {
            items.push(Value::Integer(span.start as i64));
            items.push(Value::Integer(span.end as i64));
            items.push(plist_value(&span.props));
        }
        Value::list(items)
    }
}

fn plist_pairs(value: &Value) -> Result<Vec<(String, Value)>, LispError> {
    let items = value.to_vec()?;
    let mut props = Vec::new();
    let mut i = 0;
    while i + 1 < items.len() {
        let key = items[i].as_symbol()?.to_string();
        props.push((key, items[i + 1].clone()));
        i += 2;
    }
    Ok(props)
}

fn plist_value(props: &[(String, Value)]) -> Value {
    let mut items = Vec::new();
    for (key, value) in props {
        items.push(Value::Symbol(key.clone()));
        items.push(value.clone());
    }
    Value::list(items)
}

fn shift_string_props(props: &[TextPropertySpan], offset: usize) -> Vec<TextPropertySpan> {
    props
        .iter()
        .map(|span| TextPropertySpan {
            start: span.start + offset,
            end: span.end + offset,
            props: span.props.clone(),
        })
        .collect()
}

fn slice_string_props(
    props: &[TextPropertySpan],
    from: usize,
    to: usize,
) -> Vec<TextPropertySpan> {
    let mut sliced = Vec::new();
    for span in props {
        let start = span.start.max(from);
        let end = span.end.min(to);
        if start < end {
            sliced.push(TextPropertySpan {
                start: start - from,
                end: end - from,
                props: span.props.clone(),
            });
        }
    }
    merge_string_props(sliced)
}

fn merge_string_props(mut props: Vec<TextPropertySpan>) -> Vec<TextPropertySpan> {
    props.retain(|span| span.start < span.end && !span.props.is_empty());
    props.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<TextPropertySpan> = Vec::new();
    for span in props {
        if let Some(last) = merged.last_mut()
            && last.end == span.start
            && last.props == span.props
        {
            last.end = span.end;
        } else {
            merged.push(span);
        }
    }
    merged
}

fn string_property_at(value: &Value, pos: usize, prop: &str) -> Option<Value> {
    let string = string_like(value)?;
    string
        .props
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .and_then(|span| {
            span.props
                .iter()
                .find(|(name, _)| name == prop)
                .map(|(_, value)| value.clone())
        })
}

fn string_properties_at(value: &Value, pos: usize) -> Vec<(String, Value)> {
    string_like(value)
        .and_then(|string| {
            string
                .props
                .iter()
                .find(|span| span.start <= pos && pos < span.end)
                .map(|span| span.props.clone())
        })
        .unwrap_or_default()
}

fn call_function_value(
    interp: &mut Interpreter,
    function: &Value,
    args: &[Value],
    env: &super::types::Env,
) -> Result<Value, LispError> {
    let mut items = vec![function.clone()];
    for arg in args {
        items.push(Value::list([Value::symbol("quote"), arg.clone()]));
    }
    let mut call_env = env.clone();
    interp.eval(&Value::list(items), &mut call_env)
}

fn run_change_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    args: &[Value],
    env: &super::types::Env,
) -> Result<(), LispError> {
    if interp.change_hooks_are_running() {
        return Ok(());
    }
    let hook_values = interp
        .lookup_var(hook_name, env)
        .map(|value| value.to_vec().unwrap_or_default())
        .or_else(|| interp.buffer_local_hook(interp.current_buffer_id(), hook_name))
        .unwrap_or_default();
    if hook_values.is_empty() {
        return Ok(());
    }
    interp.enter_change_hooks();
    let mut result = Ok(());
    for hook in hook_values {
        if let Err(error) = call_function_value(interp, &hook, args, env) {
            result = Err(error);
            break;
        }
    }
    interp.leave_change_hooks();
    result
}

fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &super::types::Env,
) -> Result<String, LispError> {
    if from >= to {
        return Ok(String::new());
    }
    let has_before_hooks = interp
        .lookup_var("before-change-functions", env)
        .map(|value| !value.to_vec().unwrap_or_default().is_empty())
        .or_else(|| {
            interp
                .buffer_local_hook(interp.current_buffer_id(), "before-change-functions")
                .map(|hooks| !hooks.is_empty())
        })
        .unwrap_or(false);
    if has_before_hooks {
        let start_marker = interp.make_marker();
        let end_marker = interp.make_marker();
        let dead_marker = interp.make_marker();
        if let (Value::Marker(start_id), Value::Marker(end_id), Value::Marker(dead_id)) =
            (start_marker, end_marker, dead_marker)
        {
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(start_id, Some(from), Some(buffer_id));
            let _ = interp.set_marker(end_id, Some(to), Some(buffer_id));
            let _ = interp.set_marker(dead_id, None, None);
            interp
                .buffer
                .push_undo_meta(Value::cons(Value::Marker(start_id), Value::Integer(-(from as i64))));
            interp
                .buffer
                .push_undo_meta(Value::cons(Value::Marker(end_id), Value::Integer(-(to as i64))));
            interp
                .buffer
                .push_undo_meta(Value::cons(Value::Marker(dead_id), Value::Integer(-1)));
        }
    }
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(from as i64), Value::Integer(to as i64)],
        env,
    )?;
    let deleted = interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(from as i64),
            Value::Integer(from as i64),
            Value::Integer((to - from) as i64),
        ],
        env,
    )?;
    Ok(deleted)
}

fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: &[TextPropertySpan],
    inherit: bool,
    before_markers: bool,
    env: &super::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    let start = interp.buffer.point();
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(start as i64), Value::Integer(start as i64)],
        env,
    )?;
    if before_markers {
        if inherit {
            interp.insert_current_buffer_before_markers_and_inherit(text);
        } else {
            interp.insert_current_buffer_before_markers(text);
        }
    } else if inherit {
        interp.insert_current_buffer_and_inherit(text);
    } else {
        interp.insert_current_buffer(text);
    }
    for span in props {
        interp.buffer.add_text_properties(
            start + span.start,
            start + span.end,
            &span.props,
        );
    }
    let end = start + text.chars().count();
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            Value::Integer(0),
        ],
        env,
    )?;
    Ok(())
}

fn combine_insert_args(args: &[Value]) -> Result<StringLike, LispError> {
    let mut text = String::new();
    let mut props = Vec::new();
    for arg in args {
        if let Some(string) = string_like(arg) {
            let offset = text.chars().count();
            text.push_str(&string.text);
            props.extend(shift_string_props(&string.props, offset));
        } else {
            let fragment = match arg {
                Value::Integer(n) => {
                    let offset = text.chars().count();
                    if let Some(c) = char::from_u32(*n as u32) {
                        c.to_string()
                    } else if (0..=0x3F_FFFF).contains(n) {
                        props.push(TextPropertySpan {
                            start: offset,
                            end: offset + 1,
                            props: vec![("emaxx-raw-char".into(), Value::Integer(*n))],
                        });
                        RAW_CHAR_SENTINEL.to_string()
                    } else {
                        String::new()
                    }
                }
                Value::Nil => String::new(),
                _ => arg.to_string(),
            };
            text.push_str(&fragment);
        }
    }
    Ok(StringLike {
        text,
        props: merge_string_props(props),
    })
}

fn normalize_bigint_value(value: BigInt) -> Value {
    value
        .to_i64()
        .map(Value::Integer)
        .unwrap_or(Value::BigInteger(value))
}

fn integer_like_i64(interp: &Interpreter, value: &Value) -> Result<i64, LispError> {
    match value {
        Value::Integer(n) => Ok(*n),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(|pos| pos as i64)
            .ok_or_else(|| LispError::TypeError("number-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn integer_like_bigint(interp: &Interpreter, value: &Value) -> Result<BigInt, LispError> {
    match value {
        Value::Integer(n) => Ok(BigInt::from(*n)),
        Value::BigInteger(n) => Ok(n.clone()),
        Value::Marker(id) => interp
            .marker_position(*id)
            .map(BigInt::from)
            .ok_or_else(|| LispError::TypeError("number-or-marker-p".into(), value.type_name())),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn numeric_to_f64(interp: &Interpreter, value: &Value) -> Result<f64, LispError> {
    match value {
        Value::Float(f) => Ok(*f),
        Value::BigInteger(n) => n
            .to_f64()
            .ok_or_else(|| LispError::TypeError("number".into(), value.type_name())),
        _ => Ok(integer_like_i64(interp, value)? as f64),
    }
}

fn numeric_lt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? < numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? < integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? < integer_like_i64(interp, right)?)
}

fn numeric_gt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? > numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? > integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? > integer_like_i64(interp, right)?)
}

fn number_to_string(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Integer(n) => Ok(n.to_string()),
        Value::BigInteger(n) => Ok(n.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn format_source_props(value: &Value, from: usize, to: usize) -> Option<Vec<(String, Value)>> {
    let string = string_like(value)?;
    let mut props = Vec::new();
    for span in string.props {
        if span.start < to && from < span.end {
            for (name, value) in span.props {
                if !props.iter().any(|(existing, _)| existing == &name) {
                    props.push((name, value));
                }
            }
        }
    }
    if props.is_empty() { None } else { Some(props) }
}

fn props_at_string_offset(spans: &[TextPropertySpan], pos: usize) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn auto_save_path_for_buffer(buffer: &crate::buffer::Buffer) -> String {
    if let Some(path) = &buffer.file {
        format!("{path}#")
    } else {
        std::env::temp_dir()
            .join(format!("{}.autosave", buffer.name.replace('/', "_")))
            .display()
            .to_string()
    }
}

fn forward_line_bigint(buffer: &mut crate::buffer::Buffer, n: BigInt) -> BigInt {
    if n.is_zero() {
        let _ = buffer.forward_line(0);
        return BigInt::zero();
    }

    let max_isize = BigInt::from(isize::MAX);
    let min_isize = BigInt::from(isize::MIN);
    if n >= min_isize && n <= max_isize {
        let step = match n.to_isize() {
            Some(value) => value,
            None => return BigInt::zero(),
        };
        return BigInt::from(buffer.forward_line(step) as i64);
    }

    if n.sign() == Sign::Minus {
        let available = count_backward_line_moves(buffer);
        move_line_steps(buffer, available, false);
        n + BigInt::from(available)
    } else {
        let available = count_forward_line_moves(buffer);
        move_line_steps(buffer, available, true);
        n - BigInt::from(available)
    }
}

fn move_line_steps(buffer: &mut crate::buffer::Buffer, mut steps: usize, forward: bool) {
    while steps > 0 {
        let chunk = steps.min(isize::MAX as usize);
        let _ = buffer.forward_line(if forward {
            chunk as isize
        } else {
            -(chunk as isize)
        });
        steps -= chunk;
    }
}

fn count_forward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut count = 0;
    let mut pos = buffer.point();
    while pos < buffer.point_max() {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

fn count_backward_line_moves(buffer: &crate::buffer::Buffer) -> usize {
    let mut line_start = buffer.point();
    while line_start > buffer.point_min() {
        if buffer.char_at(line_start - 1) == Some('\n') {
            break;
        }
        line_start -= 1;
    }

    let mut count = 0;
    let mut pos = buffer.point_min();
    while pos < line_start {
        if buffer.char_at(pos) == Some('\n') {
            count += 1;
        }
        pos += 1;
    }
    count
}

fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &super::types::Env,
) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(format!("{:?}", text)),
        Value::Cons(_, _) => {
            let raw_items = value.to_vec()?;
            if matches!(
                raw_items.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
            ) {
                let items = vector_items(value)?;
                Ok(format!(
                    "[{}]",
                    items
                        .iter()
                        .map(|item| render_prin1(interp, item, env))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" ")
                ))
            } else {
                Ok(value.to_string())
            }
        }
        Value::BuiltinFunc(_)
        | Value::Lambda(_, _, _)
        | Value::Buffer(_, _)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_) => {
            if let Some(function) = interp.lookup_var("print-unreadable-function", env)
                && function.is_truthy()
            {
                let rendered = call_function_value(interp, &function, &[], env)?;
                return string_text(&rendered);
            }
            Ok(value.to_string())
        }
        _ => Ok(value.to_string()),
    }
}

fn format_char_conversion(arg: &Value) -> Result<String, LispError> {
    let n = match arg {
        Value::Integer(n) => *n,
        Value::BigInteger(n) => n
            .to_i64()
            .ok_or_else(|| LispError::TypeError("character".into(), arg.type_name()))?,
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
        .ok_or_else(|| LispError::Signal(format!("Invalid character: {}", n)))
}

fn format_s_conversion(
    arg: &Value,
    precision: Option<usize>,
) -> Result<(String, Vec<TextPropertySpan>), LispError> {
    if let Some(string) = string_like(arg) {
        let end = precision
            .unwrap_or_else(|| string.text.chars().count())
            .min(string.text.chars().count());
        let text = string.text.chars().take(end).collect::<String>();
        let props = slice_string_props(&string.props, 0, end);
        return Ok((text, props));
    }
    let mut text = number_to_string(arg).unwrap_or_else(|_| arg.to_string());
    if let Some(precision) = precision {
        text = text.chars().take(precision).collect();
    }
    Ok((text, Vec::new()))
}

fn bigint_from_truncated_float(value: f64) -> Result<BigInt, LispError> {
    if !value.is_finite() {
        return Err(LispError::TypeError("integer".into(), "float".into()));
    }
    let bits = value.to_bits();
    let sign = if bits >> 63 == 0 { 1 } else { -1 };
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    if exponent == 0 || exponent < 1023 {
        return Ok(BigInt::zero());
    }
    let significand = (1u64 << 52) | mantissa;
    let shift = exponent - 1023 - 52;
    let mut result = BigInt::from(significand);
    if shift >= 0 {
        result <<= shift as usize;
    } else {
        result >>= (-shift) as usize;
    }
    if sign < 0 {
        result = -result;
    }
    Ok(result)
}

fn integer_for_format(interp: &Interpreter, value: &Value) -> Result<(Option<i64>, BigInt), LispError> {
    match value {
        Value::Integer(n) => Ok((Some(*n), BigInt::from(*n))),
        Value::BigInteger(n) => Ok((None, n.clone())),
        Value::Float(f) => Ok((None, bigint_from_truncated_float(*f)?)),
        Value::Marker(_) => {
            let n = integer_like_i64(interp, value)?;
            Ok((Some(n), BigInt::from(n)))
        }
        _ => Err(LispError::TypeError("integer".into(), value.type_name())),
    }
}

fn format_bigint_radix(value: &BigInt, radix: u32, upper: bool) -> String {
    let mut text = value.abs().to_str_radix(radix);
    if upper {
        text.make_ascii_uppercase();
    }
    text
}

fn apply_precision(mut digits: String, precision: Option<usize>) -> String {
    if let Some(precision) = precision
        && digits.len() < precision
    {
        digits = format!("{}{}", "0".repeat(precision - digits.len()), digits);
    }
    digits
}

fn format_numeric_conversion(
    interp: &Interpreter,
    arg: &Value,
    conv: char,
    flag_hash: bool,
    flag_plus: bool,
    flag_space: bool,
    precision: Option<usize>,
) -> Result<String, LispError> {
    let (_fixnum, bigint) = integer_for_format(interp, arg)?;
    let positive_sign = if flag_plus {
        "+"
    } else if flag_space {
        " "
    } else {
        ""
    };
    match conv {
        'd' => {
            let mut digits = apply_precision(bigint.abs().to_string(), precision);
            if bigint.sign() == Sign::Minus {
                digits.insert(0, '-');
            } else if !positive_sign.is_empty() {
                digits.insert_str(0, positive_sign);
            }
            Ok(digits)
        }
        'o' | 'x' | 'X' | 'b' | 'B' => {
            let radix = match conv.to_ascii_lowercase() {
                'o' => 8,
                'x' => 16,
                'b' => 2,
                _ => unreachable!(),
            };
            let upper = conv.is_ascii_uppercase();
            let digit_precision = if bigint.sign() == Sign::Minus {
                precision.map(|value| value.saturating_sub(1))
            } else {
                precision
            };
            let digits = apply_precision(format_bigint_radix(&bigint, radix, upper), digit_precision);
            let prefix = if flag_hash && !bigint.is_zero() {
                match conv {
                    'x' => "0x",
                    'X' => "0X",
                    'b' => "0b",
                    'B' => "0B",
                    _ => "0",
                }
            } else {
                ""
            };
            let sign = if bigint.sign() == Sign::Minus {
                "-"
            } else {
                positive_sign
            };
            Ok(format!("{}{}{}", sign, prefix, digits))
        }
        _ => Err(LispError::Signal(format!("Invalid format operation %{}", conv))),
    }
}

pub fn buffer_undo_list_value(buffer: &crate::buffer::Buffer) -> Value {
    let mut entries = buffer
        .undo_entries()
        .iter()
        .rev()
        .map(|entry| match entry {
            crate::buffer::UndoEntry::Insert { pos, len } => {
                Value::cons(Value::Integer(*pos as i64), Value::Integer(*len as i64))
            }
            crate::buffer::UndoEntry::Delete { pos, text, .. } => {
                Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
            }
            crate::buffer::UndoEntry::Boundary => Value::Nil,
        })
        .collect::<Vec<_>>();
    entries.extend(buffer.undo_meta_entries().iter().rev().cloned());
    Value::list(entries)
}

fn values_equal(interp: &Interpreter, left: &Value, right: &Value) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return left_string.text == right_string.text;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| values_equal(interp, left, right));
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b))
        | (Value::BigInteger(b), Value::Integer(a)) => &BigInt::from(*a) == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Buffer(a, _), Value::Buffer(b, _)) => a == b,
        (Value::Marker(a), Value::Marker(b)) => markers_equal(interp, *a, *b),
        (Value::Overlay(a), Value::Overlay(b)) => overlays_equal(interp, *a, *b),
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            values_equal(interp, a_car, b_car) && values_equal(interp, a_cdr, b_cdr)
        }
        _ => false,
    }
}

fn values_equal_including_properties(left: &Value, right: &Value) -> bool {
    if let (Some(left_string), Some(right_string)) = (string_like(left), string_like(right)) {
        return left_string.text == right_string.text && left_string.props == right_string.props;
    }
    if let (Ok(left_items), Ok(right_items)) = (vector_items(left), vector_items(right))
        && matches!(left, Value::Cons(_, _))
        && matches!(right, Value::Cons(_, _))
    {
        return left_items.len() == right_items.len()
            && left_items
                .iter()
                .zip(right_items.iter())
                .all(|(left, right)| values_equal_including_properties(left, right));
    }
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b))
        | (Value::BigInteger(b), Value::Integer(a)) => &BigInt::from(*a) == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            values_equal_including_properties(a_car, b_car)
                && values_equal_including_properties(a_cdr, b_cdr)
        }
        _ => left == right,
    }
}

fn markers_equal(interp: &Interpreter, left_id: u64, right_id: u64) -> bool {
    let Some(left) = interp.find_marker(left_id) else {
        return left_id == right_id;
    };
    let Some(right) = interp.find_marker(right_id) else {
        return false;
    };
    left.buffer_id == right.buffer_id && left.position == right.position
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

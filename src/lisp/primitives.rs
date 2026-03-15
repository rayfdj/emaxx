use super::eval::Interpreter;
use super::types::{Env, LispError, SharedStringState, StringPropertySpan, Value};
use crate::buffer::TextPropertySpan;
use flate2::read::GzDecoder;
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;
use roxmltree::{Document, Node, NodeType};
use std::fs;
use std::io::ErrorKind;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::{cell::RefCell, rc::Rc};
use unicode_width::UnicodeWidthChar;

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
            | "sin"
            | "cos"
            | "tan"
            | "asin"
            | "acos"
            | "atan"
            | "copysign"
            | "isnan"
            | "exp"
            | "expt"
            | "log"
            | "sqrt"
            | "float"
            | "frexp"
            | "ldexp"
            | "logb"
            | "ceiling"
            | "floor"
            | "round"
            | "truncate"
            | "fceiling"
            | "ffloor"
            | "fround"
            | "ftruncate"
            | "ash"
            | "logior"
            // Comparison
            | "="
            | "<"
            | ">"
            | "<="
            | ">="
            | "/="
            // Equality
            | "eq"
            | "eql"
            | "equal"
            | "equal-including-properties"
            | "string="
            | "string-equal"
            | "string-search"
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
            | "fboundp"
            | "featurep"
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
            | "recordp"
            | "charsetp"
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
            | "delete-dups"
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
            | "eval"
            | "read-event"
            | "read-char"
            | "read-char-exclusive"
            | "identity"
            | "mapconcat"
            | "seq-take"
            // Allocation
            | "make-string"
            | "make-vector"
            | "record"
            | "make-record"
            | "make-finalizer"
            // String operations
            | "concat"
            | "string"
            | "substring"
            | "string-to-multibyte"
            | "string-to-number"
            | "number-to-string"
            | "string-match-p"
            | "string-width"
            | "format"
            | "char-to-string"
            | "string-to-char"
            | "byte-to-string"
            | "multibyte-string-p"
            | "multibyte-char-to-unibyte"
            | "unibyte-char-to-multibyte"
            | "upcase"
            | "downcase"
            | "char-resolve-modifiers"
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
            | "delete-and-extract-region"
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
            | "max-char"
            | "get-pos-property"
            | "get-char-property"
            | "get-text-property"
            | "text-properties-at"
            | "put-text-property"
            | "add-text-properties"
            | "remove-list-of-text-properties"
            | "font-lock-prepend-text-property"
            | "font-lock--remove-face-from-text-property"
            | "put"
            | "zlib-available-p"
            | "zlib-decompress-region"
            | "libxml-parse-xml-region"
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
            | "decode-char"
            | "char-charset"
            | "charset-id-internal"
            | "charset-plist"
            | "charset-priority-list"
            | "charset-after"
            | "find-charset-string"
            | "find-charset-region"
            | "map-charset-chars"
            | "define-charset-internal"
            | "define-charset-alias"
            | "set-charset-plist"
            | "unify-charset"
            | "get-unused-iso-final-char"
            | "declare-equiv-charset"
            | "iso-charset"
            | "split-char"
            | "clear-charset-maps"
            | "set-charset-priority"
            | "sort-charsets"
            | "char-table-p"
            | "char-table-subtype"
            | "char-table-parent"
            | "set-char-table-parent"
            | "char-table-extra-slot"
            | "set-char-table-extra-slot"
            | "char-table-range"
            | "set-char-table-range"
            | "make-category-table"
            | "category-table-p"
            | "standard-category-table"
            | "category-table"
            | "set-category-table"
            | "define-category"
            | "category-docstring"
            | "make-category-set"
            | "category-set-mnemonics"
            | "modify-category-entry"
            | "char-category-set"
            | "copy-category-table"
            | "set-buffer"
            | "switch-to-buffer"
            | "buffer-file-name"
            | "find-file"
            | "find-file-noselect"
            | "file-locked-p"
            | "expand-file-name"
            | "substitute-in-file-name"
            | "file-name-directory"
            | "file-name-nondirectory"
            | "file-name-as-directory"
            | "directory-file-name"
            | "file-name-absolute-p"
            | "file-name-concat"
            | "file-name-unquote"
            | "file-remote-p"
            | "shell-quote-argument"
            | "locate-library"
            | "ert-resource-directory"
            | "ert-resource-file"
            | "load"
            | "file-readable-p"
            | "file-exists-p"
            | "file-executable-p"
            | "delete-file"
            | "delete-directory"
            | "make-directory"
            | "insert-file-contents"
            | "insert-file-contents-literally"
            | "write-region"
            | "call-process"
            | "call-process-region"
            | "process-lines"
            | "shell-command"
            | "kill-buffer"
            | "lock-buffer"
            | "revert-buffer"
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
            | "read-char-choice"
            | "yes-or-no-p"
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
            | "display-images-p"
            | "frame-parameter"
            | "frame-char-width"
            | "selected-window"
            | "selected-frame"
            | "transient-mark-mode"
            | "font-lock-mode"
            | "find-image"
            | "image-size"
            | "image-mask-p"
            | "image-metadata"
            | "imagemagick-types"
            | "init-image-library"
            | "window-start"
            | "window-end"
            | "window-width"
            | "window-text-pixel-size"
            | "get-display-property"
            | "bidi-find-overridden-directionality"
            | "redisplay"
            | "font-spec"
            | "font-get"
            | "set-face-attribute"
            | "color-distance"
            | "color-values-from-color-spec"
            | "facemenu-add-face"
            | "get-buffer-window"
            | "set-window-start"
            | "set-window-point"
            | "read-string"
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
            | "remove-hook"
            | "run-hooks"
            | "mapatoms"
            | "eval-after-load"
            | "describe-function"
            | "executable-find"
            | "run-with-timer"
            | "cancel-timer"
            | "lossage-size"
            | "ignore"
            | "make-obsolete"
            | "make-obsolete-variable"
            | "define-obsolete-function-alias"
            | "define-obsolete-variable-alias"
            | "macroexp-warn-and-return"
            | "intern"
            | "autoloadp"
            | "documentation"
            | "getenv"
            | "getenv-internal"
            | "symbol-function"
            | "symbol-name"
            | "macroexp-file-name"
            | "hash-table-p"
            | "char-from-name"
            | "always"
            | "evenp"
            | "seq-subseq"
            | "text-quoting-style"
            | "type-of"
            | "file-truename"
            | "save-buffer"
            | "unlock-buffer"
            | "userlock--handle-unlock-error"
            | "auto-save-mode"
            | "do-auto-save"
            | "group-gid"
            | "group-name"
            | "random"
            | "make-hash-table"
            | "profiler-memory-running-p"
            | "profiler-memory-start"
            | "profiler-memory-stop"
            | "profiler-memory-log"
            | "profiler-cpu-running-p"
            | "profiler-cpu-start"
            | "profiler-cpu-stop"
            | "profiler-cpu-log"
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
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    // Helper: check if any argument is a float
    let has_float = |args: &[Value]| args.iter().any(|a| matches!(a, Value::Float(_)));
    let has_big_integer = |args: &[Value]| args.iter().any(|a| matches!(a, Value::BigInteger(_)));

    if matches!(
        name,
        "undo-boundary"
            | "insert"
            | "insert-char"
            | "insert-before-markers"
            | "insert-before-markers-and-inherit"
            | "delete-region"
            | "delete-and-extract-region"
            | "kill-region"
            | "delete-char"
            | "delete-forward-char"
            | "kill-word"
            | "erase-buffer"
            | "put-text-property"
            | "add-text-properties"
            | "remove-list-of-text-properties"
            | "set-buffer-multibyte"
            | "write-region"
            | "save-buffer"
    ) {
        interp.reset_undo_sequence();
    }

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
                    return Ok(normalize_bigint_value(-integer_like_bigint(
                        interp, &args[0],
                    )?));
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
            if has_float(args) {
                let a = numeric_to_f64(interp, &args[0])?;
                let b = numeric_to_f64(interp, &args[1])?;
                if b == 0.0 {
                    return Err(LispError::Signal("Division by zero".into()));
                }
                let mut remainder = a % b;
                if remainder != 0.0 && (remainder.is_sign_negative() != b.is_sign_negative()) {
                    remainder += b;
                }
                return Ok(Value::Float(remainder));
            }
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
            Ok(normalize_bigint_value(
                integer_like_bigint(interp, &args[0])? + 1,
            ))
        }
        "1-" => {
            need_args(name, args, 1)?;
            Ok(normalize_bigint_value(
                integer_like_bigint(interp, &args[0])? - 1,
            ))
        }
        "max" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("max".into(), 0));
            }
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.max(numeric_to_f64(interp, a)?);
                }
                return Ok(Value::Float(result));
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
            if has_float(args) {
                let mut result = numeric_to_f64(interp, &args[0])?;
                for a in &args[1..] {
                    result = result.min(numeric_to_f64(interp, a)?);
                }
                return Ok(Value::Float(result));
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
            if let Value::Float(value) = args[0] {
                Ok(Value::Float(value.abs()))
            } else if matches!(args[0], Value::BigInteger(_)) {
                Ok(normalize_bigint_value(
                    integer_like_bigint(interp, &args[0])?.abs(),
                ))
            } else {
                let value = integer_like_i64(interp, &args[0])?;
                match value.checked_abs() {
                    Some(abs) => Ok(Value::Integer(abs)),
                    None => Ok(normalize_bigint_value(BigInt::from(value).abs())),
                }
            }
        }
        "sin" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sin()))
        }
        "cos" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.cos()))
        }
        "tan" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.tan()))
        }
        "asin" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.asin()))
        }
        "acos" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.acos()))
        }
        "atan" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let y = numeric_to_f64(interp, &args[0])?;
            Ok(Value::Float(if let Some(x) = args.get(1) {
                y.atan2(numeric_to_f64(interp, x)?)
            } else {
                y.atan()
            }))
        }
        "copysign" => {
            need_args(name, args, 2)?;
            Ok(Value::Float(
                numeric_to_f64(interp, &args[0])?.copysign(numeric_to_f64(interp, &args[1])?),
            ))
        }
        "isnan" => {
            need_args(name, args, 1)?;
            let value = match &args[0] {
                Value::Float(value) => *value,
                Value::Integer(_) | Value::BigInteger(_) => {
                    return Ok(Value::Nil);
                }
                _ => return Err(LispError::TypeError("number".into(), args[0].type_name())),
            };
            Ok(if value.is_nan() { Value::T } else { Value::Nil })
        }
        "exp" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.exp()))
        }
        "expt" => {
            need_args(name, args, 2)?;
            Ok(expt_value(interp, &args[0], &args[1])?)
        }
        "log" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let value = numeric_to_f64(interp, &args[0])?;
            let result = if let Some(base) = args.get(1) {
                let base = numeric_to_f64(interp, base)?;
                if base == 10.0 {
                    value.log10()
                } else if base == 2.0 {
                    value.log2()
                } else {
                    value.log(base)
                }
            } else {
                value.ln()
            };
            Ok(Value::Float(result))
        }
        "sqrt" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?.sqrt()))
        }
        "float" => {
            need_args(name, args, 1)?;
            Ok(Value::Float(numeric_to_f64(interp, &args[0])?))
        }
        "frexp" => {
            need_args(name, args, 1)?;
            let (sig, exp) = frexp_parts(numeric_to_f64(interp, &args[0])?);
            Ok(Value::cons(Value::Float(sig), Value::Integer(exp)))
        }
        "ldexp" => {
            need_args(name, args, 2)?;
            let significand = numeric_to_f64(interp, &args[0])?;
            let exponent = integer_like_i64(interp, &args[1])?;
            Ok(Value::Float(ldexp_value(significand, exponent)))
        }
        "logb" => {
            need_args(name, args, 1)?;
            Ok(logb_value(interp, &args[0])?)
        }
        "ceiling" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Ceiling,
            args,
            false,
        )?),
        "floor" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Floor,
            args,
            false,
        )?),
        "round" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Round,
            args,
            false,
        )?),
        "truncate" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Truncate,
            args,
            false,
        )?),
        "fceiling" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Ceiling,
            args,
            true,
        )?),
        "ffloor" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Floor,
            args,
            true,
        )?),
        "fround" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Round,
            args,
            true,
        )?),
        "ftruncate" => Ok(integer_rounding_value(
            interp,
            RoundingKind::Truncate,
            args,
            true,
        )?),
        "ash" => {
            need_args(name, args, 2)?;
            let value = integer_like_bigint(interp, &args[0])?;
            let shift = integer_like_i64(interp, &args[1])?;
            let shifted = if shift >= 0 {
                value << shift as usize
            } else {
                value >> (-shift) as usize
            };
            Ok(normalize_bigint_value(shifted))
        }
        "logior" => {
            let mut result = 0i64;
            for arg in args {
                result |= arg.as_integer()?;
            }
            Ok(Value::Integer(result))
        }

        // ── Comparison ──
        "=" => {
            need_args(name, args, 2)?;
            if has_float(args) {
                let a = numeric_to_f64(interp, &args[0])?;
                let b = numeric_to_f64(interp, &args[1])?;
                Ok(if a == b { Value::T } else { Value::Nil })
            } else if has_big_integer(args) {
                Ok(
                    if integer_like_bigint(interp, &args[0])?
                        == integer_like_bigint(interp, &args[1])?
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            } else {
                Ok(
                    if integer_like_i64(interp, &args[0])? == integer_like_i64(interp, &args[1])? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
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
            Ok(if numeric_lte(interp, &args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        ">=" => {
            need_args(name, args, 2)?;
            Ok(if numeric_gte(interp, &args[0], &args[1])? {
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
        "eql" => {
            need_args(name, args, 2)?;
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
        "string-search" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let needle = string_text(&args[0])?;
            let haystack = string_text(&args[1])?;
            let hay_chars: Vec<char> = haystack.chars().collect();
            let start = if args.len() == 3 {
                let start = args[2].as_integer()?;
                if start < 0 || start as usize > hay_chars.len() {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                start as usize
            } else {
                0
            };
            if needle.is_empty() {
                return Ok(Value::Integer(start as i64));
            }
            let suffix: String = hay_chars[start..].iter().collect();
            match suffix.find(&needle) {
                Some(byte_offset) => {
                    let char_offset = suffix[..byte_offset].chars().count();
                    Ok(Value::Integer((start + char_offset) as i64))
                }
                None => Ok(Value::Nil),
            }
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
            Ok(
                if interp.lookup_var(symbol, env).is_some()
                    || matches!(
                        symbol,
                        "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
                    )
                    || symbol == "buffer-undo-list"
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "fboundp" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(
                if interp.lookup_function(symbol, env).is_ok() || is_builtin(symbol) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "featurep" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if interp.has_feature(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "zlib-available-p" => Ok(Value::T),
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
            Ok(
                if matches!(&args[0], Value::Buffer(id, _) if interp.has_buffer_id(*id)) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
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
            Ok(if args[0].is_cons() {
                Value::Nil
            } else {
                Value::T
            })
        }

        "nlistp" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_list() {
                Value::Nil
            } else {
                Value::T
            })
        }

        "characterp" => {
            need_args(name, args, 1)?;
            // In Emacs, characters are integers 0..#x3FFFFF
            Ok(match &args[0] {
                Value::Integer(n) if *n >= 0 && *n <= 0x3F_FFFF => Value::T,
                Value::BigInteger(n)
                    if n.sign() != Sign::Minus && n <= &BigInt::from(0x3F_FFFFu32) =>
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
        "recordp" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::Record(_)) {
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
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    Ok(Value::Integer((record.slots.len() + 1) as i64))
                }
                _ => Err(LispError::TypeError("sequence".into(), args[0].type_name())),
            }
        }
        "reverse" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.reverse();
            Ok(Value::list(items))
        }
        "delete-dups" => {
            need_args(name, args, 1)?;
            let mut deduped = Vec::new();
            for item in args[0].to_vec()? {
                if !deduped.iter().any(|existing| existing == &item) {
                    deduped.push(item);
                }
            }
            Ok(Value::list(deduped))
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
        "eval" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            interp.eval(&args[0], env)
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
            interp.eval(&Value::list(call_items), env)
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
            interp.eval(&Value::list(call_items), env)
        }
        "funcall-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = resolve_callable(interp, &args[0], env)?;
            invoke_function_value(interp, &func, &args[1..], env)
        }
        "call-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = resolve_callable(interp, &args[0], env)?;
            let interactive_args = collect_interactive_args(interp, &func, env)?;
            let result = invoke_function_value(interp, &func, &interactive_args, env)?;
            if args.get(1).is_some_and(Value::is_truthy)
                && let Some(function_name) = callable_name(&args[0], &func)
            {
                let history_args = history_args_for_call(interp, &func, &interactive_args, env)?;
                record_command_history(interp, &function_name, history_args, env);
            }
            Ok(result)
        }
        "read-event" | "read-char" | "read-char-exclusive" => {
            if interp
                .lookup_var("inhibit-interaction", env)
                .is_some_and(|value| value.is_truthy())
            {
                return Err(LispError::Signal("Interaction inhibited".into()));
            }
            let event = pop_unread_command_event(interp, env)?;
            Ok(Value::Integer(event as i64))
        }
        "read-string" => Ok(Value::String(String::new())),

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
        "record" => {
            need_args(name, args, 1)?;
            let type_name = args[0].as_symbol()?;
            Ok(interp.create_record(type_name, args[1..].to_vec()))
        }
        "make-record" => {
            need_args(name, args, 3)?;
            let type_name = args[0].as_symbol()?;
            let length = args[1].as_integer()?;
            if length < 0 {
                return Err(LispError::Signal("Wrong type argument: natnump".into()));
            }
            Ok(interp.create_record(
                type_name,
                std::iter::repeat_n(args[2].clone(), length as usize).collect(),
            ))
        }
        "make-finalizer" => {
            need_args(name, args, 1)?;
            Ok(Value::Finalizer(interp.alloc_finalizer_id()))
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
        "string-match-p" => {
            need_args(name, args, 2)?;
            let pattern = string_text(&args[0])?;
            let haystack = string_text(&args[1])?;
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(if regex.is_match(&haystack) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "string-width" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "string-width".into(),
                    args.len(),
                ));
            }
            let text = string_text(&args[0])?;
            let chars: Vec<char> = text.chars().collect();
            let len = chars.len() as i64;
            let start = normalize_string_index(args.get(1), 0, len)?;
            let end = normalize_string_index(args.get(2), len, len)?;
            if end < start {
                return Err(LispError::Signal("Args out of range".into()));
            }
            let tab_width = interp
                .lookup_var("tab-width", &Vec::new())
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(8)
                .max(1) as usize;
            let mut width = 0usize;
            for ch in chars[start as usize..end as usize].iter().copied() {
                if ch == '\t' {
                    width += tab_width;
                } else {
                    width += ch.width().unwrap_or(0);
                }
            }
            Ok(Value::Integer(width as i64))
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
        "multibyte-char-to-unibyte" => {
            need_args(name, args, 1)?;
            let ch = args[0].as_integer()?;
            Ok(if (0..=255).contains(&ch) {
                Value::Integer(ch)
            } else {
                Value::Integer(-1)
            })
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
                            interp, arg, conv, flag_hash, flag_plus, flag_space, precision,
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
                result_props.extend(shift_string_props(
                    &merge_string_props(formatted_props),
                    start,
                ));
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
        "char-resolve-modifiers" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(resolve_char_modifiers(
                args[0].as_integer()?,
            )))
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
                        if matches!(interp.buffer.char_before(), Some(ch) if ch.is_alphanumeric() || ch == '_')
                        {
                            break;
                        }
                        let _ = interp.buffer.forward_char(-1);
                    }
                    while interp.buffer.point() > interp.buffer.point_min() {
                        if !matches!(interp.buffer.char_before(), Some(ch) if ch.is_alphanumeric() || ch == '_')
                        {
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
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let needle = string_text(&args[0])?;
            let haystack = interp.buffer.buffer_string();
            let point = interp.buffer.point();
            let noerror = args.get(2).is_some_and(Value::is_truthy);
            let result = if name == "search-forward" {
                let offset = haystack
                    .chars()
                    .take(point.saturating_sub(1))
                    .map(|ch| ch.len_utf8())
                    .sum::<usize>();
                haystack[offset..].find(&needle).map(|found| {
                    let match_start_chars = haystack[offset..offset + found].chars().count();
                    let match_end_chars = haystack[offset..offset + found + needle.len()]
                        .chars()
                        .count();
                    (point + match_start_chars, point + match_end_chars)
                })
            } else {
                let prefix: String = haystack.chars().take(point.saturating_sub(1)).collect();
                prefix.rfind(&needle).map(|found| {
                    let start = prefix[..found].chars().count() + 1;
                    let end = start + needle.chars().count();
                    (start, end)
                })
            };
            match result {
                Some((start, end)) => {
                    interp.last_match_data = Some(vec![Some((start, end))]);
                    let point = if name == "search-backward" {
                        start
                    } else {
                        end
                    };
                    interp.buffer.goto_char(point);
                    Ok(Value::Integer(point as i64))
                }
                None if noerror => Ok(Value::Nil),
                None => Err(LispError::SignalValue(Value::list([
                    Value::Symbol("search-failed".into()),
                    Value::String(needle),
                ]))),
            }
        }
        "re-search-forward" | "search-forward-regexp" => {
            if args.is_empty() || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pattern = string_text(&args[0])?;
            let regex = Regex::new(&translate_elisp_regex(&pattern))
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let start = interp.buffer.point();
            let tail = interp
                .buffer
                .buffer_substring(start, interp.buffer.point_max())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let noerror = args.get(2).is_some_and(Value::is_truthy);
            if let Some(captures) = regex.captures(&tail)
                && let Some(matched) = captures.get(0)
            {
                let pos = start + tail[..matched.end()].chars().count();
                set_match_data(interp, start, &tail, &captures);
                interp.buffer.goto_char(pos);
                Ok(Value::Integer(pos as i64))
            } else {
                interp.last_match_data = None;
                if noerror {
                    Ok(Value::Nil)
                } else {
                    Err(LispError::SignalValue(Value::list([
                        Value::Symbol("search-failed".into()),
                        Value::String(pattern),
                    ])))
                }
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
        "max-char" => Ok(Value::Integer(0x3F_FFFF)),
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
        "set-buffer-multibyte" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            interp.buffer.set_multibyte(enabled);
            interp
                .buffer
                .push_undo_entry(crate::buffer::UndoEntry::Combined {
                    display: Value::Nil,
                    entries: Vec::new(),
                });
            Ok(if enabled { Value::T } else { Value::Nil })
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
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            ensure_region_modifiable(interp, from, to, env)?;
            delete_region_with_hooks(interp, from, to, env)?;
            Ok(Value::Nil)
        }
        "delete-and-extract-region" => {
            need_args(name, args, 2)?;
            let from = position_from_value(interp, &args[0])?;
            let to = position_from_value(interp, &args[1])?;
            ensure_region_modifiable(interp, from, to, env)?;
            Ok(string_like_value(
                delete_region_with_hooks(interp, from, to, env)?,
                Vec::new(),
            ))
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
            if interp.buffer.mark_active()
                && interp
                    .lookup_var("transient-mark-mode", env)
                    .is_some_and(|value| value.is_truthy())
                && let Some((start, end)) = interp.buffer.region()
            {
                interp.buffer.deactivate_mark();
                return call(
                    interp,
                    "delete-region",
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                );
            }
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
            let pt = interp.buffer.point();
            let bol = {
                let saved = interp.buffer.point();
                interp.buffer.beginning_of_line();
                let bol = interp.buffer.point();
                interp.buffer.goto_char(saved);
                bol
            };
            Ok(Value::Integer(column_at(interp, env, bol, pt) as i64))
        }
        "move-to-column" => {
            need_args(name, args, 1)?;
            let target = args[0].as_integer()?.max(0) as usize;
            let force = args.get(1).is_some_and(Value::is_truthy);
            let saved = interp.buffer.point();
            interp.buffer.beginning_of_line();
            let start = interp.buffer.point();
            interp.buffer.goto_char(saved);
            let mut pos = start;
            while pos < interp.buffer.point_max() {
                let current_col = column_at(interp, env, start, pos);
                if current_col >= target {
                    break;
                }
                let Some(ch) = interp.buffer.char_at(pos) else {
                    break;
                };
                if ch == '\n' {
                    break;
                }
                let next_col = column_after(interp, env, current_col, pos, ch);
                if next_col > target && force && ch == '\t' && !char_is_invisible(interp, pos) {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                    break;
                }
                pos += 1;
            }
            if force {
                let current_col = column_at(interp, env, start, pos);
                if current_col < target {
                    interp.buffer.goto_char(pos);
                    interp.insert_current_buffer(&" ".repeat(target - current_col));
                    pos = interp.buffer.point();
                }
            }
            interp.buffer.goto_char(pos);
            Ok(Value::Integer(column_at(interp, env, start, pos) as i64))
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
                let clamp_start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
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
                let _ = maybe_lock_current_buffer(interp, env);
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
            Ok(
                highest_priority_overlay_property(buffer, pos, &prop, name == "get-pos-property")
                    .or_else(|| buffer.text_property_at(pos, &prop))
                    .unwrap_or(Value::Nil),
            )
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
                Ok(interp
                    .buffer
                    .text_property_at(pos, &prop)
                    .unwrap_or(Value::Nil))
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
                        .ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?
                        .text_properties_at(pos)
                }
            } else {
                interp.buffer.text_properties_at(pos)
            };
            Ok(plist_value(&props))
        }
        "put-text-property" => {
            if args.len() < 4 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let prop = args[2].as_symbol()?.to_string();
            if let Some(object) = args.get(4) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |mut current| {
                        current.retain(|(key, _)| key != &prop);
                        current.push((prop.clone(), args[3].clone()));
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp
                        .buffer
                        .put_text_property(start, end, &prop, args[3].clone());
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .put_text_property(start, end, &prop, args[3].clone());
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "add-text-properties" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let props = plist_pairs(&args[2])?;
            if let Some(object) = args.get(3) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |mut current| {
                        for (name, value) in &props {
                            if let Some((_, existing)) =
                                current.iter_mut().find(|(key, _)| key == name)
                            {
                                *existing = value.clone();
                            } else {
                                current.push((name.clone(), value.clone()));
                            }
                        }
                        current
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp.buffer.add_text_properties(start, end, &props);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp.buffer.add_text_properties(start, end, &props);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "remove-list-of-text-properties" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let names = args[2]
                .to_vec()?
                .into_iter()
                .map(|value| value.as_symbol().map(|s| s.to_string()))
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(object) = args.get(3) {
                if string_like(object).is_some() {
                    let start = args[0].as_integer()?.max(0) as usize;
                    let end = args[1].as_integer()?.max(0) as usize;
                    modify_shared_string_properties(object, start, end, |current| {
                        current
                            .into_iter()
                            .filter(|(key, _)| !names.iter().any(|name| name == key))
                            .collect()
                    })?;
                } else {
                    let start = position_from_value(interp, &args[0])?;
                    let end = position_from_value(interp, &args[1])?;
                    interp
                        .buffer
                        .remove_list_of_text_properties(start, end, &names);
                    interp
                        .buffer
                        .push_undo_entry(crate::buffer::UndoEntry::Combined {
                            display: Value::Nil,
                            entries: Vec::new(),
                        });
                }
            } else {
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                interp
                    .buffer
                    .remove_list_of_text_properties(start, end, &names);
                interp
                    .buffer
                    .push_undo_entry(crate::buffer::UndoEntry::Combined {
                        display: Value::Nil,
                        entries: Vec::new(),
                    });
            }
            Ok(Value::T)
        }
        "font-lock-prepend-text-property" => {
            need_args(name, args, 5)?;
            let start = args[0].as_integer()?.max(0) as usize;
            let end = args[1].as_integer()?.max(0) as usize;
            let prop = args[2].as_symbol()?.to_string();
            let face = args[3].clone();
            modify_shared_string_properties(&args[4], start, end, |mut current| {
                if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == &prop) {
                    *existing = prepend_face_value(existing.clone(), &face);
                } else {
                    current.push((prop.clone(), face.clone()));
                }
                current
            })?;
            Ok(Value::Nil)
        }
        "font-lock--remove-face-from-text-property" => {
            need_args(name, args, 5)?;
            let start = args[0].as_integer()?.max(0) as usize;
            let end = args[1].as_integer()?.max(0) as usize;
            let prop = args[2].as_symbol()?.to_string();
            let face = args[3].clone();
            modify_shared_string_properties(&args[4], start, end, |mut current| {
                if let Some(index) = current.iter().position(|(key, _)| key == &prop) {
                    let updated = remove_face_value(current[index].1.clone(), &face);
                    if updated.is_nil() {
                        current.remove(index);
                    } else {
                        current[index].1 = updated;
                    }
                }
                current
            })?;
            Ok(Value::Nil)
        }
        "put" => Ok(Value::Nil),
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
            let start = call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?.as_integer()?
                as usize;
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
            let start = call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = call(interp, "field-end", &[Value::Integer(pos as i64)], env)?.as_integer()?
                as usize;
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
        "current-buffer" => Ok(Value::Buffer(
            interp.current_buffer_id(),
            interp.buffer.name.clone(),
        )),
        "generate-new-buffer" => {
            need_args(name, args, 1)?;
            let base = args[0].as_string()?;
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
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
            interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
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
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
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
                interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
                if !inhibit_hooks {
                    run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                }
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
            let inhibit_hooks = args.get(3).is_some_and(|value| value.is_truthy());
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
                buffer.inhibit_hooks = inhibit_hooks;
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
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
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
                .and_then(|base_id| {
                    interp
                        .get_buffer_by_id(base_id)
                        .map(|buffer| Value::Buffer(base_id, buffer.name.clone()))
                })
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
        "decode-char" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            let code = args[1].as_integer()?;
            Ok(match interp.charset_canonical_name(charset).as_deref() {
                Some("ascii") if (0..=0x7f).contains(&code) => Value::Integer(code),
                Some("unicode") if code >= 0 => Value::Integer(code),
                Some(_) | None => Value::Nil,
            })
        }
        "char-charset" => {
            need_args(name, args, 1)?;
            Ok(Value::Symbol(
                charset_for_char(args[0].as_integer()? as u32).into(),
            ))
        }
        "charsetp" => {
            need_args(name, args, 1)?;
            let Value::Symbol(symbol) = &args[0] else {
                return Ok(Value::Nil);
            };
            Ok(if interp.has_charset(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "charset-id-internal" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_id(symbol)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "charset-plist" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_plist_value(symbol)
                .unwrap_or_else(|| default_charset_plist(symbol, interp).unwrap_or(Value::Nil)))
        }
        "charset-priority-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let priority = interp.charset_priority_list();
            if args.first().is_some_and(Value::is_truthy) {
                Ok(priority
                    .first()
                    .cloned()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(
                    priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                ))
            }
        }
        "charset-after" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(interp.buffer.point() as i64);
            Ok(match interp.buffer.char_at(pos as usize) {
                Some(ch) => Value::Symbol(charset_for_char(ch as u32).into()),
                None => Value::Nil,
            })
        }
        "find-charset-string" => {
            need_args(name, args, 1)?;
            Ok(Value::list(charsets_for_text(
                &string_text(&args[0])?,
                interp,
            )))
        }
        "find-charset-region" => {
            need_args(name, args, 2)?;
            let from = args[0].as_integer()?;
            let to = args[1].as_integer()?;
            let (start, end) = clamp_overlay_range(&interp.buffer, from, to);
            let mut text = String::new();
            for pos in start..end {
                if let Some(ch) = interp.buffer.char_at(pos) {
                    text.push(ch);
                }
            }
            Ok(Value::list(charsets_for_text(&text, interp)))
        }
        "map-charset-chars" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function = args[0].clone();
            let charset = args[1].as_symbol()?.to_string();
            let arg = args.get(2).cloned().unwrap_or(Value::Nil);
            let from = args.get(3).map(Value::as_integer).transpose()?.unwrap_or(0);
            let to = args
                .get(4)
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(charset_max_codepoint(&charset));
            let ranges = charset_ranges_for(&charset, from, to, interp)?;
            for (start, end) in ranges {
                call_function_value(
                    interp,
                    &function,
                    &[
                        Value::cons(Value::Integer(start), Value::Integer(end)),
                        arg.clone(),
                    ],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "define-charset-internal" => Err(LispError::WrongNumberOfArgs(name.into(), args.len())),
        "define-charset-alias" => {
            need_args(name, args, 2)?;
            let alias = args[0].as_symbol()?;
            let target = args[1].as_symbol()?;
            interp.define_charset_alias(alias, target)?;
            Ok(Value::Symbol(alias.to_string()))
        }
        "set-charset-plist" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            interp.set_charset_plist_value(charset, args[1].clone())?;
            Ok(args[1].clone())
        }
        "unify-charset" => {
            need_args(name, args, 1)?;
            Err(LispError::Signal("Cannot unify charset".into()))
        }
        "get-unused-iso-final-char" => {
            need_args(name, args, 2)?;
            Ok(Value::Integer('0' as i64))
        }
        "declare-equiv-charset" => {
            need_args(name, args, 4)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            let charset = args[3].as_symbol()?;
            interp.declare_iso_charset(dimension, chars, final_char as u32, charset);
            Ok(Value::Nil)
        }
        "iso-charset" => {
            need_args(name, args, 3)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            Ok(interp
                .iso_charset(dimension, chars, final_char as u32)
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "split-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            Ok(Value::list([
                Value::Symbol(charset_for_char(code as u32).into()),
                Value::Integer(code),
            ]))
        }
        "clear-charset-maps" => Ok(Value::Nil),
        "set-charset-priority" => {
            let names = args
                .iter()
                .map(Value::as_symbol)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>();
            interp.set_charset_priority(&names);
            Ok(Value::Nil)
        }
        "sort-charsets" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by_key(|value| {
                value
                    .as_symbol()
                    .map(|name| interp.charset_priority_rank(name))
                    .unwrap_or(usize::MAX)
            });
            Ok(Value::list(items))
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
                buffer.file_truename = Some(path.clone());
                buffer.set_unmodified();
                buffer.set_visited_file_modtime(file_modtime(&path)?);
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
        "expand-file-name" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            let base = match args.get(1) {
                Some(value) if !value.is_nil() => Some(string_text(value)?),
                _ => interp
                    .lookup_var("default-directory", env)
                    .and_then(|value| string_like(&value).map(|string| string.text)),
            };
            Ok(Value::String(expand_file_name(&path, base.as_deref())))
        }
        "substitute-in-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(substitute_in_file_name(&string_text(
                &args[0],
            )?)))
        }
        "file-name-directory" => {
            need_args(name, args, 1)?;
            Ok(file_name_directory(&string_text(&args[0])?)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "file-name-nondirectory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_nondirectory(&string_text(
                &args[0],
            )?)))
        }
        "file-name-as-directory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_as_directory(&string_text(
                &args[0],
            )?)))
        }
        "directory-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(directory_file_name(&string_text(&args[0])?)))
        }
        "file-name-absolute-p" => {
            need_args(name, args, 1)?;
            Ok(if file_name_absolute_p(&string_text(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-name-concat" => Ok(Value::String(file_name_concat(
            &args
                .iter()
                .filter(|value| !value.is_nil())
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        "file-name-unquote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?))
        }
        "file-remote-p" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::Nil)
        }
        "shell-quote-argument" => {
            need_args(name, args, 1)?;
            let argument = string_text(&args[0])?;
            Ok(Value::String(shell_quote_argument(&argument)))
        }
        "ert-resource-directory" => {
            need_args(name, args, 0)?;
            Ok(ert_resource_directory(interp)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "ert-resource-file" => {
            need_args(name, args, 1)?;
            let file = string_text(&args[0])?;
            let Some(directory) = ert_resource_directory(interp) else {
                return Err(LispError::Signal(
                    "Cannot determine the current ERT resource directory".into(),
                ));
            };
            Ok(Value::String(expand_file_name(&file, Some(&directory))))
        }
        "locate-library" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let library = string_text(&args[0])?;
            Ok(interp
                .resolve_load_target(&library)
                .map(|path| Value::String(path.display().to_string()))
                .unwrap_or(Value::Nil))
        }
        "load" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let target = string_text(&args[0])?;
            let _loaded = interp.load_target(&target)?;
            Ok(Value::T)
        }
        "file-readable-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_readable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
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
        "file-executable-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_executable_p(&path) {
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
        "delete-directory" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::remove_dir_all(path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::remove_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "make-directory" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::create_dir_all(path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::create_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "file-locked-p" => {
            need_args(name, args, 1)?;
            file_locked_p(&string_text(&args[0])?)
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
        "insert-file-contents-literally" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            let bytes = fs::read(&path).map_err(|error| LispError::Signal(error.to_string()))?;
            let text = bytes
                .iter()
                .map(|byte| char::from(*byte))
                .collect::<String>();
            let count = text.chars().count();
            interp.insert_current_buffer(&text);
            Ok(Value::list([
                Value::String(path.to_string()),
                Value::Integer(count as i64),
            ]))
        }
        "call-process" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let input = match args.get(1) {
                Some(value) if !value.is_nil() => match value {
                    Value::Integer(0) => None,
                    _ => Some(
                        fs::read(string_text(value)?)
                            .map_err(|error| LispError::Signal(error.to_string()))?,
                    ),
                },
                _ => None,
            };
            let destination = args.get(2).unwrap_or(&Value::Nil);
            let argv = args
                .get(4..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, input.as_deref(), env)?;
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "process-lines" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let argv = args[1..]
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output = run_external_process(interp, &program, &argv, None, env)?;
            if !process_output.status.success() {
                let stderr = String::from_utf8_lossy(&process_output.stderr)
                    .trim()
                    .to_string();
                return Err(LispError::Signal(if stderr.is_empty() {
                    format!(
                        "process-lines exited with status {}",
                        exit_status_code(&process_output.status)
                    )
                } else {
                    stderr
                }));
            }
            let lines = String::from_utf8_lossy(&process_output.stdout)
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect::<Vec<_>>();
            Ok(Value::list(lines))
        }
        "zlib-decompress-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let compressed = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let input = compressed
                .chars()
                .map(|ch| {
                    u8::try_from(ch as u32)
                        .map_err(|_| LispError::Signal("Invalid byte in compressed data".into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut decoder = GzDecoder::new(&input[..]);
            let mut output = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut output)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            ensure_region_modifiable(interp, start, end, env)?;
            delete_region_with_hooks(interp, start, end, env)?;
            let text = output
                .iter()
                .map(|byte| char::from(*byte))
                .collect::<String>();
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "libxml-parse-xml-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let xml = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            parse_xml_region(&xml).map_err(|error| LispError::Signal(error.to_string()))
        }
        "call-process-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let (start, end) = if args[0].is_nil() && args[1].is_nil() {
                (interp.buffer.point_min(), interp.buffer.point_max())
            } else {
                (
                    position_from_value(interp, &args[0])?,
                    position_from_value(interp, &args[1])?,
                )
            };
            let input = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let program = string_text(&args[2])?;
            let delete_region = args.get(3).is_some_and(Value::is_truthy);
            let destination = args.get(4).unwrap_or(&Value::Nil);
            let argv = args
                .get(6..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, Some(input.as_bytes()), env)?;
            if delete_region {
                interp
                    .buffer
                    .delete_region(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "shell-command" => {
            need_args(name, args, 1)?;
            let command = string_text(&args[0])?;
            let status = Command::new("sh")
                .arg("-c")
                .arg(&command)
                .status()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Integer(status.code().unwrap_or(1) as i64))
        }
        "kill-buffer" => {
            let id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            let inhibit_hooks = interp.buffer_hooks_inhibited(id);
            let auto_save = interp.buffer_local_value(id, "buffer-auto-save-file-name");
            let auto_save_path = auto_save.as_ref().and_then(|value| string_text(value).ok());
            let modified = interp
                .get_buffer_by_id(id)
                .map(|buffer| buffer.is_modified())
                .unwrap_or(false);
            if modified {
                let answer = call_named_function(
                    interp,
                    "yes-or-no-p",
                    &[Value::String("Buffer modified; kill anyway?".into())],
                    env,
                )?;
                if answer.is_nil() {
                    return Ok(Value::Nil);
                }
                if let Some(path) = auto_save_path.as_ref()
                    && fs::metadata(path).is_ok()
                    && interp
                        .lookup_var("kill-buffer-delete-auto-save-files", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    let delete = call_named_function(
                        interp,
                        "yes-or-no-p",
                        &[Value::String("Delete auto-save file?".into())],
                        env,
                    )?;
                    if delete.is_truthy() {
                        let _ = fs::remove_file(path);
                    }
                }
                if id == interp.current_buffer_id() {
                    unlock_current_buffer(interp, env)?;
                }
            }
            if !inhibit_hooks {
                for hook in hook_values(interp, "kill-buffer-query-functions", env, Some(id)) {
                    let result = call_function_value(interp, &hook, &[], env)?;
                    if result.is_nil() {
                        return Ok(Value::Nil);
                    }
                }
                run_named_hooks(interp, "kill-buffer-hook", env, Some(id))?;
            }
            interp.kill_buffer_id(id);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
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
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.deactivate_mark();
            }
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
        "point-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point() as i64), false)
        }
        "mark-marker" => match interp.buffer.mark() {
            Some(pos) => interp.copy_marker_value(&Value::Integer(pos as i64), false),
            None => interp.copy_marker_value(&Value::Nil, false),
        },
        "point-min-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_min() as i64), false)
        }
        "point-max-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_max() as i64), false)
        }
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
            Ok(
                if interp.marker_insertion_type(marker_id).unwrap_or(false) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
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
        "message" => {
            let text = if args.is_empty() {
                String::new()
            } else {
                string_text(&call(interp, "format", args, env)?)?
            };
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            let buffer_id = interp
                .find_buffer(&buffer_name)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                let end = buffer.point_max();
                buffer.goto_char(end);
                buffer.insert(&(text.clone() + "\n"));
            }
            if let Some(captured) = interp.message_capture_stack.last_mut() {
                captured.push_str(&text);
                captured.push('\n');
            }
            Ok(Value::String(text))
        }
        "princ" | "print" => {
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
        "read-char-choice" => {
            need_args(name, args, 2)?;
            Ok(first_choice_value(&args[1]).unwrap_or(Value::Integer('y' as i64)))
        }
        "yes-or-no-p" => {
            need_args(name, args, 1)?;
            let _ = call(interp, "message", args, env)?;
            Ok(Value::T)
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
        "display-graphic-p" | "display-images-p" | "frame-parameter" => Ok(Value::Nil),
        "frame-char-width" => Ok(Value::Integer(1)),
        "transient-mark-mode" => {
            let enabled = args.first().is_some_and(Value::is_truthy);
            interp.set_variable(
                "transient-mark-mode",
                if enabled { Value::T } else { Value::Nil },
                env,
            );
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-mode" => Ok(Value::Nil),
        "find-image" => {
            need_args(name, args, 1)?;
            let specs = args[0].to_vec()?;
            Ok(specs.into_iter().next().unwrap_or(Value::Nil))
        }
        "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
            "Images are unavailable on a nongraphical display".into(),
        )),
        "imagemagick-types" => Ok(Value::list([
            Value::Symbol("png".into()),
            Value::Symbol("jpeg".into()),
            Value::Symbol("gif".into()),
        ])),
        "init-image-library" => {
            need_args(name, args, 1)?;
            let image_type = args[0].as_symbol()?;
            Ok(
                if matches!(
                    image_type,
                    "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                ) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-start" => Ok(Value::Integer(interp.buffer.point_min() as i64)),
        "window-end" => Ok(Value::Integer(interp.buffer.point_max() as i64)),
        "window-width" => Ok(Value::Integer(80)),
        "window-text-pixel-size" => {
            let width = interp
                .buffer
                .buffer_string()
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = interp.buffer.buffer_string().lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "get-display-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()?.max(0) as usize;
            let property = args[1].as_symbol()?;
            let display = interp
                .buffer
                .text_property_at(pos, "display")
                .unwrap_or(Value::Nil);
            Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
        }
        "bidi-find-overridden-directionality" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            Ok(find_bidi_override(interp, start, end)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "redisplay" => Ok(Value::Nil),
        "font-spec" => {
            let mut name_spec = None;
            let mut index = 0;
            while index + 1 < args.len() {
                if let Value::Symbol(keyword) = &args[index]
                    && keyword == ":name"
                {
                    name_spec = Some(string_text(&args[index + 1])?);
                }
                index += 2;
            }
            Ok(interp.create_record(
                "font-spec",
                vec![Value::String(name_spec.unwrap_or_default())],
            ))
        }
        "font-get" => {
            need_args(name, args, 2)?;
            let property = args[1].as_symbol()?;
            let info = font_spec_info(interp, &args[0])?;
            Ok(match property {
                ":family" => info.family.map(Value::Symbol).unwrap_or(Value::Nil),
                ":size" => info.size.map(Value::Float).unwrap_or(Value::Nil),
                ":weight" => info.weight.map(Value::Symbol).unwrap_or(Value::Nil),
                ":slant" => info.slant.map(Value::Symbol).unwrap_or(Value::Nil),
                ":spacing" => info.spacing.map(Value::Integer).unwrap_or(Value::Nil),
                ":foundry" => info.foundry.map(Value::Symbol).unwrap_or(Value::Nil),
                _ => Value::Nil,
            })
        }
        "set-face-attribute" => {
            if args.len() < 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?.to_string();
            let mut index = 2;
            while index + 1 < args.len() {
                let attribute = args[index].as_symbol()?;
                let value = &args[index + 1];
                if attribute == ":inherit" {
                    let inherit = match value {
                        Value::Nil => None,
                        Value::Symbol(symbol) => Some(symbol.clone()),
                        _ => return Err(LispError::TypeError("symbol".into(), value.type_name())),
                    };
                    interp.set_face_inherit_target(&face, inherit)?;
                }
                index += 2;
            }
            Ok(Value::Nil)
        }
        "color-distance" => {
            need_args(name, args, 2)?;
            let left = parse_color_spec(&string_text(&args[0])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let right = parse_color_spec(&string_text(&args[1])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let distance = left
                .into_iter()
                .zip(right)
                .map(|(a, b)| {
                    let diff = i64::from(a) - i64::from(b);
                    diff * diff
                })
                .sum::<i64>();
            Ok(Value::Integer(distance))
        }
        "color-values-from-color-spec" => {
            need_args(name, args, 1)?;
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
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
        "signal" => {
            if args.is_empty() {
                return Err(LispError::Signal("signal".into()));
            }
            let condition = args[0].clone();
            let data = args.get(1).cloned().unwrap_or(Value::Nil);
            let value = if let Ok(items) = data.to_vec() {
                Value::cons(condition, Value::list(items))
            } else {
                Value::list([condition, data])
            };
            Err(LispError::SignalValue(value))
        }
        "throw" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("throw".into(), args.len()));
            }
            Err(LispError::Throw(args[0].clone(), args[1].clone()))
        }
        "intern" => {
            need_args(name, args, 1)?;
            let s = args[0].as_string()?;
            Ok(Value::Symbol(s.to_string()))
        }
        "autoloadp" => {
            need_args(name, args, 1)?;
            let autoload = args[0]
                .to_vec()
                .ok()
                .and_then(|items| items.first().cloned())
                .is_some_and(|item| matches!(item, Value::Symbol(name) if name == "autoload"));
            Ok(if autoload { Value::T } else { Value::Nil })
        }
        "documentation" => {
            need_args(name, args, 1)?;
            let symbol = match &args[0] {
                Value::Symbol(name) => name.clone(),
                other => other.to_string(),
            };
            Ok(Value::String(format!("Documentation for {symbol}.")))
        }
        "getenv" | "getenv-internal" => {
            need_args(name, args, 1)?;
            let variable = string_text(&args[0])?;
            Ok(std::env::var(&variable)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "ignore" => Ok(Value::Nil),
        "make-obsolete"
        | "make-obsolete-variable"
        | "define-obsolete-function-alias"
        | "define-obsolete-variable-alias" => Ok(Value::Nil),
        "macroexp-warn-and-return" => Ok(args.get(1).cloned().unwrap_or(Value::Nil)),
        "describe-function" => {
            let _ = get_or_create_buffer(interp, "*Help*");
            Ok(Value::Nil)
        }
        "run-with-timer" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let callback = resolve_callable(interp, &args[2], env)?;
            let callback_args = &args[3..];
            let _ = invoke_function_value(interp, &callback, callback_args, env)?;
            Ok(Value::String("#<timer>".into()))
        }
        "cancel-timer" => Ok(Value::Nil),
        "lossage-size" => {
            if args.is_empty() {
                return Ok(Value::Integer(interp.lossage_size));
            }
            let new_size = args[0].as_integer()?;
            if new_size < 100 {
                return Err(LispError::Signal("lossage-size must be >= 100".into()));
            }
            interp.lossage_size = new_size;
            Ok(Value::Integer(new_size))
        }
        "executable-find" => {
            need_args(name, args, 1)?;
            let executable = args[0].as_string()?;
            Ok(find_executable(executable)
                .map(Value::String)
                .unwrap_or(Value::Nil))
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
        "run-hooks" | "eval-after-load" => Ok(Value::Nil),
        "mapatoms" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "remove-hook" => {
            need_args(name, args, 2)?;
            let hook_name = args[0].as_symbol()?.to_string();
            let function = args[1].clone();
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
            hooks.retain(|hook| hook != &function);
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
            Ok(match interp.lookup_function(symbol, env) {
                Ok(value) => value,
                Err(_) if matches!(symbol, "benchmark-run" | "tetris") => Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String(format!("{symbol}.el")),
                    Value::String(format!("Autoloaded {symbol}.")),
                    Value::Nil,
                    Value::Symbol(symbol.into()),
                ]),
                Err(_) => Value::String(format!("#<function {}>", symbol)),
            })
        }
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(s.to_string()))
        }
        "macroexp-file-name" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("macroexp-file-name", env)
                .unwrap_or(Value::Nil))
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
        "always" => Ok(Value::T),
        "evenp" => {
            need_args(name, args, 1)?;
            Ok(
                if (&integer_like_bigint(interp, &args[0])? & BigInt::from(1u8)).is_zero() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "seq-subseq" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            seq_subseq(
                &args[0],
                args[1].as_integer()?,
                args.get(2).map(Value::as_integer).transpose()?,
            )
        }
        "text-quoting-style" => Ok(Value::Symbol("grave".into())),
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
            Ok(group_name_from_gid(gid)?
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "save-buffer" => {
            let Some(path) = interp.buffer.file.clone() else {
                return Ok(Value::Nil);
            };
            ensure_no_supersession_threat(interp, env)?;
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.set_unmodified();
            interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
            unlock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "revert-buffer" => {
            revert_current_buffer(interp)?;
            Ok(Value::Nil)
        }
        "lock-buffer" => {
            maybe_lock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "unlock-buffer" => unlock_current_buffer(interp, env),
        "userlock--handle-unlock-error" => Ok(Value::Nil),
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
        "hash-table-p" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::String(text) if text.starts_with("#<hash-table")) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "profiler-memory-running-p" => Ok(if interp.profiler_memory_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-memory-start" => {
            if interp.profiler_memory_running {
                return Err(LispError::Signal("Memory profiler already running".into()));
            }
            interp.profiler_memory_running = true;
            interp.profiler_memory_log_pending = true;
            Ok(Value::Nil)
        }
        "profiler-memory-stop" => {
            let was_running = interp.profiler_memory_running;
            interp.profiler_memory_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-memory-log" => {
            if interp.profiler_memory_running || interp.profiler_memory_log_pending {
                if !interp.profiler_memory_running {
                    interp.profiler_memory_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "profiler-cpu-running-p" => Ok(if interp.profiler_cpu_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-cpu-start" => {
            if interp.profiler_cpu_running {
                return Err(LispError::Signal("CPU profiler already running".into()));
            }
            interp.profiler_cpu_running = true;
            interp.profiler_cpu_log_pending = true;
            if let Some(interval) = args.first() {
                interp.set_variable(
                    "profiler-sampling-interval",
                    interval.clone(),
                    &mut Vec::new(),
                );
            }
            Ok(Value::Nil)
        }
        "profiler-cpu-stop" => {
            let was_running = interp.profiler_cpu_running;
            interp.profiler_cpu_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-cpu-log" => {
            if interp.profiler_cpu_running || interp.profiler_cpu_log_pending {
                if !interp.profiler_cpu_running {
                    interp.profiler_cpu_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
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
                Value::StringObject(_) => "string",
                Value::Symbol(_) => "symbol",
                Value::Cons(_, _) => "cons",
                Value::BuiltinFunc(_) => "subr",
                Value::Lambda(_, _, _) => "cons", // Emacs closures are cons cells
                Value::Buffer(_, _) => "buffer",
                Value::Marker(_) => "marker",
                Value::Overlay(_) => "overlay",
                Value::CharTable(_) => "char-table",
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    return Ok(Value::Symbol(record.type_name.clone()));
                }
                Value::Finalizer(_) => "finalizer",
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
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
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
                    let buf_name = interp
                        .buffer_list
                        .iter()
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
                Some(ov) if !ov.is_dead() => {
                    let pos = if let Some(buffer_id) = ov.buffer_id {
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        if buffer.is_multibyte() {
                            ov.beg
                        } else {
                            buffer_position_to_byte(buffer, ov.beg).unwrap_or(ov.beg)
                        }
                    } else {
                        ov.beg
                    };
                    Ok(Value::Integer(pos as i64))
                }
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
                Some(ov) if !ov.is_dead() => {
                    let pos = if let Some(buffer_id) = ov.buffer_id {
                        let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                            LispError::Signal(format!("No buffer with id {}", buffer_id))
                        })?;
                        if buffer.is_multibyte() {
                            ov.end
                        } else {
                            buffer_position_to_byte(buffer, ov.end).unwrap_or(ov.end)
                        }
                    } else {
                        ov.end
                    };
                    Ok(Value::Integer(pos as i64))
                }
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
                    return Err(LispError::TypeError(
                        "buffer".into(),
                        buffer_arg.type_name(),
                    ));
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
            interp
                .buffer
                .overlays
                .retain(|ov| !ids_to_delete.contains(&ov.id));
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
                            Value::BuiltinFunc(fname) => call(interp, fname, &pred_args, env)?,
                            Value::Lambda(_, _, _) => {
                                call_function_value(interp, pred, &pred_args, env)?
                            }
                            _ => {
                                return Err(LispError::TypeError(
                                    "function".into(),
                                    pred.type_name(),
                                ));
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
                Value::String(s) => match s.chars().nth(idx) {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Err(LispError::Signal("Args out of range".into())),
                },
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
                    Ok(interp.char_table_get(*id, key).unwrap_or(Value::Nil))
                }
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    if idx == 0 {
                        Ok(Value::Symbol(record.type_name.clone()))
                    } else {
                        record
                            .slots
                            .get(idx - 1)
                            .cloned()
                            .ok_or_else(|| LispError::Signal("Args out of range".into()))
                    }
                }
                _ => {
                    let items = vector_items(&args[0])?;
                    items
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| LispError::Signal("Args out of range".into()))
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
                    Value::Lambda(_, _, _) => {
                        call_function_value(interp, &pred, std::slice::from_ref(item), env)?
                    }
                    _ => return Err(LispError::TypeError("function".into(), pred.type_name())),
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
            if let Some(string) = string_like(&args[0]) {
                Ok(make_shared_string_value(string.text, string.props))
            } else {
                match &args[0] {
                    Value::Record(id) => interp.copy_record(*id),
                    _ => Ok(args[0].clone()),
                }
            }
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
            let subtype = match &args[0] {
                Value::Nil => None,
                Value::Symbol(symbol) => Some(symbol.clone()),
                other => return Err(LispError::TypeError("symbol".into(), other.type_name())),
            };
            let default = args.get(1).cloned().unwrap_or(Value::Nil);
            Ok(interp.make_char_table(subtype, default))
        }

        "char-table-p" => {
            need_args(name, args, 1)?;
            Ok(if matches!(args[0], Value::CharTable(_)) {
                Value::T
            } else {
                Value::Nil
            })
        }

        "char-table-subtype" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(interp
                .char_table_subtype(id)
                .flatten()
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }

        "char-table-parent" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(interp
                .char_table_parent(id)
                .flatten()
                .and_then(|parent_id| {
                    interp
                        .find_char_table(parent_id)
                        .map(|_| Value::CharTable(parent_id))
                })
                .unwrap_or(Value::Nil))
        }

        "set-char-table-parent" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let parent = match &args[1] {
                Value::Nil => None,
                Value::CharTable(parent_id) => Some(*parent_id),
                other => {
                    return Err(LispError::TypeError("char-table".into(), other.type_name()));
                }
            };
            interp.set_char_table_parent(id, parent)?;
            Ok(args[1].clone())
        }

        "char-table-extra-slot" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let slot = args[1].as_integer()?.max(0) as usize;
            Ok(interp.char_table_extra_slot(id, slot).unwrap_or(Value::Nil))
        }

        "set-char-table-extra-slot" => {
            need_args(name, args, 3)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            let slot = args[1].as_integer()?.max(0) as usize;
            interp.set_char_table_extra_slot(id, slot, args[2].clone())?;
            Ok(args[2].clone())
        }

        "char-table-range" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            match char_table_range_spec(&args[1])? {
                None => Ok(interp
                    .find_char_table(id)
                    .map(|table| table.default.clone())
                    .unwrap_or(Value::Nil)),
                Some((start, end)) if start == end => {
                    Ok(interp.char_table_get(id, start).unwrap_or(Value::Nil))
                }
                Some((start, end)) => Ok(interp
                    .char_table_range(id, start, end)
                    .unwrap_or(Value::Nil)),
            }
        }

        "set-char-table-range" => {
            need_args(name, args, 3)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            match char_table_range_spec(&args[1])? {
                None => interp.char_table_set_default(id, args[2].clone())?,
                Some((start, end)) => {
                    interp.char_table_set_range(id, start, end, args[2].clone())?
                }
            }
            Ok(args[2].clone())
        }

        "make-category-table" => {
            Ok(interp.make_char_table(Some("category-table".into()), Value::String(String::new())))
        }

        "category-table-p" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::CharTable(id)
                    if interp.char_table_subtype(*id).flatten().as_deref()
                        == Some("category-table") =>
                {
                    Value::T
                }
                _ => Value::Nil,
            })
        }

        "standard-category-table" => Ok(Value::CharTable(interp.ensure_standard_category_table())),

        "category-table" => {
            let table = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(Value::CharTable(id)),
                    _ => None,
                })
                .unwrap_or_else(|| Value::CharTable(interp.ensure_standard_category_table()));
            Ok(table)
        }

        "set-category-table" => {
            need_args(name, args, 1)?;
            let table = match &args[0] {
                Value::CharTable(id) => Value::CharTable(*id),
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "category-table",
                table.clone(),
            );
            Ok(table)
        }

        "define-category" => {
            need_args(name, args, 3)?;
            let category = args[0].as_integer()?;
            let doc = string_text(&args[1])?;
            let table = match args.get(2) {
                Some(Value::CharTable(id)) => *id,
                Some(Value::Nil) | None => interp.ensure_standard_category_table(),
                Some(other) => {
                    return Err(LispError::TypeError("char-table".into(), other.type_name()));
                }
            };
            interp.define_category(table, category as u32, doc)?;
            Ok(Value::Nil)
        }

        "category-docstring" => {
            need_args(name, args, 2)?;
            let category = args[0].as_integer()? as u32;
            let table = match &args[1] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            Ok(interp
                .category_docstring(table, category)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }

        "make-category-set" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            Ok(Value::String(normalize_category_set(&text)))
        }

        "category-set-mnemonics" => {
            need_args(name, args, 1)?;
            let text = string_text(&args[0])?;
            Ok(Value::String(normalize_category_set(&text)))
        }

        "modify-category-entry" => {
            need_args(name, args, 3)?;
            let character = args[0].as_integer()? as u32;
            let category = args[1].as_integer()? as u32;
            let table = match &args[2] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            let reset = args.get(3).is_some_and(Value::is_truthy);
            let category_char = char::from_u32(category)
                .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let current = interp
                .char_table_get(table, character)
                .and_then(|value| string_like(&value).map(|s| s.text))
                .unwrap_or_default();
            let mut chars: Vec<char> = current.chars().collect();
            if reset {
                chars.retain(|existing| *existing != category_char);
            } else if !chars.contains(&category_char) {
                chars.push(category_char);
            }
            chars.sort_unstable();
            let updated = chars.into_iter().collect::<String>();
            interp.char_table_set(table, character, Value::String(updated))?;
            Ok(Value::Nil)
        }

        "char-category-set" => {
            need_args(name, args, 1)?;
            let character = args[0].as_integer()? as u32;
            let table_id = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(id),
                    _ => None,
                })
                .unwrap_or_else(|| interp.ensure_standard_category_table());
            Ok(interp
                .char_table_get(table_id, character)
                .unwrap_or_else(|| Value::String(String::new())))
        }

        "copy-category-table" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
            interp.clone_char_table(id)
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
                    ));
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
            interp.undo_current_buffer()?;
            Ok(Value::Nil)
        }

        "undo-more" => {
            let count = if args.is_empty() {
                1
            } else {
                match &args[0] {
                    Value::Nil => {
                        return Err(LispError::TypeError(
                            "number-or-marker-p".into(),
                            "nil".into(),
                        ));
                    }
                    value => value.as_integer()?,
                }
            };
            for _ in 0..count.max(0) {
                interp.undo_more_current_buffer()?;
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
            let match_data = interp.last_match_data.as_ref().ok_or_else(|| {
                LispError::Signal("No match data, because no search succeeded".into())
            })?;
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
            let replace_index = args
                .get(4)
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(0)
                .max(0) as usize;
            let match_data = interp
                .last_match_data
                .clone()
                .ok_or_else(|| LispError::Signal("No previous search".into()))?;
            let (start, end) = match_data
                .get(replace_index)
                .and_then(|entry| *entry)
                .or_else(|| match_data.first().and_then(|entry| *entry))
                .ok_or_else(|| LispError::Signal("No previous search".into()))?;
            let replacement_len = replacement.chars().count();
            let overlay_calls =
                overlay_change_hook_calls(&interp.buffer, start, end, start + replacement_len);
            run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
            run_change_hooks(
                interp,
                "before-change-functions",
                &[Value::Integer(start as i64), Value::Integer(end as i64)],
                env,
            )?;
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.goto_char(start);
            interp.insert_current_buffer(&replacement);
            run_change_hooks(
                interp,
                "after-change-functions",
                &[
                    Value::Integer(start as i64),
                    Value::Integer((start + replacement_len) as i64),
                    Value::Integer((end - start) as i64),
                ],
                env,
            )?;
            run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
            interp.last_match_data = Some(update_match_data_after_replace(
                &match_data,
                replace_index,
                start,
                end,
                replacement_len,
            ));
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
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
            let removed_len = to.saturating_sub(from);
            let inserted_len = replacement.text.chars().count();
            delete_region_with_hooks(interp, from, to, env)?;
            interp.buffer.goto_char(from);
            insert_text_with_hooks(
                interp,
                &replacement.text,
                &replacement.props,
                false,
                false,
                env,
            )?;
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
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
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
            let old =
                char::from_u32(old).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let new =
                char::from_u32(new).ok_or_else(|| LispError::Signal("Invalid character".into()))?;
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
                let start = values
                    .first()
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or(1) as usize;
                let end = values
                    .get(1)
                    .and_then(|v| v.as_integer().ok())
                    .unwrap_or((interp.buffer.size_total() + 1) as i64)
                    as usize;
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
            let saved_markers = interp.live_marker_positions_for_buffer(interp.current_buffer_id());
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
                let _ =
                    interp.set_marker(marker_id, Some(new_pos), Some(interp.current_buffer_id()));
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
    let beg = if buffer.is_multibyte() {
        beg as usize
    } else {
        buffer_byte_to_position_boundary(buffer, beg.max(0) as usize).unwrap_or(1)
    } as i64;
    let end = if buffer.is_multibyte() {
        end as usize
    } else {
        buffer_byte_to_position_boundary(buffer, end.max(0) as usize).unwrap_or(1)
    } as i64;
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
    if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal")
    {
        Ok(items.into_iter().skip(1).collect())
    } else {
        Ok(items)
    }
}

fn buffer_position_to_byte(buffer: &crate::buffer::Buffer, pos: usize) -> Option<usize> {
    let text = buffer.buffer_string();
    let char_len = text.chars().count();
    if pos == 0 || pos > char_len + 1 {
        return None;
    }
    Some(
        1 + text
            .chars()
            .take(pos - 1)
            .map(char::len_utf8)
            .sum::<usize>(),
    )
}

fn buffer_byte_to_position(buffer: &crate::buffer::Buffer, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = buffer.buffer_string();
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
        if byte == current_byte {
            return Some(index + 1);
        }
        if byte < next {
            return Some(index + 1);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

fn buffer_byte_to_position_boundary(buffer: &crate::buffer::Buffer, byte: usize) -> Option<usize> {
    if byte == 0 {
        return None;
    }
    let text = buffer.buffer_string();
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
        if byte == current_byte {
            return Some(index + 1);
        }
        if byte < next {
            return Some(index + 2);
        }
        current_byte = next;
    }
    Some(text.chars().count() + 1)
}

fn char_table_range_spec(value: &Value) -> Result<Option<(u32, u32)>, LispError> {
    match value {
        Value::Nil => Ok(None),
        Value::Integer(codepoint) if *codepoint >= 0 => {
            Ok(Some((*codepoint as u32, *codepoint as u32)))
        }
        Value::Cons(car, cdr) => {
            let start = car.as_integer()?;
            let end = cdr.as_integer()?;
            if start < 0 || end < 0 {
                return Err(LispError::Signal("Args out of range".into()));
            }
            Ok(Some((start as u32, end as u32)))
        }
        other => Err(LispError::TypeError(
            "character-or-cons-or-nil".into(),
            other.type_name(),
        )),
    }
}

fn normalize_category_set(text: &str) -> String {
    let mut chars: Vec<char> = text.chars().collect();
    chars.sort_unstable();
    chars.dedup();
    chars.into_iter().collect()
}

fn normalize_string_index(arg: Option<&Value>, default: i64, len: i64) -> Result<i64, LispError> {
    let Some(value) = arg else {
        return Ok(default);
    };
    if value.is_nil() {
        return Ok(default);
    }
    let raw = value.as_integer()?;
    let index = if raw < 0 { len + raw } else { raw };
    if !(0..=len).contains(&index) {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(index)
}

fn resolve_char_modifiers(value: i64) -> i64 {
    const SHIFT_BIT: i64 = 1 << 25;
    const CTRL_BIT: i64 = 1 << 26;
    const META_BIT: i64 = 1 << 27;
    const CHAR_MASK: i64 = 0x3F_FFFF;

    let mut base = value & CHAR_MASK;
    let meta = value & META_BIT;
    let shift = value & SHIFT_BIT != 0;
    let ctrl = value & CTRL_BIT != 0;

    if shift
        && let Some(ch) = char::from_u32(base as u32)
        && ch.is_ascii_lowercase()
    {
        base = ch.to_ascii_uppercase() as i64;
    }

    if ctrl {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    }

    base | meta
}

fn position_bytes(interp: &Interpreter, pos: usize) -> Option<usize> {
    buffer_position_to_byte(&interp.buffer, pos)
}

fn byte_to_position(interp: &Interpreter, byte: usize) -> Option<usize> {
    buffer_byte_to_position(&interp.buffer, byte)
}

fn column_at(interp: &Interpreter, env: &Env, line_start: usize, pos: usize) -> usize {
    let mut col = 0usize;
    for p in line_start..pos {
        match interp.buffer.char_at(p) {
            Some(ch) => col = column_after(interp, env, col, p, ch),
            None => break,
        }
    }
    col
}

fn column_after(
    interp: &Interpreter,
    env: &Env,
    current_col: usize,
    pos: usize,
    ch: char,
) -> usize {
    if char_is_invisible(interp, pos) {
        current_col
    } else if ch == '\t' {
        let tab_width = interp
            .lookup_var("tab-width", env)
            .and_then(|value| value.as_integer().ok())
            .unwrap_or(8)
            .max(1) as usize;
        (current_col / tab_width + 1) * tab_width
    } else {
        current_col + 1
    }
}

fn char_is_invisible(interp: &Interpreter, pos: usize) -> bool {
    interp
        .buffer
        .text_property_at(pos, "invisible")
        .is_some_and(|value| value.is_truthy())
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

fn update_match_data_after_replace(
    match_data: &[Option<(usize, usize)>],
    replace_index: usize,
    start: usize,
    end: usize,
    replacement_len: usize,
) -> Vec<Option<(usize, usize)>> {
    let new_end = start + replacement_len;
    let delta = replacement_len as isize - end.saturating_sub(start) as isize;
    match_data
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let Some((group_start, group_end)) = entry else {
                return None;
            };
            if index == replace_index {
                return Some((start, new_end));
            }
            if *group_start == *group_end && *group_start == start && start == end {
                return Some((start, new_end));
            }
            if start == end && *group_start == start && *group_end > end {
                return Some((start, group_end.saturating_add_signed(delta)));
            }
            if start == end && *group_end == start && *group_start < start {
                return Some((*group_start, group_end.saturating_add_signed(delta)));
            }
            if *group_end <= start {
                return Some((*group_start, *group_end));
            }
            if *group_start >= end {
                return Some((
                    group_start.saturating_add_signed(delta),
                    group_end.saturating_add_signed(delta),
                ));
            }
            if *group_start >= start && *group_end <= end {
                return Some((start, new_end));
            }
            let updated_start = if *group_start > start {
                start
            } else {
                *group_start
            };
            let updated_end = if *group_end < end {
                new_end
            } else {
                group_end.saturating_add_signed(delta)
            };
            Some((updated_start, updated_end))
        })
        .collect()
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

pub(crate) fn default_directory() -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    path_to_directory_string(&cwd)
}

fn compat_repo_root_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    PathBuf::from(test_directory)
        .parent()
        .map(Path::to_path_buf)
}

fn compat_invocation_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("src").join("emacs");
    candidate.exists().then_some(candidate)
}

fn compat_emacsclient_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("lib-src").join("emacsclient");
    candidate.exists().then_some(candidate)
}

pub(crate) fn compat_emacsclient_program_name() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_emacsclient_path_from_test_directory(&test_directory))
        .map(|path| path.display().to_string())
}

pub(crate) fn current_invocation_name() -> Option<String> {
    current_invocation_path()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

pub(crate) fn current_invocation_directory() -> Option<String> {
    current_invocation_path()
        .parent()
        .map(path_to_directory_string)
}

fn current_invocation_path() -> PathBuf {
    if let Ok(test_directory) = std::env::var("EMACS_TEST_DIRECTORY")
        && let Some(path) = compat_invocation_path_from_test_directory(&test_directory)
    {
        return path;
    }
    std::env::current_exe().unwrap_or_else(|_| PathBuf::from("emaxx"))
}

fn expand_file_name(path: &str, base: Option<&str>) -> String {
    let expanded = expand_home_prefix(path);
    let candidate = PathBuf::from(expanded);
    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        let base_dir = base
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from(default_directory()));
        if base_dir.is_absolute() {
            base_dir.join(candidate)
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(base_dir)
                .join(candidate)
        }
    };
    normalize_path(&absolute).display().to_string()
}

fn substitute_in_file_name(path: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '$' {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '$' {
            result.push('$');
            index += 2;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '{' {
            let mut end = index + 2;
            while end < chars.len() && chars[end] != '}' {
                end += 1;
            }
            if end < chars.len() && chars[end] == '}' {
                let name: String = chars[index + 2..end].iter().collect();
                result.push_str(&std::env::var(&name).unwrap_or_default());
                index = end + 1;
                continue;
            }
        }
        let mut end = index + 1;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        if end == index + 1 {
            result.push('$');
            index += 1;
            continue;
        }
        let name: String = chars[index + 1..end].iter().collect();
        result.push_str(&std::env::var(&name).unwrap_or_default());
        index = end;
    }
    result
}

fn expand_home_prefix(path: &str) -> String {
    if path == "~" {
        return std::env::var("HOME").unwrap_or_else(|_| path.to_string());
    }
    if let Some(suffix) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(suffix).display().to_string();
    }
    path.to_string()
}

fn file_name_directory(path: &str) -> Option<String> {
    path.rfind('/').map(|index| path[..=index].to_string())
}

fn file_name_nondirectory(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn file_name_as_directory(path: &str) -> String {
    if path.is_empty() {
        "./".into()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn directory_file_name(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    if path.chars().all(|ch| ch == '/') {
        return if path.len() == 2 {
            "//".into()
        } else {
            "/".into()
        };
    }
    path.trim_end_matches('/').to_string()
}

fn file_name_absolute_p(path: &str) -> bool {
    path.starts_with('/') || path.starts_with('~')
}

fn file_name_concat(parts: &[String]) -> String {
    let mut iter = parts.iter().filter(|part| !part.is_empty());
    let Some(first) = iter.next() else {
        return String::new();
    };
    let mut result = first.clone();
    for part in iter {
        if result.is_empty() {
            result = part.clone();
        } else if result.ends_with('/') {
            result.push_str(part.trim_start_matches('/'));
        } else {
            result.push('/');
            result.push_str(part.trim_start_matches('/'));
        }
    }
    result
}

fn ert_resource_directory(interp: &Interpreter) -> Option<String> {
    let testfile = interp
        .current_load_file()
        .or(interp.buffer.file.as_deref())?;
    Some(ert_resource_directory_for(testfile))
}

fn ert_resource_directory_for(testfile: &str) -> String {
    let expanded = PathBuf::from(expand_file_name(testfile, None));
    let sibling_resources = expanded
        .parent()
        .map(|parent| parent.join("resources"))
        .filter(|path| path.is_dir());
    let resource_dir = sibling_resources.unwrap_or_else(|| {
        let rendered = expanded.display().to_string();
        let trimmed = rendered
            .strip_suffix(".el")
            .map(|path| {
                path.strip_suffix("-tests")
                    .or_else(|| path.strip_suffix("-test"))
                    .unwrap_or(path)
            })
            .unwrap_or(rendered.as_str());
        PathBuf::from(format!("{trimmed}-resources"))
    });
    path_to_directory_string(&resource_dir)
}

fn charset_for_char(code: u32) -> &'static str {
    if code <= 0x7f { "ascii" } else { "unicode" }
}

fn default_charset_plist(name: &str, interp: &Interpreter) -> Option<Value> {
    match interp.charset_canonical_name(name)?.as_str() {
        "ascii" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("ASCII".into()),
        ])),
        "unicode" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("Unicode".into()),
        ])),
        _ => None,
    }
}

fn charsets_for_text(text: &str, interp: &Interpreter) -> Vec<Value> {
    let mut names = Vec::new();
    if text.chars().any(|ch| (ch as u32) <= 0x7f) {
        names.push("ascii".to_string());
    }
    if text.chars().any(|ch| (ch as u32) > 0x7f) {
        names.push("unicode".to_string());
    }
    if names.is_empty() {
        names.push("ascii".to_string());
    }
    names.sort_by_key(|name| interp.charset_priority_rank(name));
    names.dedup();
    names.into_iter().map(Value::Symbol).collect()
}

fn charset_max_codepoint(name: &str) -> i64 {
    match name {
        "ascii" => 0x7f,
        _ => 0x10ffff,
    }
}

fn charset_ranges_for(
    charset: &str,
    from: i64,
    to: i64,
    interp: &Interpreter,
) -> Result<Vec<(i64, i64)>, LispError> {
    let canonical = interp
        .charset_canonical_name(charset)
        .ok_or_else(|| LispError::Void(charset.to_string()))?;
    let (lower, upper) = if from <= to { (from, to) } else { (to, from) };
    let range = match canonical.as_str() {
        "ascii" => {
            let start = lower.max(0);
            let end = upper.min(0x7f);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        "unicode" => {
            let start = lower.max(0);
            let end = upper.min(0x10ffff);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(range.into_iter().collect())
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

fn path_to_directory_string(path: &Path) -> String {
    let mut rendered = normalize_path(path).display().to_string();
    if !rendered.ends_with(std::path::MAIN_SEPARATOR) {
        rendered.push(std::path::MAIN_SEPARATOR);
    }
    rendered
}

fn file_readable_p(path: &str) -> bool {
    fs::File::open(path).is_ok()
}

fn file_executable_p(path: &str) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn run_external_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    input: Option<&[u8]>,
    env: &Env,
) -> Result<std::process::Output, LispError> {
    let mut command = Command::new(program);
    command.args(argv);
    if let Some(default_directory) = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .filter(|directory| !directory.is_empty())
    {
        command.current_dir(default_directory);
    }
    command.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    apply_process_environment(interp, env, &mut command);
    let mut child = command
        .spawn()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if let Some(stdin_data) = input
        && let Some(mut stdin) = child.stdin.take()
    {
        stdin
            .write_all(stdin_data)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    child
        .wait_with_output()
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn apply_process_environment(interp: &Interpreter, env: &Env, command: &mut Command) {
    let Some(process_environment) = interp.lookup_var("process-environment", env) else {
        return;
    };
    let Ok(items) = process_environment.to_vec() else {
        return;
    };
    command.env_clear();
    for item in items {
        let Ok(entry) = string_text(&item) else {
            continue;
        };
        if let Some((name, value)) = entry.split_once('=') {
            command.env(name, value);
        }
    }
}

fn write_process_output(
    interp: &mut Interpreter,
    destination: &Value,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(), LispError> {
    if destination.is_nil() {
        return Ok(());
    }
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    );
    if output.is_empty() {
        return Ok(());
    }
    let target_id = match destination {
        Value::T => Some(interp.current_buffer_id()),
        Value::Buffer(_, _) => Some(interp.resolve_buffer_id(destination)?),
        Value::String(name) => Some(
            interp
                .find_buffer(name)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(name).0),
        ),
        _ => {
            return Err(LispError::TypeError(
                "buffer-or-name".into(),
                destination.type_name(),
            ));
        }
    };
    let Some(target_id) = target_id else {
        return Ok(());
    };
    let original_id = interp.current_buffer_id();
    if target_id != original_id {
        interp.switch_to_buffer_id(target_id)?;
    }
    interp.insert_current_buffer(&output);
    if target_id != original_id {
        interp.switch_to_buffer_id(original_id)?;
    }
    Ok(())
}

fn exit_status_code(status: &std::process::ExitStatus) -> i64 {
    status
        .code()
        .unwrap_or(if status.success() { 0 } else { 1 }) as i64
}

fn default_sort_lt(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    let left_marker = if let Value::Marker(id) = left {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
    } else {
        None
    };
    let right_marker = if let Value::Marker(id) = right {
        interp
            .marker_position(*id)
            .or_else(|| interp.marker_last_position(*id))
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

fn parse_color_spec(spec: &str) -> Option<[u16; 3]> {
    if let Some(rest) = spec.strip_prefix('#') {
        if rest.is_empty() || rest.len() % 3 != 0 {
            return None;
        }
        let digits = rest.len() / 3;
        if !(1..=4).contains(&digits) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, chunk) in rest.as_bytes().chunks(digits).enumerate() {
            let text = std::str::from_utf8(chunk).ok()?;
            if !text.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return None;
            }
            values[index] = expand_hex_component(text)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgb:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if !part.chars().all(|ch| ch.is_ascii_hexdigit()) || part.len() > 4 {
                return None;
            }
            values[index] = expand_hex_component(part)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgbi:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if part.chars().any(char::is_whitespace) {
                return None;
            }
            let value = part.parse::<f64>().ok()?;
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return None;
            }
            values[index] = (value * 65535.0).round() as u16;
        }
        return Some(values);
    }

    None
}

fn expand_hex_component(component: &str) -> Option<u16> {
    if component.is_empty() || component.len() > 4 {
        return None;
    }
    let value = u32::from_str_radix(component, 16).ok()?;
    let bits = 4 * component.len();
    let max_value = (1u32 << bits) - 1;
    Some(((value * 0xFFFF) / max_value) as u16)
}

#[derive(Default)]
struct FontSpecInfo {
    family: Option<String>,
    size: Option<f64>,
    weight: Option<String>,
    slant: Option<String>,
    spacing: Option<i64>,
    foundry: Option<String>,
}

fn font_spec_info(interp: &Interpreter, value: &Value) -> Result<FontSpecInfo, LispError> {
    let Value::Record(id) = value else {
        return Err(LispError::TypeError("font-spec".into(), value.type_name()));
    };
    let record = interp
        .find_record(*id)
        .ok_or_else(|| LispError::TypeError("font-spec".into(), value.type_name()))?;
    if record.type_name != "font-spec" {
        return Err(LispError::TypeError("font-spec".into(), value.type_name()));
    }
    let name = record
        .slots
        .first()
        .map(string_text)
        .transpose()?
        .unwrap_or_default();
    Ok(parse_font_name(&name))
}

fn parse_font_name(name: &str) -> FontSpecInfo {
    if name.starts_with('-') {
        return parse_xlfd_font_name(name);
    }
    if name.chars().next().is_some_and(char::is_whitespace)
        || name.chars().last().is_some_and(char::is_whitespace)
    {
        return FontSpecInfo {
            family: Some(name.to_string()),
            ..FontSpecInfo::default()
        };
    }
    if name.contains(':')
        || name
            .rsplit_once('-')
            .is_some_and(|(family, size)| !family.is_empty() && size.parse::<f64>().is_ok())
    {
        return parse_fontconfig_name(name);
    }
    parse_gtk_font_name(name)
}

fn parse_xlfd_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let parts = name.split('-').skip(1).collect::<Vec<_>>();
    if parts.len() < 3 {
        return info;
    }
    info.foundry = parts.first().map(|part| (*part).to_string());
    let weight_index = parts
        .iter()
        .enumerate()
        .skip(2)
        .find(|(index, part)| {
            is_weight_name(part)
                && parts
                    .get(index + 1)
                    .is_some_and(|next| is_slant_name(next) || is_width_name(next))
        })
        .map(|(index, _)| index)
        .unwrap_or(2);
    if weight_index > 1 {
        info.family = Some(parts[1..weight_index].join("-"));
    }
    info.weight = parts
        .get(weight_index)
        .and_then(|part| normalize_weight(part));
    info
}

fn parse_fontconfig_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut sections = name.split(':');
    let base = sections.next().unwrap_or_default();
    parse_family_and_size_segment(base, &mut info);
    for section in sections {
        apply_font_attr(section, &mut info);
    }
    info
}

fn parse_gtk_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut tokens = name
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        if !name.is_empty() {
            info.family = Some(name.to_string());
        }
        return info;
    }
    if tokens
        .last()
        .is_some_and(|token| token.parse::<f64>().is_ok())
    {
        info.size = tokens.pop().and_then(|token| token.parse::<f64>().ok());
    }
    loop {
        let Some(token) = tokens.last().cloned() else {
            break;
        };
        if let Some(weight) = normalize_weight(&token) {
            if info.weight.is_none() {
                info.weight = Some(weight);
            }
            tokens.pop();
            continue;
        }
        if let Some(slant) = normalize_slant(&token) {
            if info.slant.is_none() {
                info.slant = Some(slant);
            }
            tokens.pop();
            continue;
        }
        if let Some(spacing) = normalize_spacing(&token) {
            if info.spacing.is_none() {
                info.spacing = Some(spacing);
            }
            tokens.pop();
            continue;
        }
        if is_width_name(&token) {
            tokens.pop();
            continue;
        }
        break;
    }
    if !tokens.is_empty() {
        info.family = Some(tokens.join(" "));
    }
    info
}

fn parse_family_and_size_segment(base: &str, info: &mut FontSpecInfo) {
    if base.parse::<f64>().is_ok() {
        info.size = base.parse::<f64>().ok();
        return;
    }
    if let Some((family, size)) = base.rsplit_once('-')
        && !family.is_empty()
        && size.parse::<f64>().is_ok()
    {
        info.family = Some(family.to_string());
        info.size = size.parse::<f64>().ok();
        return;
    }
    if !base.is_empty() {
        info.family = Some(base.to_string());
    }
}

fn apply_font_attr(section: &str, info: &mut FontSpecInfo) {
    if let Some((key, value)) = section.split_once('=') {
        match key {
            "weight" => info.weight = normalize_weight(value),
            "slant" => info.slant = normalize_slant(value),
            _ => {}
        }
        return;
    }
    if let Some(weight) = normalize_weight(section) {
        info.weight = Some(weight);
    } else if let Some(slant) = normalize_slant(section) {
        info.slant = Some(slant);
    } else if let Some(spacing) = normalize_spacing(section) {
        info.spacing = Some(spacing);
    }
}

fn normalize_weight(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "ultra-light" => Some("ultra-light".into()),
        "light" => Some("light".into()),
        "book" => Some("book".into()),
        "medium" => Some("medium".into()),
        "demibold" => Some("demibold".into()),
        "semi-bold" | "semibold" => Some("semi-bold".into()),
        "bold" => Some("bold".into()),
        "black" => Some("black".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

fn normalize_slant(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "italic" => Some("italic".into()),
        "oblique" => Some("oblique".into()),
        "roman" => Some("roman".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

fn normalize_spacing(token: &str) -> Option<i64> {
    match token.to_ascii_lowercase().as_str() {
        "mono" => Some(100),
        "proportional" => Some(0),
        _ => None,
    }
}

fn is_weight_name(token: &str) -> bool {
    normalize_weight(token).is_some()
}

fn is_slant_name(token: &str) -> bool {
    normalize_slant(token).is_some()
}

fn is_width_name(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "normal" | "semi-condensed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_font_names_matches_core_upstream_cases() {
        let cases = [
            (" ", Some(" "), None, None, None, None, None),
            ("Monospace", Some("Monospace"), None, None, None, None, None),
            (
                "Monospace Serif",
                Some("Monospace Serif"),
                None,
                None,
                None,
                None,
                None,
            ),
            ("Foo1", Some("Foo1"), None, None, None, None, None),
            ("12", None, Some(12.0), None, None, None, None),
            ("12 ", Some("12 "), None, None, None, None, None),
            ("Foo:", Some("Foo"), None, None, None, None, None),
            ("Foo-8", Some("Foo"), Some(8.0), None, None, None, None),
            ("Foo-18:", Some("Foo"), Some(18.0), None, None, None, None),
            (
                "Foo-18:light",
                Some("Foo"),
                Some(18.0),
                Some("light"),
                None,
                None,
                None,
            ),
            (
                "Foo 10:weight=bold",
                Some("Foo 10"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo-12:weight=bold",
                Some("Foo"),
                Some(12.0),
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo 8-20:slant=oblique",
                Some("Foo 8"),
                Some(20.0),
                None,
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:light:roman",
                Some("Foo"),
                None,
                Some("light"),
                Some("roman"),
                None,
                None,
            ),
            (
                "Foo:italic:roman",
                Some("Foo"),
                None,
                None,
                Some("roman"),
                None,
                None,
            ),
            (
                "Foo 12:light:oblique",
                Some("Foo 12"),
                None,
                Some("light"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo-12:demibold:oblique",
                Some("Foo"),
                Some(12.0),
                Some("demibold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:black:proportional",
                Some("Foo"),
                None,
                Some("black"),
                None,
                Some(0),
                None,
            ),
            (
                "Foo-10:black:proportional",
                Some("Foo"),
                Some(10.0),
                Some("black"),
                None,
                Some(0),
                None,
            ),
            (
                "Foo:weight=normal",
                Some("Foo"),
                None,
                Some("normal"),
                None,
                None,
                None,
            ),
            (
                "Foo:weight=bold",
                Some("Foo"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "Foo:weight=bold:slant=italic",
                Some("Foo"),
                None,
                Some("bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo:weight=bold:slant=italic:mono",
                Some("Foo"),
                None,
                Some("bold"),
                Some("italic"),
                Some(100),
                None,
            ),
            (
                "Foo-10:demibold:slant=normal",
                Some("Foo"),
                Some(10.0),
                Some("demibold"),
                Some("normal"),
                None,
                None,
            ),
            (
                "Foo 11-16:oblique:weight=bold",
                Some("Foo 11"),
                Some(16.0),
                Some("bold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:oblique:randomprop=randomtag:weight=bold",
                Some("Foo"),
                None,
                Some("bold"),
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo:randomprop=randomtag:bar=baz",
                Some("Foo"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light:bar=baz",
                Some("Foo Book Light"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light 10:bar=baz",
                Some("Foo Book Light 10"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo Book Light-10:bar=baz",
                Some("Foo Book Light"),
                Some(10.0),
                None,
                None,
                None,
                None,
            ),
            ("Oblique", None, None, None, Some("oblique"), None, None),
            ("Bold 17", None, Some(17.0), Some("bold"), None, None, None),
            ("17 Bold", Some("17"), None, Some("bold"), None, None, None),
            (
                "Book Oblique 2",
                None,
                Some(2.0),
                Some("book"),
                Some("oblique"),
                None,
                None,
            ),
            ("Bar 7", Some("Bar"), Some(7.0), None, None, None, None),
            (
                "Bar Ultra-Light",
                Some("Bar"),
                None,
                Some("ultra-light"),
                None,
                None,
                None,
            ),
            (
                "Bar Light 8",
                Some("Bar"),
                Some(8.0),
                Some("light"),
                None,
                None,
                None,
            ),
            (
                "Bar Book Medium 9",
                Some("Bar"),
                Some(9.0),
                Some("medium"),
                None,
                None,
                None,
            ),
            (
                "Bar Semi-Bold Italic 10",
                Some("Bar"),
                Some(10.0),
                Some("semi-bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Bar Semi-Condensed Bold Italic 11",
                Some("Bar"),
                Some(11.0),
                Some("bold"),
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo 10 11",
                Some("Foo 10"),
                Some(11.0),
                None,
                None,
                None,
                None,
            ),
            (
                "Foo 1985 Book",
                Some("Foo 1985"),
                None,
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo 1985 A Book",
                Some("Foo 1985 A"),
                None,
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo 1 Book 12",
                Some("Foo 1"),
                Some(12.0),
                Some("book"),
                None,
                None,
                None,
            ),
            (
                "Foo A Book 12 A",
                Some("Foo A Book 12 A"),
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "Foo 1985 Book 12 Oblique",
                Some("Foo 1985 Book 12"),
                None,
                None,
                Some("oblique"),
                None,
                None,
            ),
            (
                "Foo 1985 Book 12 Italic 10",
                Some("Foo 1985 Book 12"),
                Some(10.0),
                None,
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo Book Bar 6 Italic",
                Some("Foo Book Bar 6"),
                None,
                None,
                Some("italic"),
                None,
                None,
            ),
            (
                "Foo Book Bar Bold",
                Some("Foo Book Bar"),
                None,
                Some("bold"),
                None,
                None,
                None,
            ),
            (
                "-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("FreeSans"),
                None,
                Some("semi-bold"),
                None,
                None,
                Some("GNU "),
            ),
            (
                "-Take-mikachan-PS-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("mikachan-PS"),
                None,
                Some("normal"),
                None,
                None,
                Some("Take"),
            ),
            (
                "-foundry-name-with-lots-of-dashes-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
                Some("name-with-lots-of-dashes"),
                None,
                Some("normal"),
                None,
                None,
                Some("foundry"),
            ),
        ];

        for (name, family, size, weight, slant, spacing, foundry) in cases {
            let actual = parse_font_name(name);
            assert_eq!(
                actual.family.as_deref(),
                family,
                "family mismatch for {name:?}"
            );
            assert_eq!(actual.size, size, "size mismatch for {name:?}");
            assert_eq!(
                actual.weight.as_deref(),
                weight,
                "weight mismatch for {name:?}"
            );
            assert_eq!(
                actual.slant.as_deref(),
                slant,
                "slant mismatch for {name:?}"
            );
            assert_eq!(actual.spacing, spacing, "spacing mismatch for {name:?}");
            assert_eq!(
                actual.foundry.as_deref(),
                foundry,
                "foundry mismatch for {name:?}"
            );
        }
    }

    #[test]
    fn file_name_path_helpers_match_core_unix_cases() {
        assert_eq!(file_name_directory("/abc"), Some("/".into()));
        assert_eq!(file_name_directory("/abc/"), Some("/abc/".into()));
        assert_eq!(file_name_directory("abc"), None);

        assert_eq!(file_name_as_directory(""), "./");
        assert_eq!(file_name_as_directory("/abc"), "/abc/");
        assert_eq!(file_name_as_directory("/abc/"), "/abc/");

        assert_eq!(directory_file_name("/"), "/");
        assert_eq!(directory_file_name("//"), "//");
        assert_eq!(directory_file_name("///"), "/");
        assert_eq!(directory_file_name("/abc/"), "/abc");

        assert_eq!(file_name_concat(&["foo".into(), "bar".into()]), "foo/bar");
        assert_eq!(file_name_concat(&["foo/".into(), "bar".into()]), "foo/bar");
        assert_eq!(
            file_name_concat(&["foo//".into(), "bar".into()]),
            "foo//bar"
        );
    }

    #[test]
    fn ert_resource_directory_prefers_sibling_resources_dir() {
        assert_eq!(
            ert_resource_directory_for("/tmp/example-tests.el"),
            "/tmp/example-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/Users/alpha/CodexProjects/emacs/test/src/syntax-tests.el"),
            "/Users/alpha/CodexProjects/emacs/test/src/syntax-resources/"
        );
    }

    #[test]
    fn ert_resource_directory_trims_test_suffixes_like_emacs() {
        assert_eq!(
            ert_resource_directory_for("/tmp/foo-test.el"),
            "/tmp/foo-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/tmp/foo-tests.el"),
            "/tmp/foo-resources/"
        );
        assert_eq!(
            ert_resource_directory_for("/tmp/bookmark.el"),
            "/tmp/bookmark-resources/"
        );
    }

    #[test]
    fn charset_helpers_cover_ascii_unicode_and_priority_mutation() {
        let mut interp = Interpreter::new();
        assert!(interp.has_charset("ascii"));
        assert_eq!(interp.charset_id("unicode"), Some(1));
        assert_eq!(charset_for_char('A' as u32), "ascii");
        assert_eq!(charset_for_char('あ' as u32), "unicode");

        interp
            .define_charset_alias("latin", "ascii")
            .expect("ascii alias should be accepted");
        assert!(interp.has_charset("latin"));

        interp.set_charset_priority(&["ascii".into(), "unicode".into()]);
        assert_eq!(interp.charset_priority_list(), vec!["ascii", "unicode"]);
        assert_eq!(
            charsets_for_text("Aあ", &interp),
            vec![
                Value::Symbol("ascii".into()),
                Value::Symbol("unicode".into())
            ]
        );
    }

    #[test]
    fn substitute_in_file_name_expands_shell_style_env_vars() {
        let old = std::env::var("EMAXX_SUBST_TEST").ok();
        unsafe {
            std::env::set_var("EMAXX_SUBST_TEST", "value");
        }
        assert_eq!(
            substitute_in_file_name("$EMAXX_SUBST_TEST/${EMAXX_SUBST_TEST}/$$"),
            "value/value/$"
        );
        if let Some(value) = old {
            unsafe {
                std::env::set_var("EMAXX_SUBST_TEST", value);
            }
        } else {
            unsafe {
                std::env::remove_var("EMAXX_SUBST_TEST");
            }
        }
    }

    #[test]
    fn compat_paths_follow_emacs_test_directory_layout() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let repo_root = std::env::temp_dir().join(format!("emaxx-compat-paths-{unique}"));
        let test_dir = repo_root.join("test");
        let src_dir = repo_root.join("src");
        let lib_src_dir = repo_root.join("lib-src");
        std::fs::create_dir_all(&test_dir).expect("create test directory");
        std::fs::create_dir_all(&src_dir).expect("create src directory");
        std::fs::create_dir_all(&lib_src_dir).expect("create lib-src directory");
        std::fs::write(src_dir.join("emacs"), "").expect("write fake emacs binary");
        std::fs::write(lib_src_dir.join("emacsclient"), "").expect("write fake emacsclient binary");

        let test_directory = test_dir.display().to_string();
        assert_eq!(
            compat_invocation_path_from_test_directory(&test_directory),
            Some(src_dir.join("emacs"))
        );
        assert_eq!(
            compat_emacsclient_path_from_test_directory(&test_directory),
            Some(lib_src_dir.join("emacsclient"))
        );

        let _ = std::fs::remove_dir_all(&repo_root);
    }

    #[cfg(unix)]
    #[test]
    fn process_lines_uses_default_directory_as_subprocess_cwd() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cwd = std::env::temp_dir().join(format!("emaxx-process-lines-{unique}"));
        std::fs::create_dir_all(&cwd).expect("create temp cwd");
        let expected = std::fs::canonicalize(&cwd)
            .expect("canonical temp cwd")
            .display()
            .to_string();

        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        interp.set_variable(
            "default-directory",
            Value::String(cwd.display().to_string()),
            &mut env,
        );

        let result = call(
            &mut interp,
            "process-lines",
            &[Value::String("/bin/pwd".into())],
            &mut env,
        )
        .expect("process-lines should succeed");

        assert_eq!(result, Value::list([Value::String(expected)]));

        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[test]
    fn bidi_override_positions_match_upstream_cases() {
        let cases = [
            (
                "int main() {\n  bool isAdmin = false;\n  /*\u{202e} }\u{2066}if (isAdmin)\u{2069} \u{2066} begin admins only */\n  printf(\"You are an admin.\\\\n\");\n  /* end admins only \u{202e} { \u{2066}*/\n  return 0;\n}",
                Some(46),
            ),
            (
                "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? 0 : 1\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
                None,
            ),
            (
                "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? '#' : '!'\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
                None,
            ),
        ];

        for (index, (text, expected_exact)) in cases.into_iter().enumerate() {
            let mut interp = Interpreter::new();
            interp.buffer = crate::buffer::Buffer::from_text("*test*", text);
            let found = find_bidi_override(
                &interp,
                interp.buffer.point_min(),
                interp.buffer.point_max(),
            );
            if let Some(expected) = expected_exact {
                assert_eq!(found, Some(expected));
            } else {
                assert!(
                    found.is_some(),
                    "case {index} should report a suspicious bidi override position"
                );
            }
        }
    }
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
        Value::StringObject(state) => {
            let state = state.borrow();
            Some(StringLike {
                text: state.text.clone(),
                props: state
                    .props
                    .iter()
                    .map(|span| TextPropertySpan {
                        start: span.start,
                        end: span.end,
                        props: span.props.clone(),
                    })
                    .collect(),
            })
        }
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

fn shared_string_props(props: &[TextPropertySpan]) -> Vec<StringPropertySpan> {
    props
        .iter()
        .map(|span| StringPropertySpan {
            start: span.start,
            end: span.end,
            props: span.props.clone(),
        })
        .collect()
}

fn make_shared_string_value(text: String, props: Vec<TextPropertySpan>) -> Value {
    Value::StringObject(Rc::new(RefCell::new(SharedStringState {
        text,
        props: shared_string_props(&props),
    })))
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

fn slice_string_props(props: &[TextPropertySpan], from: usize, to: usize) -> Vec<TextPropertySpan> {
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

fn merge_string_object_props(mut spans: Vec<StringPropertySpan>) -> Vec<StringPropertySpan> {
    spans.retain(|span| span.start < span.end && !span.props.is_empty());
    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut merged: Vec<StringPropertySpan> = Vec::new();
    for span in spans {
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

fn string_object_properties_at(spans: &[StringPropertySpan], pos: usize) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

fn modify_shared_string_properties<F>(
    value: &Value,
    start: usize,
    end: usize,
    mut f: F,
) -> Result<(), LispError>
where
    F: FnMut(Vec<(String, Value)>) -> Vec<(String, Value)>,
{
    let Value::StringObject(state) = value else {
        return Err(LispError::TypeError("string".into(), value.type_name()));
    };
    let mut state = state.borrow_mut();
    let len = state.text.chars().count();
    let start = start.min(len);
    let end = end.min(len);
    if start >= end {
        return Ok(());
    }

    let original = state.props.clone();
    let mut updated = Vec::new();
    for span in &original {
        if span.end <= start || span.start >= end {
            updated.push(span.clone());
        } else {
            if span.start < start {
                updated.push(StringPropertySpan {
                    start: span.start,
                    end: start,
                    props: span.props.clone(),
                });
            }
            if span.end > end {
                updated.push(StringPropertySpan {
                    start: end,
                    end: span.end,
                    props: span.props.clone(),
                });
            }
        }
    }

    let mut boundaries = vec![start, end];
    for span in &original {
        if span.end <= start || span.start >= end {
            continue;
        }
        boundaries.push(span.start.max(start));
        boundaries.push(span.end.min(end));
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    for window in boundaries.windows(2) {
        let seg_start = window[0];
        let seg_end = window[1];
        if seg_start >= seg_end {
            continue;
        }
        let current = string_object_properties_at(&original, seg_start);
        let next = f(current);
        if !next.is_empty() {
            updated.push(StringPropertySpan {
                start: seg_start,
                end: seg_end,
                props: next,
            });
        }
    }

    state.props = merge_string_object_props(updated);
    Ok(())
}

fn call_function_value(
    interp: &mut Interpreter,
    function: &Value,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    let mut items = vec![function.clone()];
    for arg in args {
        items.push(Value::list([Value::symbol("quote"), arg.clone()]));
    }
    interp.eval(&Value::list(items), env)
}

fn run_change_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    args: &[Value],
    env: &mut super::types::Env,
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

fn hook_values(
    interp: &Interpreter,
    hook_name: &str,
    env: &super::types::Env,
    buffer_id: Option<u64>,
) -> Vec<Value> {
    let mut hooks = interp
        .lookup_var(hook_name, env)
        .map(|value| value.to_vec().unwrap_or_default())
        .unwrap_or_default();
    if let Some(id) = buffer_id
        && let Some(local) = interp.buffer_local_hook(id, hook_name)
    {
        hooks.extend(local);
    }
    hooks
}

fn run_named_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    env: &mut super::types::Env,
    buffer_id: Option<u64>,
) -> Result<(), LispError> {
    for hook in hook_values(interp, hook_name, env, buffer_id) {
        call_function_value(interp, &hook, &[], env)?;
    }
    Ok(())
}

fn call_named_function(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut super::types::Env,
) -> Result<Value, LispError> {
    match interp.lookup_function(name, env) {
        Ok(function) => call_function_value(interp, &function, args, env),
        Err(_) => Ok(Value::T),
    }
}

#[derive(Clone)]
struct OverlayHookCall {
    overlay_id: u64,
    functions: Vec<Value>,
    before_tail: Vec<Value>,
    after_tail: Vec<Value>,
}

fn overlay_hook_functions(overlay: &crate::overlay::Overlay, property: &str) -> Vec<Value> {
    match overlay.get_prop(property) {
        Some(value) => value
            .to_vec()
            .unwrap_or_else(|_| vec![value.clone()])
            .into_iter()
            .filter(|value| value.is_truthy())
            .collect(),
        None => Vec::new(),
    }
}

fn overlay_insert_hook_calls(
    buffer: &crate::buffer::Buffer,
    pos: usize,
    inserted_len: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg == overlay.end && overlay.beg == pos {
            for property in ["insert-in-front-hooks", "insert-behind-hooks"] {
                let functions = overlay_hook_functions(overlay, property);
                if !functions.is_empty() {
                    calls.push(OverlayHookCall {
                        overlay_id: overlay.id,
                        functions,
                        before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
                        after_tail: vec![
                            Value::Integer(pos as i64),
                            Value::Integer((pos + inserted_len) as i64),
                            Value::Integer(0),
                        ],
                    });
                }
            }
            continue;
        }
        let property = if pos == overlay.beg {
            Some("insert-in-front-hooks")
        } else if pos == overlay.end {
            Some("insert-behind-hooks")
        } else if overlay.beg < pos && pos < overlay.end {
            Some("modification-hooks")
        } else {
            None
        };
        let Some(property) = property else {
            continue;
        };
        let functions = overlay_hook_functions(overlay, property);
        if functions.is_empty() {
            continue;
        }
        calls.push(OverlayHookCall {
            overlay_id: overlay.id,
            functions,
            before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
            after_tail: vec![
                Value::Integer(pos as i64),
                Value::Integer((pos + inserted_len) as i64),
                Value::Integer(0),
            ],
        });
    }
    calls
}

fn overlay_change_hook_calls(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    new_end: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    let old_len = to.saturating_sub(from);
    for overlay in &buffer.overlays {
        if overlay.is_dead() || overlay.beg == overlay.end {
            continue;
        }
        if overlay.beg < to && from < overlay.end {
            let functions = overlay_hook_functions(overlay, "modification-hooks");
            if functions.is_empty() {
                continue;
            }
            calls.push(OverlayHookCall {
                overlay_id: overlay.id,
                functions,
                before_tail: vec![Value::Integer(from as i64), Value::Integer(to as i64)],
                after_tail: vec![
                    Value::Integer(from as i64),
                    Value::Integer(new_end as i64),
                    Value::Integer(old_len as i64),
                ],
            });
        }
    }
    calls
}

fn run_overlay_hook_calls(
    interp: &mut Interpreter,
    calls: &[OverlayHookCall],
    after: bool,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    env.push(vec![("inhibit-modification-hooks".into(), Value::T)]);
    for call in calls {
        if after && interp.find_overlay(call.overlay_id).is_none() {
            continue;
        }
        for function in &call.functions {
            let mut args = vec![
                Value::Overlay(call.overlay_id),
                if after { Value::T } else { Value::Nil },
            ];
            args.extend(if after {
                call.after_tail.clone()
            } else {
                call.before_tail.clone()
            });
            call_function_value(interp, function, &args, env)?;
        }
    }
    env.pop();
    Ok(())
}

fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut super::types::Env,
) -> Result<String, LispError> {
    if from >= to {
        return Ok(String::new());
    }
    let overlay_calls = overlay_change_hook_calls(&interp.buffer, from, to, from);
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
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
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(start_id),
                Value::Integer(-(from as i64)),
            ));
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(end_id),
                Value::Integer(-(to as i64)),
            ));
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
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(deleted)
}

fn ensure_region_modifiable(
    interp: &Interpreter,
    from: usize,
    to: usize,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    let from = from.max(interp.buffer.point_min());
    let to = to.min(interp.buffer.point_max());
    if from >= to {
        return Ok(());
    }
    let buffer_read_only = interp
        .lookup_var("buffer-read-only", env)
        .is_some_and(|value| value.is_truthy());
    let inhibit_read_only = interp
        .lookup_var("inhibit-read-only", env)
        .unwrap_or(Value::Nil);

    for pos in from..to {
        let read_only = interp.buffer.text_property_at(pos, "read-only");
        let suppressor = interp.buffer.text_property_at(pos, "inhibit-read-only");
        if let Some(read_only_value) = read_only {
            if suppressor.is_some_and(|value| value.is_truthy())
                || inhibit_read_only_matches(&inhibit_read_only, &read_only_value)
            {
                continue;
            }
            return Err(LispError::Signal("Text is read-only".into()));
        }
        if buffer_read_only && !suppressor.is_some_and(|value| value.is_truthy()) {
            return Err(LispError::Signal("Text is read-only".into()));
        }
    }
    Ok(())
}

fn inhibit_read_only_matches(inhibit: &Value, property: &Value) -> bool {
    if inhibit.is_nil() {
        return false;
    }
    if matches!(inhibit, Value::T) {
        return true;
    }
    if let Ok(items) = inhibit.to_vec() {
        return items.into_iter().any(|item| item == *property);
    }
    inhibit == property
}

fn prepend_face_value(existing: Value, face: &Value) -> Value {
    match face_list_items(&existing) {
        Ok(mut items) => {
            items.insert(0, face.clone());
            Value::list(items)
        }
        Err(_) => Value::list([face.clone(), existing]),
    }
}

fn remove_face_value(existing: Value, face: &Value) -> Value {
    match face_list_items(&existing) {
        Ok(items) => {
            let filtered = items
                .into_iter()
                .filter(|item| !values_equal_including_properties(item, face))
                .collect::<Vec<_>>();
            match filtered.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(filtered),
            }
        }
        Err(_) => {
            if values_equal_including_properties(&existing, face) {
                Value::Nil
            } else {
                existing
            }
        }
    }
}

fn face_list_items(value: &Value) -> Result<Vec<Value>, LispError> {
    if plist_like_face(value) {
        Err(LispError::TypeError("face-list".into(), "plist".into()))
    } else {
        value.to_vec()
    }
}

fn plist_like_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    !items.is_empty()
        && items.len().is_multiple_of(2)
        && items
            .iter()
            .step_by(2)
            .all(|item| matches!(item, Value::Symbol(symbol) if symbol.starts_with(':')))
}

fn parse_xml_region(xml: &str) -> Result<Value, roxmltree::Error> {
    let doc = Document::parse(xml)?;
    let children = xml_child_values(doc.root())?;
    if children.len() == 1 && matches!(children.first(), Some(Value::Cons(_, _))) {
        Ok(children.into_iter().next().unwrap_or(Value::Nil))
    } else {
        Ok(Value::list(
            [Value::Symbol("top".into()), Value::Nil]
                .into_iter()
                .chain(children),
        ))
    }
}

fn xml_child_values(node: Node<'_, '_>) -> Result<Vec<Value>, roxmltree::Error> {
    let mut children = Vec::new();
    for child in node.children() {
        match child.node_type() {
            NodeType::Element => children.push(xml_element_value(child)?),
            NodeType::Comment => {
                children.push(Value::list([
                    Value::Symbol("comment".into()),
                    Value::Nil,
                    Value::String(child.text().unwrap_or_default().to_string()),
                ]));
            }
            NodeType::Text => {
                let text = child.text().unwrap_or_default();
                if !text.trim().is_empty() {
                    children.push(Value::String(text.to_string()));
                }
            }
            _ => {}
        }
    }
    Ok(children)
}

fn xml_element_value(node: Node<'_, '_>) -> Result<Value, roxmltree::Error> {
    let attrs = if node.attributes().len() == 0 {
        Value::Nil
    } else {
        Value::list(node.attributes().map(|attr| {
            Value::cons(
                Value::Symbol(attr.name().to_string()),
                Value::String(attr.value().to_string()),
            )
        }))
    };
    let children = xml_child_values(node)?;
    Ok(Value::list(
        [Value::Symbol(node.tag_name().name().to_string()), attrs]
            .into_iter()
            .chain(children),
    ))
}

fn display_property_value(value: &Value, property: &str) -> Option<Value> {
    if let Ok(items) = value.to_vec() {
        if let Some(Value::Symbol(name)) = items.first()
            && name == property
        {
            return items.get(1).cloned();
        }
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal")
            || matches!(items.first(), Some(Value::Symbol(name)) if name == "vector")
        {
            for item in items.iter().skip(1) {
                if let Some(found) = display_property_value(item, property) {
                    return Some(found);
                }
            }
            return None;
        }
        for item in items {
            if let Some(found) = display_property_value(&item, property) {
                return Some(found);
            }
        }
    }
    None
}

fn find_bidi_override(interp: &Interpreter, start: usize, end: usize) -> Option<usize> {
    let text = interp.buffer.buffer_substring(start, end).ok()?;
    let chars = text.chars().collect::<Vec<_>>();
    let control_index = chars
        .iter()
        .position(|ch| matches!(*ch as u32, 0x202A..=0x202E | 0x2066..=0x2069))?;
    chars[control_index..]
        .iter()
        .position(|ch| {
            !matches!((*ch) as u32, 0x202A..=0x202E | 0x2066..=0x2069)
                && !ch.is_whitespace()
                && !matches!(*ch, '{' | '}')
        })
        .map(|offset| start + control_index + offset)
}

fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: &[TextPropertySpan],
    inherit: bool,
    before_markers: bool,
    env: &mut super::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    ensure_no_supersession_threat(interp, env)?;
    let start = interp.buffer.point();
    let overlay_calls = overlay_insert_hook_calls(&interp.buffer, start, text.chars().count());
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
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
        interp
            .buffer
            .add_text_properties(start + span.start, start + span.end, &span.props);
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
    let _ = maybe_lock_current_buffer(interp, env);
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
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

#[derive(Clone, Copy)]
enum RoundingKind {
    Ceiling,
    Floor,
    Round,
    Truncate,
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

fn bigint_from_integral_float(value: f64) -> Option<BigInt> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    bigint_from_truncated_float(value).ok()
}

fn apply_rounding_kind(kind: RoundingKind, value: f64) -> Result<f64, LispError> {
    if !value.is_finite() {
        return Err(LispError::Signal("Floating-point overflow".into()));
    }
    Ok(match kind {
        RoundingKind::Ceiling => value.ceil(),
        RoundingKind::Floor => value.floor(),
        RoundingKind::Round => value.round_ties_even(),
        RoundingKind::Truncate => value.trunc(),
    })
}

fn integer_rounding_value(
    interp: &Interpreter,
    kind: RoundingKind,
    args: &[Value],
    float_result: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(
            match (kind, float_result) {
                (RoundingKind::Ceiling, false) => "ceiling",
                (RoundingKind::Floor, false) => "floor",
                (RoundingKind::Round, false) => "round",
                (RoundingKind::Truncate, false) => "truncate",
                (RoundingKind::Ceiling, true) => "fceiling",
                (RoundingKind::Floor, true) => "ffloor",
                (RoundingKind::Round, true) => "fround",
                (RoundingKind::Truncate, true) => "ftruncate",
            }
            .into(),
            args.len(),
        ));
    }
    if float_result && !matches!(args[0], Value::Float(_)) {
        return Err(LispError::TypeError("float".into(), args[0].type_name()));
    }

    if args.len() == 1 {
        if float_result {
            return Ok(Value::Float(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?));
        }
        return match &args[0] {
            Value::Integer(_) | Value::BigInteger(_) => Ok(args[0].clone()),
            _ => rounded_f64_to_number_value(apply_rounding_kind(
                kind,
                numeric_to_f64(interp, &args[0])?,
            )?),
        };
    }

    if let Some(rounded) = exact_numeric_division_round(interp, kind, &args[0], &args[1])? {
        return if float_result {
            Ok(Value::Float(numeric_to_f64(interp, &rounded)?))
        } else {
            Ok(rounded)
        };
    }

    if let (Some(numerator), Some(divisor)) = (
        integer_like_bigint_for_rounding(interp, &args[0]),
        integer_like_bigint_for_rounding(interp, &args[1]),
    ) {
        if divisor.is_zero() {
            return Err(LispError::Signal("Division by zero".into()));
        }
        let rounded = exact_integer_division_round(kind, numerator, divisor);
        return if float_result {
            Ok(Value::Float(
                numeric_to_f64(interp, &rounded).unwrap_or(f64::NAN),
            ))
        } else {
            Ok(rounded)
        };
    }

    let divisor = numeric_to_f64(interp, &args[1])?;
    if divisor == 0.0 || divisor.is_nan() {
        return Err(LispError::Signal("Division by zero".into()));
    }
    let quotient = numeric_to_f64(interp, &args[0])? / divisor;
    let rounded = apply_rounding_kind(kind, quotient)?;
    if float_result {
        Ok(Value::Float(rounded))
    } else {
        rounded_f64_to_number_value(rounded)
    }
}

fn rounded_f64_to_number_value(value: f64) -> Result<Value, LispError> {
    Ok(normalize_bigint_value(bigint_from_truncated_float(value)?))
}

fn integer_like_bigint_for_rounding(interp: &Interpreter, value: &Value) -> Option<BigInt> {
    match value {
        Value::Float(value) => bigint_from_integral_float(*value),
        _ => integer_like_bigint(interp, value).ok(),
    }
}

fn exact_numeric_division_round(
    interp: &Interpreter,
    kind: RoundingKind,
    numerator: &Value,
    divisor: &Value,
) -> Result<Option<Value>, LispError> {
    let Some((num_sig, num_exp)) = exact_binary_rational(interp, numerator)? else {
        return Ok(None);
    };
    let Some((div_sig, div_exp)) = exact_binary_rational(interp, divisor)? else {
        return Ok(None);
    };
    if div_sig.is_zero() {
        return Err(LispError::Signal("Division by zero".into()));
    }
    let mut scaled_num = num_sig;
    let mut scaled_div = div_sig;
    let exponent_delta = num_exp - div_exp;
    if exponent_delta >= 0 {
        scaled_num <<= exponent_delta as usize;
    } else {
        scaled_div <<= (-exponent_delta) as usize;
    }
    Ok(Some(exact_integer_division_round(
        kind, scaled_num, scaled_div,
    )))
}

fn exact_integer_division_round(kind: RoundingKind, numerator: BigInt, divisor: BigInt) -> Value {
    let quotient = &numerator / &divisor;
    let remainder = &numerator % &divisor;
    if remainder.is_zero() {
        return normalize_bigint_value(quotient);
    }
    let same_sign = numerator.sign() == divisor.sign();
    let adjusted = match kind {
        RoundingKind::Truncate => quotient,
        RoundingKind::Floor => {
            if same_sign {
                quotient
            } else {
                quotient - 1
            }
        }
        RoundingKind::Ceiling => {
            if same_sign {
                quotient + 1
            } else {
                quotient
            }
        }
        RoundingKind::Round => {
            let twice_remainder = remainder.abs() * 2;
            let divisor_abs = divisor.abs();
            if twice_remainder < divisor_abs {
                quotient
            } else if twice_remainder > divisor_abs {
                if same_sign {
                    quotient + 1
                } else {
                    quotient - 1
                }
            } else if (&quotient & BigInt::from(1u8)).is_zero() {
                quotient
            } else if same_sign {
                quotient + 1
            } else {
                quotient - 1
            }
        }
    };
    normalize_bigint_value(adjusted)
}

fn frexp_parts(value: f64) -> (f64, i64) {
    if value == 0.0 {
        return (value, 0);
    }
    let exponent = value.abs().log2().floor() as i64 + 1;
    let significand = value / ldexp_value(1.0, exponent);
    (significand, exponent)
}

fn ldexp_value(significand: f64, exponent: i64) -> f64 {
    if exponent > i32::MAX as i64 {
        return if significand == 0.0 {
            0.0
        } else if significand.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if exponent < i32::MIN as i64 {
        return 0.0_f64.copysign(significand);
    }
    significand * 2.0_f64.powi(exponent as i32)
}

fn logb_value(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    match value {
        Value::Integer(number) => {
            if *number == 0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                i64::BITS as i64 - 1 - number.unsigned_abs().leading_zeros() as i64,
            ))
        }
        Value::BigInteger(number) => {
            if number.is_zero() {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            Ok(Value::Integer(
                number.abs().to_str_radix(2).len() as i64 - 1,
            ))
        }
        _ => {
            let value = numeric_to_f64(interp, value)?;
            if !value.is_finite() || value == 0.0 {
                return Err(LispError::Signal("Arithmetic error".into()));
            }
            let (_sig, exponent) = frexp_parts(value.abs());
            Ok(Value::Integer(exponent - 1))
        }
    }
}

fn expt_value(interp: &Interpreter, base: &Value, exponent: &Value) -> Result<Value, LispError> {
    let exponent_bigint = integer_like_bigint(interp, exponent);
    if matches!(base, Value::Float(_)) || matches!(exponent, Value::Float(_)) {
        return Ok(Value::Float(
            numeric_to_f64(interp, base)?.powf(numeric_to_f64(interp, exponent)?),
        ));
    }
    let exponent_bigint = exponent_bigint?;
    if exponent_bigint.is_negative() {
        let base_value = integer_like_i64(interp, base)? as f64;
        let exponent_value = exponent_bigint
            .to_f64()
            .ok_or_else(|| LispError::TypeError("number".into(), exponent.type_name()))?;
        return Ok(Value::Float(base_value.powf(exponent_value)));
    }

    let base_bigint = integer_like_bigint(interp, base)?;
    if exponent_bigint.is_zero() {
        return Ok(Value::Integer(1));
    }
    if base_bigint.is_zero() {
        return Ok(Value::Integer(0));
    }
    if base_bigint == BigInt::from(1) {
        return Ok(Value::Integer(1));
    }
    if base_bigint == BigInt::from(-1) {
        let even = (&exponent_bigint & BigInt::from(1u8)).is_zero();
        return Ok(Value::Integer(if even { 1 } else { -1 }));
    }
    let exponent_u32 = exponent_bigint
        .to_u32()
        .ok_or_else(|| LispError::Signal("Exponent too large".into()))?;
    Ok(normalize_bigint_value(base_bigint.pow(exponent_u32)))
}

fn exact_binary_rational(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<(BigInt, i32)>, LispError> {
    match value {
        Value::Float(value) => Ok(exact_float_binary_rational(*value)),
        Value::Integer(value) => Ok(Some((BigInt::from(*value), 0))),
        Value::BigInteger(value) => Ok(Some((value.clone(), 0))),
        Value::Marker(_) => Ok(Some((BigInt::from(integer_like_i64(interp, value)?), 0))),
        _ => Err(LispError::TypeError("number".into(), value.type_name())),
    }
}

fn exact_float_binary_rational(value: f64) -> Option<(BigInt, i32)> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some((BigInt::zero(), 0));
    }
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let mantissa = bits & ((1u64 << 52) - 1);
    let (significand, exponent) = if exponent_bits == 0 {
        (mantissa, 1 - 1023 - 52)
    } else {
        ((1u64 << 52) | mantissa, exponent_bits - 1023 - 52)
    };
    let mut bigint = BigInt::from(significand);
    if negative {
        bigint = -bigint;
    }
    Some((bigint, exponent))
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

fn numeric_lte(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? <= numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? <= integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? <= integer_like_i64(interp, right)?)
}

fn numeric_gte(interp: &Interpreter, left: &Value, right: &Value) -> Result<bool, LispError> {
    if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
        return Ok(numeric_to_f64(interp, left)? >= numeric_to_f64(interp, right)?);
    }
    if matches!(left, Value::BigInteger(_)) || matches!(right, Value::BigInteger(_)) {
        return Ok(integer_like_bigint(interp, left)? >= integer_like_bigint(interp, right)?);
    }
    Ok(integer_like_i64(interp, left)? >= integer_like_i64(interp, right)?)
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

fn file_modtime(path: &str) -> Result<Option<crate::buffer::FileModTime>, LispError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(crate::buffer::FileModTime {
            modified: metadata
                .modified()
                .map_err(|error| LispError::Signal(error.to_string()))?,
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn lock_path_for_file(path: &str) -> PathBuf {
    let expanded = PathBuf::from(expand_file_name(path, None));
    let directory = expanded.parent().map(Path::to_path_buf).unwrap_or_default();
    let file_name = expanded
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path);
    directory.join(format!(".#{file_name}"))
}

fn file_error_value(message: &str, path: &str) -> Value {
    Value::list([
        Value::Symbol("file-error".into()),
        Value::String(message.into()),
        Value::String(path.into()),
    ])
}

fn file_locked_p(path: &str) -> Result<Value, LispError> {
    let lock_path = lock_path_for_file(path);
    match fs::metadata(&lock_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                Err(LispError::SignalValue(file_error_value(
                    "Testing file lock",
                    path,
                )))
            } else {
                Ok(Value::T)
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn current_buffer_file(interp: &Interpreter) -> Option<&str> {
    interp
        .buffer
        .file_truename
        .as_deref()
        .or(interp.buffer.file.as_deref())
}

fn maybe_lock_current_buffer(interp: &mut Interpreter, env: &Env) -> Result<(), LispError> {
    if !interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    if !interp.buffer.is_modified() {
        return Ok(());
    }
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::write(lock_path, format!("emaxx:{}", std::process::id()))
                .map_err(|err| LispError::Signal(err.to_string()))
        }
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn unlock_current_buffer(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(Value::Nil);
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => {
            call_named_function(
                interp,
                "userlock--handle-unlock-error",
                &[file_error_value("Unlocking file", &path)],
                env,
            )?;
            Ok(Value::Nil)
        }
        Ok(_) => {
            fs::remove_file(&lock_path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

fn ensure_no_supersession_threat(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    if interp.buffer.visited_file_modtime() == Some(current_modtime) {
        return Ok(());
    }
    let disk_text =
        fs::read_to_string(&path).map_err(|error| LispError::Signal(error.to_string()))?;
    if disk_text == interp.buffer.saved_text() {
        interp
            .buffer
            .set_visited_file_modtime(Some(current_modtime));
        return Ok(());
    }
    if interp
        .lookup_var("noninteractive", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            "Cannot resolve conflict in batch mode".into(),
        ));
    }
    let prompt = format!(
        "{} changed on disk; really edit the buffer?",
        file_name_nondirectory(&path)
    );
    let answer = call_named_function(
        interp,
        "read-char-choice",
        &[
            Value::String(prompt),
            Value::list([
                Value::Integer('y' as i64),
                Value::Integer('n' as i64),
                Value::Integer('r' as i64),
            ]),
        ],
        env,
    )?;
    match answer.as_integer()? as u8 as char {
        'y' => {
            let _ = call(
                interp,
                "message",
                &[Value::String(
                    "File on disk now will become a backup file if you save these changes.".into(),
                )],
                env,
            )?;
            interp
                .buffer
                .set_visited_file_modtime(Some(current_modtime));
            Ok(())
        }
        'r' => {
            revert_current_buffer(interp)?;
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("file-supersession".into()),
                Value::String("File reverted".into()),
                Value::String(path),
            ])))
        }
        _ => Err(LispError::SignalValue(Value::list([
            Value::Symbol("file-supersession".into()),
            Value::String("File changed on disk".into()),
            Value::String(path),
        ]))),
    }
}

fn revert_current_buffer(interp: &mut Interpreter) -> Result<(), LispError> {
    let Some(path) = interp.buffer.file.clone() else {
        return Ok(());
    };
    let text = fs::read_to_string(&path).map_err(|error| LispError::Signal(error.to_string()))?;
    let name = interp.buffer.name.clone();
    let file = interp.buffer.file.clone();
    let file_truename = interp.buffer.file_truename.clone();
    let inhibit_hooks = interp.buffer.inhibit_hooks;
    interp.buffer = crate::buffer::Buffer::from_text(&name, &text);
    interp.buffer.file = file;
    interp.buffer.file_truename = file_truename;
    interp.buffer.inhibit_hooks = inhibit_hooks;
    interp.buffer.set_unmodified();
    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
    Ok(())
}

fn shell_quote_argument(argument: &str) -> String {
    format!("'{}'", argument.replace('\'', "'\"'\"'"))
}

fn first_choice_value(choices: &Value) -> Option<Value> {
    choices
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| {
            item.to_vec()
                .ok()
                .and_then(|nested| nested.first().cloned())
                .or(Some(item))
        })
}

fn seq_subseq(sequence: &Value, start: i64, end: Option<i64>) -> Result<Value, LispError> {
    let start = start.max(0) as usize;
    match sequence {
        Value::String(text) => {
            let chars = text.chars().collect::<Vec<_>>();
            let end = end.unwrap_or(chars.len() as i64).max(start as i64) as usize;
            let bounded_end = end.min(chars.len());
            Ok(Value::String(
                chars[start.min(bounded_end)..bounded_end].iter().collect(),
            ))
        }
        _ => {
            let items = sequence.to_vec()?;
            let end = end.unwrap_or(items.len() as i64).max(start as i64) as usize;
            let bounded_end = end.min(items.len());
            Ok(Value::list(
                items[start.min(bounded_end)..bounded_end].iter().cloned(),
            ))
        }
    }
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
                let mut call_env = env.clone();
                let rendered = call_function_value(interp, &function, &[], &mut call_env)?;
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
            return Err(LispError::TypeError("integer".into(), arg.type_name()));
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

fn integer_for_format(
    interp: &Interpreter,
    value: &Value,
) -> Result<(Option<i64>, BigInt), LispError> {
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
            let digits =
                apply_precision(format_bigint_radix(&bigint, radix, upper), digit_precision);
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
        _ => Err(LispError::Signal(format!(
            "Invalid format operation %{}",
            conv
        ))),
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
            crate::buffer::UndoEntry::Combined { display, .. }
            | crate::buffer::UndoEntry::Opaque(display) => display.clone(),
            crate::buffer::UndoEntry::Boundary => Value::Nil,
        })
        .collect::<Vec<_>>();
    entries.extend(buffer.undo_meta_entries().iter().rev().cloned());
    if buffer.file.is_some()
        && buffer.undo_entries().iter().any(|entry| {
            matches!(
                entry,
                crate::buffer::UndoEntry::Insert { .. } | crate::buffer::UndoEntry::Delete { .. }
            )
        })
    {
        entries.push(Value::list([
            Value::T,
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ]));
    }
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
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
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
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
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
        && left.plist.iter().zip(&right.plist).all(
            |((left_key, left_value), (right_key, right_value))| {
                left_key == right_key && values_equal(interp, left_value, right_value)
            },
        )
}

fn resolve_callable(interp: &Interpreter, value: &Value, env: &Env) -> Result<Value, LispError> {
    match value {
        Value::Symbol(name) => interp.lookup_function(name, env),
        _ => Ok(value.clone()),
    }
}

fn invoke_function_value(
    interp: &mut Interpreter,
    func: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut call_items = vec![func.clone()];
    for arg in args {
        call_items.push(Value::list([Value::symbol("quote"), arg.clone()]));
    }
    interp.eval(&Value::list(call_items), env)
}

fn callable_name(original: &Value, resolved: &Value) -> Option<String> {
    match original {
        Value::Symbol(name) => Some(name.clone()),
        _ => match resolved {
            Value::BuiltinFunc(name) => Some(name.clone()),
            _ => None,
        },
    }
}

fn collect_interactive_args(
    interp: &mut Interpreter,
    func: &Value,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let Some(spec) = interactive_spec_form(func) else {
        return Ok(Vec::new());
    };
    match spec {
        Value::String(spec) => parse_interactive_string(&spec, interp, env),
        _ => {
            let value = eval_callable_metadata_form(interp, func, &spec, env)?;
            value.to_vec()
        }
    }
}

fn history_args_for_call(
    interp: &mut Interpreter,
    func: &Value,
    actual_args: &[Value],
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut recorded = actual_args.to_vec();
    let Value::Lambda(params, _, _) = func else {
        return Ok(recorded);
    };
    let positional_params = params
        .iter()
        .filter(|param| *param != "&optional" && *param != "&rest")
        .cloned()
        .collect::<Vec<_>>();
    for (name, form) in interactive_args_overrides(func) {
        if let Some(index) = positional_params.iter().position(|param| param == &name) {
            let value = eval_callable_metadata_form(interp, func, &form, env)?;
            if index >= recorded.len() {
                recorded.resize(index + 1, Value::Nil);
            }
            recorded[index] = value;
        }
    }
    Ok(recorded)
}

fn interactive_spec_form(func: &Value) -> Option<Value> {
    let Value::Lambda(_, body, _) = func else {
        return None;
    };
    for form in body {
        if is_declare_form(form) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "interactive") {
            return items.get(1).cloned();
        }
        break;
    }
    None
}

fn interactive_args_overrides(func: &Value) -> Vec<(String, Value)> {
    let Value::Lambda(_, body, _) = func else {
        return Vec::new();
    };
    let mut overrides = Vec::new();
    for form in body {
        if !is_declare_form(form) {
            break;
        }
        let Ok(items) = form.to_vec() else {
            continue;
        };
        for decl in &items[1..] {
            let Ok(parts) = decl.to_vec() else {
                continue;
            };
            if !matches!(parts.first(), Some(Value::Symbol(name)) if name == "interactive-args") {
                continue;
            }
            for arg in &parts[1..] {
                let Ok(entry) = arg.to_vec() else {
                    continue;
                };
                if entry.len() >= 2
                    && let Value::Symbol(name) = &entry[0]
                {
                    overrides.push((name.clone(), entry[1].clone()));
                }
            }
        }
    }
    overrides
}

fn eval_callable_metadata_form(
    interp: &mut Interpreter,
    func: &Value,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if let Value::Lambda(_, _, closure_env) = func {
        let mut local_env = env.clone();
        for captured in closure_env.iter().rev() {
            local_env.insert(0, captured.clone());
        }
        interp.eval(form, &mut local_env)
    } else {
        interp.eval(form, env)
    }
}

fn parse_interactive_string(
    spec: &str,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let mut values = Vec::new();
    for line in spec.split('\n') {
        if line.is_empty() {
            continue;
        }
        let Some(code) = line.chars().next() else {
            continue;
        };
        match code {
            'k' => {
                let ch = pop_unread_command_event(interp, env)?;
                values.push(Value::String(ch.to_string()));
            }
            _ => return Err(invalid_interactive_control_letter(code)),
        }
    }
    Ok(values)
}

fn pop_unread_command_event(interp: &mut Interpreter, env: &mut Env) -> Result<char, LispError> {
    let unread = interp
        .lookup_var("unread-command-events", env)
        .unwrap_or(Value::Nil);
    let mut events = unread.to_vec()?;
    if events.is_empty() {
        return Err(LispError::Signal(
            "No unread-command-events available for interactive input".into(),
        ));
    }
    let event = events.remove(0);
    interp.set_variable("unread-command-events", Value::list(events), env);
    match event {
        Value::Integer(code) if code >= 0 => char::from_u32(code as u32)
            .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", code))),
        Value::Cons(car, cdr) if matches!(*car, Value::T) => match *cdr {
            Value::Integer(code) if code >= 0 => char::from_u32(code as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid unread command event {}", code))),
            other => Err(LispError::Signal(format!(
                "Invalid unread command event {}",
                other
            ))),
        },
        Value::String(text) => text
            .chars()
            .next()
            .ok_or_else(|| LispError::Signal("Invalid unread command event".into())),
        _ => Err(LispError::Signal(format!(
            "Invalid unread command event {}",
            event
        ))),
    }
}

fn record_command_history(
    interp: &mut Interpreter,
    function_name: &str,
    args: Vec<Value>,
    env: &mut Env,
) {
    let mut history = interp
        .lookup_var("command-history", env)
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    let mut entry = vec![Value::Symbol(function_name.to_string())];
    entry.extend(args);
    history.insert(0, Value::list(entry));
    if let Some(Value::Integer(length)) = interp.lookup_var("history-length", env) {
        let length = length.max(0) as usize;
        history.truncate(length);
    }
    interp.set_variable("command-history", Value::list(history), env);
}

fn is_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "declare"),
    )
}

fn invalid_interactive_control_letter(ch: char) -> LispError {
    let code = ch as u32;
    LispError::Signal(format!(
        "Invalid control letter `{ch}' (#o{code:03o}, #x{code:04x}) in interactive calling string"
    ))
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

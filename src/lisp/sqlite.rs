//! sqlite.c: the SQLite interface, on SQLite's C API as GNU calls it.
//!
//! The library is the SQLite rusqlite bundles, but not rusqlite's
//! connection layer: its `Connection::open' gives every connection a
//! five-second busy timeout and extended result codes, and its statements
//! a cache, none of which GNU's sqlite3_open_v2 sets up.  A connection GNU
//! opens has no busy handler, so a locked database is `sqlite-locked-error'
//! at once.

use std::ffi::{CStr, c_char, c_int};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use num_traits::ToPrimitive;
use rusqlite::ffi;

use super::eval::Interpreter;
use super::primitives::{
    StringLike, bytes_to_shared_unibyte_value, decode_coding_text_recording, internal_string_bytes,
    push_emacs_multibyte_char, string_like, string_storage_error, string_text, vector_items,
};
use super::types::{Env, Kind, LispError, StringObjectRef, Value};

/// Image-template clone semantics: a template interpreter holds no open
/// databases; cloning a live handle is a caller bug.
impl Clone for SqliteHandleState {
    fn clone(&self) -> Self {
        panic!("image-template clone with a live sqlite handle");
    }
}

/// struct Lisp_Sqlite, by IS_STATEMENT.
pub enum SqliteHandleState {
    Database(SqliteDatabaseState),
    Set(SqliteSetState),
}

/// A database: DB the connection, null once `sqlite-close' has run.
pub struct SqliteDatabaseState {
    db: *mut ffi::sqlite3,
    name: Vec<u8>,
}

/// A statement, `sqlite-select''s set: DB its database's connection, null
/// once `sqlite-finalize' has finalized STMT; EOF once a step found no
/// more rows.
pub struct SqliteSetState {
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
    eof: bool,
    name: Vec<u8>,
}

// SAFETY: `sqlite-open' opens every connection with SQLITE_OPEN_FULLMUTEX,
// SQLite's serialized mode, in which a connection and its statements may be
// used from any thread.
unsafe impl Send for SqliteDatabaseState {}
unsafe impl Send for SqliteSetState {}

/// sqlite_free: the collector's finalizer closes a database...
impl Drop for SqliteDatabaseState {
    fn drop(&mut self) {
        if !self.db.is_null() {
            // SAFETY: an open connection this object owns.  With statements
            // still unfinalized sqlite3_close leaves it open, as GNU's does.
            unsafe { ffi::sqlite3_close(self.db) };
        }
    }
}

/// ... and finalizes a statement `sqlite-finalize' has not.
impl Drop for SqliteSetState {
    fn drop(&mut self) {
        if !self.db.is_null() {
            // SAFETY: a statement this object owns and nothing finalized;
            // finalizing a null one is a no-op.
            unsafe { ffi::sqlite3_finalize(self.stmt) };
        }
    }
}

define_dispatch!(
    pub fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "sqlite-open" => sqlite_open(interp, args, env),
            "sqlite-close" => sqlite_close(interp, args),
            "sqlite-execute" => sqlite_execute(interp, args, env),
            "sqlite-select" => sqlite_select(interp, args, env),
            "sqlite-execute-batch" => sqlite_execute_batch(interp, args),
            "sqlite-transaction" => sqlite_transaction(interp, args),
            "sqlite-commit" => sqlite_commit(interp, args),
            "sqlite-rollback" => sqlite_rollback(interp, args),
            "sqlite-load-extension" => sqlite_load_extension(interp, args, env),
            "sqlite-next" => sqlite_next(interp, args, env),
            "sqlite-columns" => sqlite_columns(interp, args, env),
            "sqlite-more-p" => sqlite_more_p(interp, args),
            "sqlite-finalize" => sqlite_finalize(interp, args),
            "sqlite-version" => sqlite_version(interp, args, env),
            "sqlitep" => sqlitep(interp, args),
            "sqlite-pragma" => sqlite_pragma(interp, args),
            "sqlite-available-p" => sqlite_available_p(args),
        }
    }
);

fn need_args(name: &str, args: &[Value], min: usize, max: usize) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

/// xsignal1 (Qsqlite_error, build_string (MESSAGE)).
fn sqlite_error(message: &str) -> LispError {
    sqlite_signal("sqlite-error", Value::string(message))
}

/// xsignal1 (CONDITION, DATA).
fn sqlite_signal(condition: &str, data: Value) -> LispError {
    LispError::SignalValue(Value::list([Value::symbol(condition), data]))
}

/// build_string: a C string's bytes, multibyte when they are multibyte
/// text (make_string).
fn build_string(interp: &Interpreter, env: &Env, bytes: &[u8]) -> Result<Value, LispError> {
    StringObjectRef::from_c_bytes(bytes)
        .map(Value::StringObject)
        .map_err(|error| string_storage_error(interp, env, error))
}

/// The bytes of a C string SQLite returned; none for a null pointer.
///
/// # Safety
/// POINTER is null or a NUL-terminated string that outlives the result.
unsafe fn c_bytes<'a>(pointer: *const c_char) -> &'a [u8] {
    if pointer.is_null() {
        b""
    } else {
        // SAFETY: as the caller promises.
        unsafe { CStr::from_ptr(pointer) }.to_bytes()
    }
}

/// sqlite3_errmsg (DB) as build_string makes it.
fn errmsg(interp: &Interpreter, env: &Env, db: *mut ffi::sqlite3) -> Result<Value, LispError> {
    // SAFETY: DB is an open connection; its message lives until its next
    // call, and is copied here.
    build_string(interp, env, unsafe { c_bytes(ffi::sqlite3_errmsg(db)) })
}

/// BYTES as a C string: up to their first NUL, terminated.
fn c_string(mut bytes: Vec<u8>) -> Vec<u8> {
    if let Some(end) = bytes.iter().position(|&byte| byte == 0) {
        bytes.truncate(end);
    }
    bytes.push(0);
    bytes
}

/// What check_sqlite found: the object's connection and statement.
#[derive(Clone, Copy)]
struct Checked {
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
}

/// check_sqlite: CHECK_SQLITE, then OBJECT the kind IS_STATEMENT asks for
/// and not closed.
fn check_sqlite(
    interp: &Interpreter,
    object: &Value,
    is_statement: bool,
) -> Result<Checked, LispError> {
    let state = match object.kind() {
        Kind::Record(id) => interp.find_sqlite_handle(id.id),
        _ => None,
    }
    .ok_or_else(|| LispError::WrongTypeArgument("sqlitep".into(), *object))?;
    let (checked, statement) = match state {
        SqliteHandleState::Database(state) => (
            Checked {
                db: state.db,
                stmt: ptr::null_mut(),
            },
            false,
        ),
        SqliteHandleState::Set(state) => (
            Checked {
                db: state.db,
                stmt: state.stmt,
            },
            true,
        ),
    };
    if is_statement && !statement {
        return Err(sqlite_error("Invalid set object"));
    } else if !is_statement && statement {
        return Err(sqlite_error("Invalid database object"));
    }
    if !is_statement && checked.db.is_null() {
        Err(sqlite_error("Database closed"))
    } else if is_statement && checked.db.is_null() {
        Err(sqlite_error("Statement closed"))
    } else {
        Ok(checked)
    }
}

fn set_state<'a>(interp: &'a mut Interpreter, object: &Value) -> Option<&'a mut SqliteSetState> {
    let Kind::Record(id) = object.kind() else {
        return None;
    };
    match interp.find_sqlite_handle_mut(id.id) {
        Some(SqliteHandleState::Set(state)) => Some(state),
        _ => None,
    }
}

/// CHECK_STRING.
fn check_string(value: &Value) -> Result<StringLike, LispError> {
    string_like(value).ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *value))
}

/// encode_string: a multibyte string as encode_string_utf_8 with
/// HANDLE-8-BIT and HANDLE-OVER-UNI t encodes it (a raw byte as itself, a
/// character beyond Unicode in its internal form), a unibyte one as its
/// bytes.
fn encode_string(string: &StringLike) -> Result<Vec<u8>, LispError> {
    if !string.multibyte {
        return internal_string_bytes(string);
    }
    let mut bytes = Vec::with_capacity(string.text.len());
    for code in string.character_codes_iter() {
        let code =
            u32::try_from(code).map_err(|_| LispError::Signal("Invalid character".into()))?;
        if (0x3f_ff80..=0x3f_ffff).contains(&code) {
            bytes.push((code - 0x3f_ff00) as u8);
        } else {
            push_emacs_multibyte_char(&mut bytes, code)?;
        }
    }
    Ok(bytes)
}

static DB_COUNT: AtomicU64 = AtomicU64::new(0);

fn sqlite_open(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-open", args, 0, 1)?;
    let file = args.first().copied().unwrap_or(Value::Nil);
    let mut flags = ffi::SQLITE_OPEN_CREATE
        | ffi::SQLITE_OPEN_READWRITE
        | ffi::SQLITE_OPEN_FULLMUTEX
        | ffi::SQLITE_OPEN_URI;
    let name = if !file.is_nil() {
        // ENCODE_FILE (Fexpand_file_name (file, Qnil)).
        let expanded =
            super::primitives::call(interp, "expand-file-name", std::slice::from_ref(&file), env)?;
        crate::lisp::primitives::encode_file_name(interp, env, &string_text(&expanded)?)?
    } else {
        // In-memory databases have to have different names to refer to
        // different databases.
        flags |= ffi::SQLITE_OPEN_MEMORY;
        format!(":memory:{}", DB_COUNT.fetch_add(1, Ordering::Relaxed) + 1).into_bytes()
    };
    let path = c_string(name.clone());
    let mut sdb = ptr::null_mut();
    // SAFETY: PATH is NUL-terminated; SDB receives the new connection.
    let ret = unsafe { ffi::sqlite3_open_v2(path.as_ptr().cast(), &mut sdb, flags, ptr::null()) };
    if ret != ffi::SQLITE_OK {
        // The handle SQLite allocates even when the open fails.
        // SAFETY: SDB is null or that handle, closed once.
        unsafe { ffi::sqlite3_close(sdb) };
        return Ok(Value::Nil);
    }
    create_sqlite_handle(
        interp,
        SqliteHandleState::Database(SqliteDatabaseState { db: sdb, name }),
    )
}

fn sqlite_close(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-close", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    // SAFETY: an open connection; with statements unfinalized it stays
    // open for them, and only this object forgets it.
    unsafe { ffi::sqlite3_close(checked.db) };
    if let Kind::Record(id) = args[0].kind()
        && let Some(SqliteHandleState::Database(state)) = interp.find_sqlite_handle_mut(id.id)
    {
        state.db = ptr::null_mut();
    }
    Ok(Value::T)
}

/// Bind ? values in a statement like "insert into foo values (?, ?, ?)":
/// the message bind_values returns, or None when every value was bound.
fn bind_values(
    interp: &mut Interpreter,
    env: &mut Env,
    db: *mut ffi::sqlite3,
    stmt: *mut ffi::sqlite3_stmt,
    values: &Value,
) -> Result<Option<Vec<u8>>, LispError> {
    // SAFETY: STMT is the caller's prepared statement (or null).
    unsafe { ffi::sqlite3_reset(stmt) };
    let values = match values.kind() {
        Kind::Vector(_) => vector_items(values)?,
        _ => values.to_vec()?,
    };
    for (i, value) in values.into_iter().enumerate() {
        let index = i as c_int + 1;
        let ret = if let Some(string) = string_like(&value) {
            let bytes = internal_string_bytes(&string)?;
            if bytes.is_empty() {
                bind_text(stmt, index, &bytes)
            } else {
                let coding_system = property_at_start(&string.props, "coding-system")
                    .copied()
                    .unwrap_or(Value::Nil);
                if coding_system.is_nil() {
                    // Default to utf-8.
                    bind_text(stmt, index, &encode_string(&string)?)
                } else if matches!(coding_system.kind(), Kind::Symbol(symbol) if symbol == "binary")
                {
                    if bytes.len() != string.text.chars().count() {
                        return Err(sqlite_error("BLOB values must be unibyte"));
                    }
                    // SAFETY: SQLITE_TRANSIENT has SQLite copy the bytes.
                    unsafe {
                        ffi::sqlite3_bind_blob(
                            stmt,
                            index,
                            bytes.as_ptr().cast(),
                            bytes.len() as c_int,
                            ffi::SQLITE_TRANSIENT(),
                        )
                    }
                } else {
                    let encoded = super::primitives::call(
                        interp,
                        "encode-coding-string",
                        &[value, coding_system],
                        env,
                    )?;
                    bind_text(
                        stmt,
                        index,
                        &internal_string_bytes(&check_string(&encoded)?)?,
                    )
                }
            }
        } else {
            // SAFETY: binding a scalar to the caller's statement.
            unsafe {
                match value.kind() {
                    Kind::Integer(number) => ffi::sqlite3_bind_int64(stmt, index, number),
                    // bignum_to_intmax: 0 for one beyond intmax_t.
                    Kind::BigInteger(number) => {
                        ffi::sqlite3_bind_int64(stmt, index, number.to_i64().unwrap_or(0))
                    }
                    Kind::Float(number) => ffi::sqlite3_bind_double(stmt, index, number.get()),
                    Kind::Nil => ffi::sqlite3_bind_null(stmt, index),
                    Kind::T => ffi::sqlite3_bind_int(stmt, index, 1),
                    Kind::Symbol(symbol) if symbol == "false" => {
                        ffi::sqlite3_bind_int(stmt, index, 0)
                    }
                    _ => return Ok(Some(b"invalid argument".to_vec())),
                }
            }
        };
        if ret != ffi::SQLITE_OK {
            // SAFETY: DB is the statement's open connection.
            return Ok(Some(unsafe { c_bytes(ffi::sqlite3_errmsg(db)) }.to_vec()));
        }
    }
    Ok(None)
}

fn bind_text(stmt: *mut ffi::sqlite3_stmt, index: c_int, bytes: &[u8]) -> c_int {
    // SAFETY: SQLITE_TRANSIENT has SQLite copy the bytes before returning.
    unsafe {
        ffi::sqlite3_bind_text(
            stmt,
            index,
            bytes.as_ptr().cast(),
            bytes.len() as c_int,
            ffi::SQLITE_TRANSIENT(),
        )
    }
}

/// Fget_text_property (make_fixnum (0), PROPERTY, string).
fn property_at_start<'a>(
    props: &'a [crate::buffer::TextPropertySpan],
    property: &str,
) -> Option<&'a Value> {
    props
        .iter()
        .find(|span| span.start == 0 && span.end > 0)
        .and_then(|span| {
            span.props.iter().find_map(
                |(name, value)| {
                    if name == property { Some(value) } else { None }
                },
            )
        })
}

/// The bytes of column I's value, as SQLite keeps them for the row.
///
/// # Safety
/// POINTER and LENGTH are what SQLite returned for the current row.
unsafe fn column_bytes<'a>(pointer: *const std::ffi::c_void, length: c_int) -> &'a [u8] {
    if pointer.is_null() || length <= 0 {
        &[]
    } else {
        // SAFETY: as the caller promises.
        unsafe { std::slice::from_raw_parts(pointer.cast(), length as usize) }
    }
}

/// row_to_value: the current row's columns as a list.
fn row_to_value(
    interp: &mut Interpreter,
    env: &mut Env,
    stmt: *mut ffi::sqlite3_stmt,
) -> Result<Value, LispError> {
    // SAFETY: STMT has just stepped to a row; each column is read in the
    // order SQLite's documentation gives (the value, then its length).
    unsafe {
        let len = ffi::sqlite3_column_count(stmt);
        let mut values = Vec::with_capacity(len.max(0) as usize);
        for i in 0..len {
            let value = match ffi::sqlite3_column_type(stmt, i) {
                ffi::SQLITE_INTEGER => Value::Integer(ffi::sqlite3_column_int64(stmt, i)),
                ffi::SQLITE_FLOAT => Value::float(ffi::sqlite3_column_double(stmt, i)),
                ffi::SQLITE_BLOB => {
                    let blob = ffi::sqlite3_column_blob(stmt, i);
                    bytes_to_shared_unibyte_value(column_bytes(
                        blob,
                        ffi::sqlite3_column_bytes(stmt, i),
                    ))
                }
                ffi::SQLITE_TEXT => {
                    let text = ffi::sqlite3_column_text(stmt, i);
                    let source = bytes_to_shared_unibyte_value(column_bytes(
                        text.cast(),
                        ffi::sqlite3_column_bytes(stmt, i),
                    ));
                    // code_convert_string_norecord (..., Qutf_8, false).
                    decode_coding_text_recording(
                        interp,
                        &source,
                        Some("utf-8"),
                        false,
                        false,
                        None,
                        false,
                        env,
                    )?
                }
                _ => Value::Nil,
            };
            values.push(value);
        }
        Ok(Value::list(values))
    }
}

/// sqlite_prepare_errdata: (ERRSTR ERRMSG ERRCODE EXTENDED-ERRCODE).
fn sqlite_prepare_errdata(
    interp: &Interpreter,
    env: &Env,
    code: c_int,
    sdb: *mut ffi::sqlite3,
) -> Result<Value, LispError> {
    // SAFETY: SDB is the open connection the failed call was made on.
    unsafe {
        let message = ffi::sqlite3_errmsg(sdb);
        let lerrmsg = if message.is_null() {
            Value::Nil
        } else {
            build_string(interp, env, c_bytes(message))?
        };
        let errstr = build_string(interp, env, c_bytes(ffi::sqlite3_errstr(code)))?;
        let ext_errcode = Value::Integer(i64::from(ffi::sqlite3_extended_errcode(sdb)));
        Ok(Value::list([
            errstr,
            lerrmsg,
            Value::Integer(i64::from(code)),
            ext_errcode,
        ]))
    }
}

/// The VALUES check Fsqlite_execute and Fsqlite_select share.
fn check_values(values: &Value) -> Result<(), LispError> {
    if matches!(values.kind(), Kind::Nil | Kind::Cons(_) | Kind::Vector(_)) {
        Ok(())
    } else {
        Err(sqlite_error("VALUES must be a list or a vector"))
    }
}

fn sqlite_execute(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-execute", args, 2, 3)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    let query = check_string(&args[1])?;
    let values = args.get(2).copied().unwrap_or(Value::Nil);
    check_values(&values)?;
    let sdb = checked.db;
    let encoded = c_string(encode_string(&query)?);
    let mut stmt = ptr::null_mut();
    // We only execute the first statement -- if there's several
    // (separated by a semicolon), the subsequent statements won't be done.
    // SAFETY: ENCODED is NUL-terminated; SDB is open.
    let mut ret = unsafe {
        ffi::sqlite3_prepare_v2(sdb, encoded.as_ptr().cast(), -1, &mut stmt, ptr::null_mut())
    };
    let data = 'exit: {
        if ret != ffi::SQLITE_OK {
            // SAFETY: finalizing what the failed prepare left, if anything.
            unsafe { ffi::sqlite3_finalize(stmt) };
            stmt = ptr::null_mut();
            break 'exit sqlite_prepare_errdata(interp, env, ret, sdb)?;
        }
        // Bind ? values.
        if !values.is_nil() {
            match bind_values(interp, env, sdb, stmt, &values) {
                Ok(None) => {}
                Ok(Some(error)) => break 'exit build_string(interp, env, &error)?,
                Err(error) => {
                    // SAFETY: the statement prepared above, finalized once.
                    unsafe { ffi::sqlite3_finalize(stmt) };
                    return Err(error);
                }
            }
        }
        // SAFETY: the statement prepared above.
        ret = unsafe { ffi::sqlite3_step(stmt) };
        if ret == ffi::SQLITE_ROW {
            let mut data = Vec::new();
            loop {
                match row_to_value(interp, env, stmt) {
                    Ok(row) => data.push(row),
                    Err(error) => {
                        // SAFETY: as above.
                        unsafe { ffi::sqlite3_finalize(stmt) };
                        return Err(error);
                    }
                }
                // SAFETY: as above.
                if unsafe { ffi::sqlite3_step(stmt) } != ffi::SQLITE_ROW {
                    break;
                }
            }
            // SAFETY: as above.
            unsafe { ffi::sqlite3_finalize(stmt) };
            return Ok(Value::list(data));
        } else if ret == ffi::SQLITE_OK || ret == ffi::SQLITE_DONE {
            // SAFETY: SDB is open; then the statement prepared above.
            let rows = Value::Integer(i64::from(unsafe { ffi::sqlite3_changes(sdb) }));
            unsafe { ffi::sqlite3_finalize(stmt) };
            return Ok(rows);
        }
        errmsg(interp, env, sdb)?
    };
    // SAFETY: the statement prepared above, or null.
    unsafe { ffi::sqlite3_finalize(stmt) };
    let condition = if ret == ffi::SQLITE_LOCKED || ret == ffi::SQLITE_BUSY {
        "sqlite-locked-error"
    } else {
        "sqlite-error"
    };
    Err(sqlite_signal(condition, data))
}

/// column_names: STMT's column names, a list of strings.
fn column_names(
    interp: &Interpreter,
    env: &Env,
    stmt: *mut ffi::sqlite3_stmt,
) -> Result<Value, LispError> {
    // SAFETY: STMT is a prepared statement (or null, with no columns).
    unsafe {
        let count = ffi::sqlite3_column_count(stmt);
        let mut columns = Vec::with_capacity(count.max(0) as usize);
        for i in 0..count {
            columns.push(build_string(
                interp,
                env,
                c_bytes(ffi::sqlite3_column_name(stmt, i)),
            )?);
        }
        Ok(Value::list(columns))
    }
}

fn sqlite_select(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-select", args, 2, 4)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    let query = check_string(&args[1])?;
    let values = args.get(2).copied().unwrap_or(Value::Nil);
    let return_type = args.get(3).copied().unwrap_or(Value::Nil);
    check_values(&values)?;
    let sdb = checked.db;
    let encoded = encode_string(&query)?;
    let length = encoded.len() as c_int;
    let encoded = {
        let mut bytes = encoded;
        bytes.push(0);
        bytes
    };
    let mut stmt = ptr::null_mut();
    // SAFETY: ENCODED holds LENGTH bytes and a terminator; SDB is open.
    let ret = unsafe {
        ffi::sqlite3_prepare_v2(
            sdb,
            encoded.as_ptr().cast(),
            length,
            &mut stmt,
            ptr::null_mut(),
        )
    };
    if ret != ffi::SQLITE_OK {
        // SAFETY: finalizing what the failed prepare left, if anything.
        unsafe { ffi::sqlite3_finalize(stmt) };
        let errmsg = sqlite_prepare_errdata(interp, env, ret, sdb)?;
        return Err(sqlite_signal("sqlite-error", errmsg));
    }
    // Query with parameters.
    if !values.is_nil() {
        let bound = bind_values(interp, env, sdb, stmt, &values);
        let error = match bound {
            Ok(None) => None,
            Ok(Some(error)) => Some(Ok(error)),
            Err(error) => Some(Err(error)),
        };
        if let Some(error) = error {
            // SAFETY: the statement prepared above, finalized once.
            unsafe { ffi::sqlite3_finalize(stmt) };
            let error = error?;
            return Err(sqlite_signal(
                "sqlite-error",
                build_string(interp, env, &error)?,
            ));
        }
    }
    // Return a handle to get the data.
    if matches!(return_type.kind(), Kind::Symbol(symbol) if symbol == "set") {
        let name = match args[0].kind() {
            Kind::Record(id) => match interp.find_sqlite_handle(id.id) {
                Some(SqliteHandleState::Database(state)) => state.name.clone(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        };
        return create_sqlite_handle(
            interp,
            SqliteHandleState::Set(SqliteSetState {
                db: sdb,
                stmt,
                eof: false,
                name,
            }),
        );
    }
    // Return the data directly.
    let mut data = Vec::new();
    let result = (|| {
        // SAFETY: the statement prepared above.
        while unsafe { ffi::sqlite3_step(stmt) } == ffi::SQLITE_ROW {
            data.push(row_to_value(interp, env, stmt)?);
        }
        if matches!(return_type.kind(), Kind::Symbol(symbol) if symbol == "full") {
            Ok(Value::cons(
                column_names(interp, env, stmt)?,
                Value::list(std::mem::take(&mut data)),
            ))
        } else {
            Ok(Value::list(std::mem::take(&mut data)))
        }
    })();
    // SAFETY: the statement prepared above, finalized once.
    unsafe { ffi::sqlite3_finalize(stmt) };
    result
}

/// sqlite_exec: t when sqlite3_exec runs QUERY's statements, else nil.
fn sqlite_exec(sdb: *mut ffi::sqlite3, query: Vec<u8>) -> Value {
    let query = c_string(query);
    // SAFETY: QUERY is NUL-terminated; SDB is open.
    let ret = unsafe {
        ffi::sqlite3_exec(
            sdb,
            query.as_ptr().cast(),
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    if ret != ffi::SQLITE_OK {
        Value::Nil
    } else {
        Value::T
    }
}

fn sqlite_execute_batch(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-execute-batch", args, 2, 2)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    let statements = check_string(&args[1])?;
    Ok(sqlite_exec(checked.db, encode_string(&statements)?))
}

fn sqlite_transaction(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-transaction", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    Ok(sqlite_exec(checked.db, b"begin".to_vec()))
}

fn sqlite_commit(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-commit", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    Ok(sqlite_exec(checked.db, b"commit".to_vec()))
}

fn sqlite_rollback(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-rollback", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    Ok(sqlite_exec(checked.db, b"rollback".to_vec()))
}

// Fsqlite_pragma: sqlite_exec of concat2 (build_string ("PRAGMA "),
// pragma)'s data, the pragma's own bytes unencoded.
fn sqlite_pragma(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-pragma", args, 2, 2)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    let pragma = check_string(&args[1])?;
    let mut query = b"PRAGMA ".to_vec();
    query.extend_from_slice(&internal_string_bytes(&pragma)?);
    Ok(sqlite_exec(checked.db, query))
}

fn sqlite_load_extension(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-load-extension", args, 2, 2)?;
    let checked = check_sqlite(interp, &args[0], false)?;
    let module = check_string(&args[1])?.text;
    let basename = super::primitives::call(
        interp,
        "file-name-nondirectory",
        std::slice::from_ref(&args[1]),
        env,
    )?;
    if !allowed_module_name(&string_text(&basename)?) {
        return Err(sqlite_error("Module name not on allowlist"));
    }
    // Expand all Lisp data explicitly, so as to avoid signaling an error
    // while extension loading is enabled.
    let path = super::primitives::expand_file_name_runtime(interp, env, &module, None)?;
    let path = c_string(path.into_bytes());
    let sdb = checked.db;
    // SAFETY: SDB is open; db_config enables only the C API's extension
    // loading, never the SQL load_extension function, and disables it
    // again before returning.
    unsafe {
        let mut result = ffi::sqlite3_db_config(
            sdb,
            ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
            1,
            ptr::null_mut::<c_int>(),
        );
        if result == ffi::SQLITE_OK {
            result = ffi::sqlite3_load_extension(
                sdb,
                path.as_ptr().cast(),
                ptr::null(),
                ptr::null_mut(),
            );
            ffi::sqlite3_db_config(
                sdb,
                ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
                0,
                ptr::null_mut::<c_int>(),
            );
            if result == ffi::SQLITE_OK {
                return Ok(Value::T);
            }
        }
    }
    Ok(Value::Nil)
}

fn sqlite_next(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-next", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], true)?;
    if set_state(interp, &args[0]).is_some_and(|state| state.eof) {
        return Ok(Value::Nil);
    }
    // SAFETY: the set's live statement.
    let ret = unsafe { ffi::sqlite3_step(checked.stmt) };
    if ret != ffi::SQLITE_ROW && ret != ffi::SQLITE_OK && ret != ffi::SQLITE_DONE {
        return Err(sqlite_signal(
            "sqlite-error",
            errmsg(interp, env, checked.db)?,
        ));
    }
    if ret == ffi::SQLITE_DONE {
        if let Some(state) = set_state(interp, &args[0]) {
            state.eof = true;
        }
        return Ok(Value::Nil);
    }
    row_to_value(interp, env, checked.stmt)
}

fn sqlite_columns(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-columns", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], true)?;
    column_names(interp, env, checked.stmt)
}

fn sqlite_more_p(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-more-p", args, 1, 1)?;
    check_sqlite(interp, &args[0], true)?;
    Ok(
        if set_state(interp, &args[0]).is_some_and(|state| state.eof) {
            Value::Nil
        } else {
            Value::T
        },
    )
}

fn sqlite_finalize(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-finalize", args, 1, 1)?;
    let checked = check_sqlite(interp, &args[0], true)?;
    // SAFETY: the set's live statement, finalized once: its DB goes null.
    unsafe { ffi::sqlite3_finalize(checked.stmt) };
    if let Some(state) = set_state(interp, &args[0]) {
        state.db = ptr::null_mut();
    }
    Ok(Value::T)
}

fn sqlite_version(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("sqlite-version", args, 0, 0)?;
    // SAFETY: sqlite3_libversion returns a static string.
    build_string(interp, env, unsafe { c_bytes(ffi::sqlite3_libversion()) })
}

fn sqlitep(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlitep", args, 1, 1)?;
    Ok(match args[0].kind() {
        Kind::Record(id) if interp.find_sqlite_handle(id.id).is_some() => Value::T,
        _ => Value::Nil,
    })
}

fn sqlite_available_p(args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-available-p", args, 0, 0)?;
    Ok(Value::T)
}

/// print.c's PVEC_SQLITE: "#<sqlite db=%p stmt=%p name=%s>", the statement
/// only for a set.
pub(crate) fn print_sqlite(interp: &Interpreter, id: u64) -> String {
    fn pointer<T>(pointer: *mut T) -> String {
        if pointer.is_null() {
            "(nil)".into()
        } else {
            format!("{:#x}", pointer as usize)
        }
    }
    match interp.find_sqlite_handle(id) {
        Some(SqliteHandleState::Database(state)) => format!(
            "#<sqlite db={} name={}>",
            pointer(state.db),
            String::from_utf8_lossy(&state.name)
        ),
        Some(SqliteHandleState::Set(state)) => format!(
            "#<sqlite db={} stmt={} name={}>",
            pointer(state.db),
            pointer(state.stmt),
            String::from_utf8_lossy(&state.name)
        ),
        None => "#<sqlite>".into(),
    }
}

fn create_sqlite_handle(
    interp: &mut Interpreter,
    state: SqliteHandleState,
) -> Result<Value, LispError> {
    let value =
        interp.create_pseudovector(crate::lisp::eval::RecordKind::Sqlite, "sqlite", Vec::new());
    let Kind::Record(id) = value.kind() else {
        return Err(LispError::Signal("sqlite record allocation failed".into()));
    };
    interp.register_sqlite_handle(id.id, state);
    Ok(Value::Record(id))
}

fn allowed_module_name(module: &str) -> bool {
    // Add names of useful and free modules here.
    let allowlist = [
        "base64",
        "cksumvfs",
        "compress",
        "csv",
        "csvtable",
        "fts3",
        "icu",
        "pcre",
        "percentile",
        "regexp",
        "rot13",
        "rtree",
        "sha1",
        "uuid",
        "vec0",
        "vector0",
        "vfslog",
        "vss0",
        "zipfile",
    ];
    // Possibly skip past a common prefix (libsqlite3_mod_ is used by
    // Debian).
    let name = module.strip_prefix("libsqlite3_mod_").unwrap_or(module);
    allowlist.iter().any(|allowed| {
        name.len() > allowed.len()
            && name.starts_with(allowed)
            && (matches!(&name[allowed.len()..], ".so" | ".dylib")
                || name[allowed.len()..].eq_ignore_ascii_case(".dll"))
    })
}

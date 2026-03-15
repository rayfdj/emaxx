use std::path::Path;

use num_traits::ToPrimitive;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags, params_from_iter};

use super::eval::Interpreter;
use super::primitives::{
    make_shared_string_value_with_multibyte, string_like, string_text, vector_items,
};
use super::types::{Env, LispError, Value};

pub enum SqliteHandleState {
    Database(SqliteDatabaseState),
    Set(SqliteSetState),
}

pub struct SqliteDatabaseState {
    pub connection: Option<Connection>,
}

pub struct SqliteSetState {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
    pub index: usize,
    pub closed: bool,
}

pub fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        "sqlite-open"
            | "sqlite-close"
            | "sqlite-execute"
            | "sqlite-select"
            | "sqlite-execute-batch"
            | "sqlite-load-extension"
            | "sqlite-next"
            | "sqlite-more-p"
            | "sqlite-finalize"
            | "sqlite-version"
            | "sqlitep"
            | "sqlite-available-p"
    )
}

pub fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    _env: &mut Env,
) -> Result<Value, LispError> {
    match name {
        "sqlite-open" => sqlite_open(interp, args),
        "sqlite-close" => sqlite_close(interp, args),
        "sqlite-execute" => sqlite_execute(interp, args),
        "sqlite-select" => sqlite_select(interp, args),
        "sqlite-execute-batch" => sqlite_execute_batch(interp, args),
        "sqlite-load-extension" => sqlite_load_extension(interp, args),
        "sqlite-next" => sqlite_next(interp, args),
        "sqlite-more-p" => sqlite_more_p(interp, args),
        "sqlite-finalize" => sqlite_finalize(interp, args),
        "sqlite-version" => sqlite_version(args),
        "sqlitep" => sqlitep(interp, args),
        "sqlite-available-p" => sqlite_available_p(args),
        _ => Err(LispError::Void(name.to_string())),
    }
}

fn need_args(name: &str, args: &[Value], min: usize, max: usize) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        Err(LispError::WrongNumberOfArgs(name.into(), args.len()))
    } else {
        Ok(())
    }
}

fn sqlite_open(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-open", args, 0, 3)?;
    let file = args.first().unwrap_or(&Value::Nil);
    let readonly = args.get(1).is_some_and(Value::is_truthy);
    let connection = if file.is_nil() {
        Connection::open_in_memory().map_err(sqlite_error)?
    } else {
        let path = string_text(file)?;
        if readonly {
            Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
            )
        } else {
            Connection::open(path)
        }
        .map_err(sqlite_error)?
    };
    create_sqlite_handle(
        interp,
        SqliteHandleState::Database(SqliteDatabaseState {
            connection: Some(connection),
        }),
    )
}

fn sqlite_close(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-close", args, 1, 1)?;
    let id = sqlite_id(&args[0])?;
    match interp.find_sqlite_handle_mut(id) {
        Some(SqliteHandleState::Database(state)) => {
            if state.connection.take().is_none() {
                Err(LispError::Signal("Database closed".into()))
            } else {
                Ok(Value::T)
            }
        }
        Some(SqliteHandleState::Set(_)) => Err(LispError::Signal("Invalid database object".into())),
        None => Err(LispError::TypeError("sqlite".into(), args[0].type_name())),
    }
}

fn sqlite_execute(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-execute", args, 2, 3)?;
    let id = sqlite_id(&args[0])?;
    let query = string_text(&args[1])?;
    let values = args.get(2).unwrap_or(&Value::Nil);
    let params = bind_parameters(values)?;
    let connection = database_connection(interp, id)?;
    let mut stmt = connection.prepare(&query).map_err(sqlite_error)?;
    if stmt.column_count() > 0 {
        Ok(Value::list(collect_rows(&mut stmt, &params)?))
    } else {
        let changed = stmt
            .execute(params_from_iter(params.iter()))
            .map_err(sqlite_error)?;
        Ok(Value::Integer(changed as i64))
    }
}

fn sqlite_select(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-select", args, 2, 4)?;
    let id = sqlite_id(&args[0])?;
    let query = string_text(&args[1])?;
    let values = args.get(2).unwrap_or(&Value::Nil);
    let return_type = args.get(3).unwrap_or(&Value::Nil);
    let params = bind_parameters(values)?;
    let (columns, rows) = {
        let connection = database_connection(interp, id)?;
        let mut stmt = connection.prepare(&query).map_err(sqlite_error)?;
        let columns = stmt
            .column_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let rows = collect_rows(&mut stmt, &params)?;
        (columns, rows)
    };

    match return_type {
        Value::Symbol(symbol) if symbol == "full" => Ok(Value::cons(
            Value::list(columns.iter().cloned().map(Value::String)),
            Value::list(rows),
        )),
        Value::Symbol(symbol) if symbol == "set" => create_sqlite_handle(
            interp,
            SqliteHandleState::Set(SqliteSetState {
                columns,
                rows,
                index: 0,
                closed: false,
            }),
        ),
        _ => Ok(Value::list(rows)),
    }
}

fn sqlite_execute_batch(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-execute-batch", args, 2, 2)?;
    let id = sqlite_id(&args[0])?;
    let statements = string_text(&args[1])?;
    database_connection(interp, id)?
        .execute_batch(&statements)
        .map_err(sqlite_error)?;
    Ok(Value::T)
}

fn sqlite_load_extension(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-load-extension", args, 2, 2)?;
    let id = sqlite_id(&args[0])?;
    let _ = database_connection(interp, id)?;
    let module = string_text(&args[1])?;
    if !allowed_module_name(&module) {
        return Err(LispError::Signal("Module name not on allowlist".into()));
    }
    Ok(if Path::new(&module).is_file() {
        Value::T
    } else {
        Value::Nil
    })
}

fn sqlite_next(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-next", args, 1, 1)?;
    let id = sqlite_id(&args[0])?;
    match interp.find_sqlite_handle_mut(id) {
        Some(SqliteHandleState::Set(state)) => {
            if state.closed {
                return Err(LispError::Signal("Statement closed".into()));
            }
            if state.index >= state.rows.len() {
                return Ok(Value::Nil);
            }
            let row = state.rows[state.index].clone();
            state.index += 1;
            Ok(row)
        }
        Some(SqliteHandleState::Database(_)) => Err(LispError::Signal("Invalid set object".into())),
        None => Err(LispError::TypeError("sqlite".into(), args[0].type_name())),
    }
}

fn sqlite_more_p(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-more-p", args, 1, 1)?;
    let id = sqlite_id(&args[0])?;
    match interp.find_sqlite_handle(id) {
        Some(SqliteHandleState::Set(state)) => {
            if state.closed {
                return Err(LispError::Signal("Statement closed".into()));
            }
            Ok(if state.index < state.rows.len() {
                Value::T
            } else {
                Value::Nil
            })
        }
        Some(SqliteHandleState::Database(_)) => Err(LispError::Signal("Invalid set object".into())),
        None => Err(LispError::TypeError("sqlite".into(), args[0].type_name())),
    }
}

fn sqlite_finalize(interp: &mut Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-finalize", args, 1, 1)?;
    let id = sqlite_id(&args[0])?;
    match interp.find_sqlite_handle_mut(id) {
        Some(SqliteHandleState::Set(state)) => {
            if state.closed {
                Err(LispError::Signal("Statement closed".into()))
            } else {
                state.closed = true;
                state.rows.clear();
                state.columns.clear();
                Ok(Value::T)
            }
        }
        Some(SqliteHandleState::Database(_)) => Err(LispError::Signal("Invalid set object".into())),
        None => Err(LispError::TypeError("sqlite".into(), args[0].type_name())),
    }
}

fn sqlite_version(args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-version", args, 0, 0)?;
    let connection = Connection::open_in_memory().map_err(sqlite_error)?;
    let version = connection
        .query_row("select sqlite_version()", [], |row| row.get::<_, String>(0))
        .map_err(sqlite_error)?;
    Ok(Value::String(version))
}

fn sqlitep(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlitep", args, 1, 1)?;
    Ok(match args[0] {
        Value::Record(id) if interp.find_sqlite_handle(id).is_some() => Value::T,
        _ => Value::Nil,
    })
}

fn sqlite_available_p(args: &[Value]) -> Result<Value, LispError> {
    need_args("sqlite-available-p", args, 0, 0)?;
    Ok(Value::T)
}

fn sqlite_error(error: rusqlite::Error) -> LispError {
    LispError::Signal(error.to_string())
}

fn sqlite_id(value: &Value) -> Result<u64, LispError> {
    match value {
        Value::Record(id) => Ok(*id),
        _ => Err(LispError::TypeError("sqlite".into(), value.type_name())),
    }
}

fn create_sqlite_handle(
    interp: &mut Interpreter,
    state: SqliteHandleState,
) -> Result<Value, LispError> {
    let value = interp.create_record("sqlite", Vec::new());
    let Value::Record(id) = value else {
        return Err(LispError::Signal("sqlite record allocation failed".into()));
    };
    interp.register_sqlite_handle(id, state);
    Ok(Value::Record(id))
}

fn database_connection(interp: &mut Interpreter, id: u64) -> Result<&Connection, LispError> {
    match interp.find_sqlite_handle_mut(id) {
        Some(SqliteHandleState::Database(state)) => state
            .connection
            .as_ref()
            .ok_or_else(|| LispError::Signal("Database closed".into())),
        Some(SqliteHandleState::Set(_)) => Err(LispError::Signal("Invalid database object".into())),
        None => Err(LispError::TypeError(
            "sqlite".into(),
            format!("record<{id}>"),
        )),
    }
}

fn bind_parameters(values: &Value) -> Result<Vec<SqlValue>, LispError> {
    if values.is_nil() {
        return Ok(Vec::new());
    }
    if !matches!(values, Value::Cons(_, _)) {
        return Err(LispError::Signal(
            "VALUES must be a list or a vector".into(),
        ));
    }
    vector_items(values)?
        .into_iter()
        .map(bind_parameter)
        .collect()
}

fn bind_parameter(value: Value) -> Result<SqlValue, LispError> {
    if let Some(string) = string_like(&value) {
        let is_binary = property_at_start(&string.props, "coding-system").is_some_and(
            |property| matches!(property, Value::Symbol(symbol) if symbol == "binary"),
        );
        if is_binary {
            if string.multibyte || string.text.len() != string.text.chars().count() {
                return Err(LispError::Signal("BLOB values must be unibyte".into()));
            }
            return Ok(SqlValue::Blob(string.text.into_bytes()));
        }
        return Ok(SqlValue::Text(string.text));
    }

    match value {
        Value::Integer(number) => Ok(SqlValue::Integer(number)),
        Value::BigInteger(number) => number
            .to_i64()
            .map(SqlValue::Integer)
            .ok_or_else(|| LispError::Signal("integer out of range".into())),
        Value::Float(number) => Ok(SqlValue::Real(number)),
        Value::Nil => Ok(SqlValue::Null),
        Value::T => Ok(SqlValue::Integer(1)),
        Value::Symbol(symbol) if symbol == "false" => Ok(SqlValue::Integer(0)),
        other => Err(LispError::Signal(format!("invalid argument: {other}"))),
    }
}

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

fn collect_rows(
    stmt: &mut rusqlite::Statement<'_>,
    params: &[SqlValue],
) -> Result<Vec<Value>, LispError> {
    let column_count = stmt.column_count();
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(sqlite_error)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        result.push(row_to_value(row, column_count)?);
    }
    Ok(result)
}

fn row_to_value(row: &rusqlite::Row<'_>, column_count: usize) -> Result<Value, LispError> {
    let mut values = Vec::with_capacity(column_count);
    for index in 0..column_count {
        let value = match row.get_ref(index).map_err(sqlite_error)? {
            ValueRef::Null => Value::Nil,
            ValueRef::Integer(number) => Value::Integer(number),
            ValueRef::Real(number) => Value::Float(number),
            ValueRef::Text(bytes) => {
                let text = String::from_utf8_lossy(bytes).into_owned();
                make_shared_string_value_with_multibyte(text, Vec::new(), true)
            }
            ValueRef::Blob(bytes) => {
                Value::String(bytes.iter().map(|byte| char::from(*byte)).collect())
            }
        };
        values.push(value);
    }
    Ok(Value::list(values))
}

fn allowed_module_name(module: &str) -> bool {
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
    let Some(name) = Path::new(module).file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let name = name.strip_prefix("libsqlite3_mod_").unwrap_or(name);
    allowlist.iter().any(|allowed| {
        name.len() > allowed.len()
            && name.starts_with(allowed)
            && matches!(&name[allowed.len()..], ".so" | ".dylib" | ".dll")
    })
}

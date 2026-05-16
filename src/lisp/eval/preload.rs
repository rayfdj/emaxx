use super::*;

pub(crate) fn preloaded_command_line_1() -> Value {
    Value::Lambda(
        vec!["args-left".into()],
        vec![Value::list([
            Value::Symbol("let".into()),
            Value::list([
                Value::list([
                    Value::Symbol("command-line-args-left".into()),
                    Value::Symbol("args-left".into()),
                ]),
                Value::list([Value::Symbol("tem".into()), Value::Nil]),
            ]),
            Value::list([
                Value::Symbol("while".into()),
                Value::Symbol("command-line-args-left".into()),
                Value::list([
                    Value::Symbol("let".into()),
                    Value::list([Value::list([
                        Value::Symbol("argi".into()),
                        Value::list([
                            Value::Symbol("car".into()),
                            Value::Symbol("command-line-args-left".into()),
                        ]),
                    ])]),
                    Value::list([
                        Value::Symbol("setq".into()),
                        Value::Symbol("command-line-args-left".into()),
                        Value::list([
                            Value::Symbol("cdr".into()),
                            Value::Symbol("command-line-args-left".into()),
                        ]),
                    ]),
                    Value::list([
                        Value::Symbol("when".into()),
                        Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol("tem".into()),
                            Value::list([
                                Value::Symbol("assoc".into()),
                                Value::Symbol("argi".into()),
                                Value::Symbol("command-switch-alist".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::list([Value::Symbol("cdr".into()), Value::Symbol("tem".into())]),
                            Value::Symbol("argi".into()),
                        ]),
                    ]),
                ]),
            ]),
            Value::Nil,
        ])],
        shared_env(Vec::new()),
    )
}

pub(crate) fn preloaded_sh_mode() -> Value {
    Value::Lambda(
        Vec::new(),
        vec![
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("major-mode".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("sh-mode".into()),
                ]),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("mode-name".into()),
                Value::String("Shell-script".into()),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-case-fold-search".into()),
                Value::Nil,
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-generic-skip-comments-and-strings".into()),
                Value::Nil,
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-create-index-function".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("imenu-default-create-index-function".into()),
                ]),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-generic-expression".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::list([
                        Value::list([
                            Value::Nil,
                            Value::String(
                                "^[ \t]*function[ \t]+\\([A-Za-z_][A-Za-z0-9_]*\\)".into(),
                            ),
                            Value::Integer(1),
                        ]),
                        Value::list([
                            Value::Nil,
                            Value::String("^[ \t]*\\([A-Za-z_][A-Za-z0-9_]*\\)[ \t]*()".into()),
                            Value::Integer(1),
                        ]),
                    ]),
                ]),
            ]),
            Value::Nil,
        ],
        shared_env(Vec::new()),
    )
}

pub(crate) fn builtin_auto_mode_alist() -> Value {
    Value::list([
        Value::cons(
            Value::String("\\.\\(?:tar\\(?:\\.gz\\)?\\|tgz\\)\\'".into()),
            Value::Symbol("tar-mode".into()),
        ),
        Value::cons(
            Value::String("\\.zip\\'".into()),
            Value::Symbol("archive-mode".into()),
        ),
        Value::cons(
            Value::String(
                "\\.\\(?:C\\|cc\\|cpp\\|cxx\\|c\\+\\+\\|hh\\|hpp\\|hxx\\|h\\+\\+\\)\\'".into(),
            ),
            Value::Symbol("c++-mode".into()),
        ),
        Value::cons(
            Value::String("\\.c\\'".into()),
            Value::Symbol("c-mode".into()),
        ),
        Value::cons(
            Value::String("\\.java\\'".into()),
            Value::Symbol("java-mode".into()),
        ),
        Value::cons(
            Value::String("\\.mk\\'".into()),
            Value::Symbol("makefile-bsdmake-mode".into()),
        ),
        Value::cons(
            Value::String("\\.texi\\'".into()),
            Value::Symbol("texinfo-mode".into()),
        ),
        Value::cons(
            Value::String("\\.wy\\'".into()),
            Value::Symbol("wisent-grammar-mode".into()),
        ),
        Value::cons(
            Value::String("\\.srt\\'".into()),
            Value::Symbol("srecode-template-mode".into()),
        ),
    ])
}

pub(crate) fn preloaded_vc_directory_exclusion_list() -> Value {
    Value::list(
        [
            "SCCS", "RCS", "CVS", "MCVS", ".src", ".svn", ".git", ".hg", ".bzr", "_MTN", "_darcs",
            "{arch}", ".repo", ".jj",
        ]
        .into_iter()
        .map(Value::string),
    )
}

pub(crate) fn builtin_file_autoload(file: &str, interactive: Value) -> Value {
    Value::list([
        Value::Symbol("autoload".into()),
        Value::String(file.into()),
        Value::Nil,
        interactive,
        Value::Nil,
    ])
}

pub(crate) fn builtin_macro_autoload(file: &str) -> Value {
    Value::list([
        Value::Symbol("autoload".into()),
        Value::String(file.into()),
        Value::Nil,
        Value::Nil,
        Value::Symbol("macro".into()),
    ])
}

pub(crate) fn builtin_autoload_function(name: &str) -> Option<Value> {
    match name {
        "command-line-1" => Some(preloaded_command_line_1()),
        "cl-assoc-if" | "cl-assoc-if-not" | "cl-delete-duplicates" => {
            Some(builtin_file_autoload("cl-seq", Value::Nil))
        }
        "connection-local-p" => Some(builtin_macro_autoload("files-x")),
        "connection-local-set-profile-variables" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "connection-local-set-profiles" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "connection-local-update-profile-variables" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "connection-local-value" => Some(builtin_macro_autoload("files-x")),
        "dired" => Some(builtin_file_autoload("dired", Value::T)),
        "gv-define-expander" => Some(builtin_macro_autoload("gv")),
        "gv-define-setter" => Some(builtin_macro_autoload("gv")),
        "gv-define-simple-setter" => Some(builtin_macro_autoload("gv")),
        "gv-letplace" => Some(builtin_macro_autoload("gv")),
        "hack-connection-local-variables" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "hack-connection-local-variables-apply" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "key-valid-p" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-global-set" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-global-unset" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-local-set" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-local-unset" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-lookup" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-lookup-keymap" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-set" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-set-after" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-substitute" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-unset" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "define-keymap" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "pp" => Some(builtin_file_autoload("pp", Value::Nil)),
        "setq-connection-local" => Some(builtin_macro_autoload("files-x")),
        "sh-mode" => Some(preloaded_sh_mode()),
        "syntax-propertize-precompile-rules" | "syntax-propertize-rules" => {
            Some(builtin_macro_autoload("syntax"))
        }
        "with-connection-local-application-variables" => Some(builtin_macro_autoload("files-x")),
        "with-connection-local-variables" => Some(builtin_macro_autoload("files-x")),
        "with-connection-local-variables-1" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "point-to-register" => Some(Value::Lambda(
            vec!["register".into(), "&optional".into(), "arg".into()],
            vec![
                Value::list([
                    Value::Symbol("interactive".into()),
                    Value::list([
                        Value::Symbol("list".into()),
                        Value::Symbol("last-input-event".into()),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("when".into()),
                    Value::list([
                        Value::Symbol("or".into()),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::Integer(7),
                        ]),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("escape".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::Integer(27),
                        ]),
                    ]),
                    Value::list([Value::Symbol("keyboard-quit".into())]),
                ]),
                Value::Nil,
            ],
            shared_env(Vec::new()),
        )),
        _ => None,
    }
}

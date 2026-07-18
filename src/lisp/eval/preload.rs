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

// `eval-defun' echoes its value the way `elisp--eval-defun' does: the value
// via `print' and the integer rendering via `princ', both to the echo area.
fn eval_defun_print_form() -> Value {
    crate::lisp::reader::Reader::new(
        "(progn
           (print result t)
           (let ((extra (and (fboundp 'eval-expression-print-format)
                             (eval-expression-print-format result))))
             (if extra (princ extra t))))",
    )
    .read_all()
    .expect("static eval-defun print form parses")
    .remove(0)
}

pub(crate) fn preloaded_eval_defun() -> Value {
    Value::Lambda(
        vec!["edebug-it".into()],
        vec![
            Value::list([
                Value::Symbol("interactive".into()),
                Value::String("P".into()),
            ]),
            Value::list([
                Value::Symbol("save-excursion".into()),
                Value::list([
                    Value::Symbol("beginning-of-defun".into()),
                    Value::Integer(1),
                ]),
                Value::list([
                    Value::Symbol("let*".into()),
                    Value::list([
                        Value::list([
                            Value::Symbol("edebug-all-defs".into()),
                            Value::list([
                                Value::Symbol("if".into()),
                                Value::Symbol("edebug-it".into()),
                                Value::T,
                                Value::list([
                                    Value::Symbol("and".into()),
                                    Value::list([
                                        Value::Symbol("boundp".into()),
                                        Value::list([
                                            Value::Symbol("quote".into()),
                                            Value::Symbol("edebug-all-defs".into()),
                                        ]),
                                    ]),
                                    Value::Symbol("edebug-all-defs".into()),
                                ]),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("form".into()),
                            Value::list([
                                Value::Symbol("funcall".into()),
                                Value::Symbol("load-read-function".into()),
                                Value::list([Value::Symbol("current-buffer".into())]),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("name".into()),
                            Value::list([
                                Value::Symbol("and".into()),
                                Value::list([
                                    Value::Symbol("consp".into()),
                                    Value::Symbol("form".into()),
                                ]),
                                Value::list([
                                    Value::Symbol("nth".into()),
                                    Value::Integer(1),
                                    Value::Symbol("form".into()),
                                ]),
                            ]),
                        ]),
                        // GNU eval-defun evaluates through `eval-region',
                        // whose readevalloop binds `current-load-list' to
                        // (BUFFER-FILE-NAME) — `macroexp-file-name' reads it.
                        Value::list([
                            Value::Symbol("result".into()),
                            Value::list([
                                Value::Symbol("let".into()),
                                Value::list([Value::list([
                                    Value::Symbol("current-load-list".into()),
                                    Value::list([
                                        Value::Symbol("list".into()),
                                        Value::Symbol("buffer-file-name".into()),
                                    ]),
                                ])]),
                                Value::list([
                                    Value::Symbol("eval".into()),
                                    Value::Symbol("form".into()),
                                ]),
                            ]),
                        ]),
                    ]),
                    Value::list([
                        Value::Symbol("when".into()),
                        Value::list([
                            Value::Symbol("and".into()),
                            Value::Symbol("edebug-it".into()),
                            Value::list([
                                Value::Symbol("symbolp".into()),
                                Value::Symbol("name".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("if".into()),
                            Value::list([
                                Value::Symbol("and".into()),
                                Value::list([
                                    Value::Symbol("boundp".into()),
                                    Value::list([
                                        Value::Symbol("quote".into()),
                                        Value::Symbol("edebug-new-definition-function".into()),
                                    ]),
                                ]),
                                Value::Symbol("edebug-new-definition-function".into()),
                            ]),
                            Value::list([
                                Value::Symbol("funcall".into()),
                                Value::Symbol("edebug-new-definition-function".into()),
                                Value::Symbol("name".into()),
                            ]),
                            Value::list([
                                Value::Symbol("when".into()),
                                Value::list([
                                    Value::Symbol("fboundp".into()),
                                    Value::list([
                                        Value::Symbol("quote".into()),
                                        Value::Symbol("edebug-new-definition".into()),
                                    ]),
                                ]),
                                Value::list([
                                    Value::Symbol("edebug-new-definition".into()),
                                    Value::Symbol("name".into()),
                                ]),
                            ]),
                        ]),
                    ]),
                    eval_defun_print_form(),
                    Value::Symbol("result".into()),
                ]),
            ]),
        ],
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
            Value::String("\\.js\\'".into()),
            Value::Symbol("javascript-mode".into()),
        ),
        Value::cons(
            Value::String("\\.el\\'".into()),
            Value::Symbol("emacs-lisp-mode".into()),
        ),
        Value::cons(
            Value::String("\\.html?\\'".into()),
            Value::Symbol("html-mode".into()),
        ),
        Value::cons(
            Value::String("\\.py\\'".into()),
            Value::Symbol("python-mode".into()),
        ),
        Value::cons(
            Value::String("\\.mk\\'".into()),
            Value::Symbol("makefile-bsdmake-mode".into()),
        ),
        Value::cons(
            Value::String("\\(?:^\\|/\\)Makefile\\'".into()),
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

pub(crate) fn preloaded_completion_table_dynamic() -> Value {
    Value::Lambda(
        vec!["fun".into(), "&optional".into(), "switch-buffer".into()],
        vec![Value::list([
            Value::Symbol("lambda".into()),
            Value::list([
                Value::Symbol("string".into()),
                Value::Symbol("pred".into()),
                Value::Symbol("action".into()),
            ]),
            Value::list([
                Value::Symbol("cond".into()),
                Value::list([
                    Value::list([
                        Value::Symbol("or".into()),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::list([
                                Value::Symbol("car-safe".into()),
                                Value::Symbol("action".into()),
                            ]),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("boundaries".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("action".into()),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("metadata".into()),
                            ]),
                        ]),
                    ]),
                    Value::Nil,
                ]),
                Value::list([
                    Value::list([
                        Value::Symbol("eq".into()),
                        Value::Symbol("action".into()),
                        Value::T,
                    ]),
                    Value::list([
                        Value::Symbol("all-completions".into()),
                        Value::Symbol("string".into()),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::Symbol("fun".into()),
                            Value::Symbol("string".into()),
                        ]),
                        Value::Symbol("pred".into()),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("action".into()),
                    Value::list([
                        Value::Symbol("test-completion".into()),
                        Value::Symbol("string".into()),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::Symbol("fun".into()),
                            Value::Symbol("string".into()),
                        ]),
                        Value::Symbol("pred".into()),
                    ]),
                ]),
                Value::list([
                    Value::T,
                    Value::list([
                        Value::Symbol("try-completion".into()),
                        Value::Symbol("string".into()),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::Symbol("fun".into()),
                            Value::Symbol("string".into()),
                        ]),
                        Value::Symbol("pred".into()),
                    ]),
                ]),
            ]),
        ])],
        shared_env(Vec::new()),
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

fn builtin_pcomplete_autoload_file(name: &str) -> Option<&'static str> {
    match name {
        "pcomplete/cvs" => Some("pcmpl-cvs"),
        "pcomplete/git" => Some("pcmpl-git"),
        "pcomplete/gzip"
        | "pcomplete/bzip2"
        | "pcomplete/make"
        | "pcomplete/tar"
        | "pcomplete/find"
        | "pcomplete/awk"
        | "pcomplete/gpg"
        | "pcomplete/gdb"
        | "pcomplete/emacs"
        | "pcomplete/emacsclient" => Some("pcmpl-gnu"),
        "pcomplete/kill"
        | "pcomplete/umount"
        | "pcomplete/mount"
        | "pcomplete/systemctl"
        | "pcomplete/journalctl" => Some("pcmpl-linux"),
        "pcomplete/rpm" | "pcomplete/dnf" => Some("pcmpl-rpm"),
        "pcomplete/cd" | "pcomplete/rmdir" | "pcomplete/rm" | "pcomplete/xargs"
        | "pcomplete/time" | "pcomplete/which" | "pcomplete/cat" | "pcomplete/tac"
        | "pcomplete/nl" | "pcomplete/od" | "pcomplete/base32" | "pcomplete/basenc"
        | "pcomplete/fmt" | "pcomplete/pr" | "pcomplete/fold" | "pcomplete/head"
        | "pcomplete/tail" | "pcomplete/split" | "pcomplete/csplit" | "pcomplete/wc"
        | "pcomplete/sum" | "pcomplete/cksum" | "pcomplete/b2sum" | "pcomplete/md5sum"
        | "pcomplete/sort" | "pcomplete/shuf" | "pcomplete/uniq" | "pcomplete/comm"
        | "pcomplete/ptx" | "pcomplete/tsort" | "pcomplete/cut" | "pcomplete/paste"
        | "pcomplete/join" | "pcomplete/tr" | "pcomplete/expand" | "pcomplete/unexpand"
        | "pcomplete/ls" | "pcomplete/cp" | "pcomplete/dd" | "pcomplete/install"
        | "pcomplete/mv" | "pcomplete/shred" | "pcomplete/ln" | "pcomplete/mkdir"
        | "pcomplete/mkfifo" | "pcomplete/mknod" | "pcomplete/readlink" | "pcomplete/chown"
        | "pcomplete/chgrp" | "pcomplete/chmod" | "pcomplete/touch" | "pcomplete/df"
        | "pcomplete/du" | "pcomplete/stat" | "pcomplete/sync" | "pcomplete/truncate"
        | "pcomplete/echo" | "pcomplete/test" | "pcomplete/tee" | "pcomplete/basename"
        | "pcomplete/dirname" | "pcomplete/pathchk" | "pcomplete/mktemp" | "pcomplete/realpath"
        | "pcomplete/id" | "pcomplete/groups" | "pcomplete/who" | "pcomplete/date"
        | "pcomplete/nproc" | "pcomplete/uname" | "pcomplete/hostname" | "pcomplete/uptime"
        | "pcomplete/chcon" | "pcomplete/runcon" | "pcomplete/chroot" | "pcomplete/env"
        | "pcomplete/nice" | "pcomplete/nohup" | "pcomplete/stdbuf" | "pcomplete/timeout"
        | "pcomplete/numfmt" | "pcomplete/seq" | "pcomplete/ssh" | "pcomplete/scp"
        | "pcomplete/telnet" | "pcomplete/sudo" | "pcomplete/doas" => Some("pcmpl-unix"),
        "pcomplete/tex" | "pcomplete/luatex" | "pcomplete/tlmgr" | "pcomplete/rg"
        | "pcomplete/ack" | "pcomplete/ag" | "pcomplete/bcc32" | "pcomplete/rclone" => {
            Some("pcmpl-x")
        }
        _ => None,
    }
}

pub(crate) fn builtin_autoload_function(name: &str) -> Option<Value> {
    if let Some(file) = builtin_pcomplete_autoload_file(name) {
        return Some(builtin_file_autoload(file, Value::Nil));
    }
    match name {
        "command-line-1" => Some(preloaded_command_line_1()),
        "completion-table-dynamic" => Some(preloaded_completion_table_dynamic()),
        "cl-assoc-if" | "cl-assoc-if-not" | "cl-delete-duplicates" => {
            Some(builtin_file_autoload("cl-seq", Value::Nil))
        }
        "cl-print-to-string-with-limit" => Some(builtin_file_autoload("cl-print", Value::Nil)),
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
        "define-skeleton" => Some(builtin_macro_autoload("skeleton")),
        // GNU dumps elisp-mode.el, so callers such as Eshell can invoke its
        // completion entry point without an explicit require.  Loading the
        // upstream library lazily preserves that startup contract here.
        "elisp-completion-at-point" => Some(builtin_file_autoload("elisp-mode", Value::Nil)),
        "eval-defun" => Some(preloaded_eval_defun()),
        "fill-paragraph" => Some(builtin_file_autoload("fill", Value::Nil)),
        "fill-region" => Some(builtin_file_autoload("fill", Value::Nil)),
        "find-lisp-object-file-name" => Some(builtin_file_autoload("help-fns", Value::Nil)),
        "gv-define-expander" => Some(builtin_macro_autoload("gv")),
        "gv-ref" => Some(builtin_macro_autoload("gv")),
        "gv-deref" => Some(builtin_file_autoload("gv", Value::Nil)),
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
        // GNU preloads newcomment.el.
        "comment-indent"
        | "comment-indent-default"
        | "indent-for-comment"
        | "comment-normalize-vars"
        | "comment-search-forward" => Some(builtin_file_autoload("newcomment", Value::Nil)),
        "common-lisp-indent-function" => Some(builtin_file_autoload("cl-indent", Value::Nil)),
        // GNU loaddefs.el autoloads this public cus-edit setter.  Keep the
        // implementation in cus-edit.el; require_feature_with_target already
        // loads that library's dumped dependencies in startup order.
        "customize-set-value" => Some(builtin_file_autoload("cus-edit", Value::T)),
        // GNU preloads nadvice.el; the old advice.el is autoloaded.
        "add-function" | "remove-function" => Some(builtin_macro_autoload("nadvice")),
        // advice-add/remove/member-p defer to GNU nadvice.el when its file
        // is loadable; the native registry arms remain the no-file fallback
        // (call_function_value falls back to the builtin when the autoload
        // target is missing).
        "advice-add"
        | "advice-remove"
        | "advice-member-p"
        | "advice-function-member-p"
        | "advice-function-mapc"
        | "advice--add-function"
        | "advice--remove-function"
        | "advice-eval-interactive-spec" => Some(builtin_file_autoload("nadvice", Value::Nil)),
        "defadvice" => Some(builtin_macro_autoload("advice")),
        "ad-activate" | "ad-deactivate" | "ad-add-advice" | "ad-is-active" => {
            Some(builtin_file_autoload("advice", Value::Nil))
        }
        // GNU loaddefs.el autoloads pp.el's complete public entry-point
        // surface.  In particular, Eshell uses pp-to-string when an
        // expansion produces a list before anything has required `pp'.
        "pp-to-string" | "pp" | "pp-display-expression" | "pp-emacs-lisp-code" => {
            Some(builtin_file_autoload("pp", Value::Nil))
        }
        "pp-buffer"
        | "pp-eval-expression"
        | "pp-macroexpand-expression"
        | "pp-eval-last-sexp"
        | "pp-macroexpand-last-sexp" => Some(builtin_file_autoload("pp", Value::T)),
        // GNU autoloads these entry points (package.el install flows).
        // byte-recompile-directory is NOT routed to bytecomp.el: loading it
        // would shadow the native byte-compile machinery (simple_compat.el
        // defines a shim over the native byte-compile-file instead).
        "loaddefs-generate" => Some(builtin_file_autoload("loaddefs-gen", Value::Nil)),
        "tar-mode" => Some(builtin_file_autoload("tar-mode", Value::T)),
        // GNU mail-utils.el autoload (url-insert's MIME dissection needs it).
        "mail-fetch-field" => Some(builtin_file_autoload("mail-utils", Value::Nil)),
        "dired-mode" => Some(builtin_file_autoload("dired", Value::Nil)),
        "prolog-mode" => Some(builtin_file_autoload("prolog", Value::Nil)),
        "setq-connection-local" => Some(builtin_macro_autoload("files-x")),
        "sh-mode" => Some(preloaded_sh_mode()),
        // GNU preloads tabulated-list.el via buff-menu.el.
        "tabulated-list-mode" => Some(builtin_file_autoload("tabulated-list", Value::Nil)),
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

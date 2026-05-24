use super::*;

#[test]
fn nconc_rejects_non_lists_before_last_arg() {
    assert_eq!(
        eval_str("(condition-case err (nconc 'a '(c)) (wrong-type-argument (car err)))"),
        Value::symbol("wrong-type-argument")
    );
}

#[test]
fn nconc_rejects_circular_lists() {
    assert_eq!(
        eval_str(
            "(let ((x (list 'a 'b))) (setcdr (cdr x) x) (condition-case err (nconc x 'tail) (circular-list (car err))))"
        ),
        Value::symbol("circular-list")
    );
}

#[test]
fn mapcan_concatenates_results() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str("(equal (mapcan #'list (list 1 2 3)) '(1 2 3))"),
            Value::T
        );
    });
}

#[test]
fn mapcan_mutates_mapped_lists_destructively() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let ((data (list (list 'foo) (list 'bar))))
                       (and
                        (equal (mapcan #'identity data) '(foo bar))
                        (equal data '((foo bar) (bar)))))"
            ),
            Value::T
        );
    });
}

#[test]
fn sxhash_eql_matches_equal_bignums() {
    assert_eq!(
        eval_str(
            "(let* ((a (1+ most-positive-fixnum)) (b (+ most-positive-fixnum 1))) (= (sxhash-eql a) (sxhash-eql b)))"
        ),
        Value::T
    );
}

#[test]
fn sort_coding_systems_uses_priority_order() {
    assert_eq!(
        eval_str(
            "(progn (set-coding-system-priority 'utf-8 'iso-latin-1) (sort-coding-systems '(iso-latin-1 undecided utf-8)))"
        ),
        Value::list([
            Value::symbol("utf-8"),
            Value::symbol("iso-latin-1"),
            Value::symbol("undecided"),
        ])
    );
}

#[test]
fn defgroup_tracks_current_group_and_members() {
    let mut interp = Interpreter::new();
    interp.set_current_load_file(Some("/tmp/custom-group.el".into()));
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (defgroup demo nil \"Doc.\" :prefix \"demo-\")
                   (custom-add-to-group 'demo 'demo-option 'custom-variable)
                   (list (custom-current-group)
                         (equal (get 'demo 'custom-prefix) \"demo-\")
                         (get 'demo 'custom-group)))"
        ),
        Value::list([
            Value::symbol("demo"),
            Value::T,
            Value::list([Value::list([
                Value::symbol("demo-option"),
                Value::symbol("custom-variable"),
            ])]),
        ])
    );
}

#[test]
fn defcustom_records_version_and_group_membership() {
    assert_eq!(
        eval_str(
            "(progn
               (defgroup sample-custom-parent nil \"Doc.\")
               (defcustom sample-custom-versioned nil \"Doc.\"
                 :type 'boolean
                 :version \"31.1\"
                 :group 'sample-custom-parent)
               (list (equal (get 'sample-custom-versioned 'custom-version) \"31.1\")
                     (get 'sample-custom-parent 'custom-group)
                     custom-versions-load-alist))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::list([
                Value::symbol("sample-custom-versioned"),
                Value::symbol("custom-variable"),
            ])]),
            Value::Nil,
        ])
    );
}

#[test]
fn mapatoms_scans_standard_obarray_symbols() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defcustom sample-mapatoms-option nil "Doc." :type 'boolean)
                  (intern "sample-mapatoms-interned")
                  (let (found)
                    (mapatoms
                     (lambda (symbol)
                       (when (string-prefix-p "sample-mapatoms-" (symbol-name symbol))
                         (push symbol found))))
                    (sort found (lambda (left right)
                                  (string< (symbol-name left)
                                           (symbol-name right))))))"#
        ),
        Value::list([
            Value::symbol("sample-mapatoms-interned"),
            Value::symbol("sample-mapatoms-option"),
        ])
    );
}

#[test]
fn booleanp_matches_nil_and_t_only() {
    assert_eq!(
        eval_str("(list (booleanp nil) (booleanp t) (booleanp 0) (booleanp 'false))"),
        Value::list([Value::T, Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn make_obsolete_variable_records_byte_obsolete_property() {
    assert_eq!(
        eval_str(
            "(progn
               (make-obsolete-variable 'sample-old-option 'sample-new-option \"31.1\" 'get)
               (get 'sample-old-option 'byte-obsolete-variable))"
        ),
        Value::list([
            Value::symbol("sample-new-option"),
            Value::symbol("get"),
            Value::String("31.1".into()),
        ])
    );
}

#[test]
fn batch_window_hscroll_defaults_to_zero() {
    assert_eq!(eval_str("(window-hscroll)"), Value::Integer(0));
}

#[test]
fn dolist_with_progress_reporter_uses_dolist_semantics() {
    assert_eq!(
        eval_str(
            "(let ((seen nil) (reporter nil))
               (dolist-with-progress-reporter (item '(1 2 3) (nreverse seen))
                   (setq reporter 'evaluated)
                 (push (list reporter item) seen)))"
        ),
        Value::list([
            Value::list([Value::symbol("evaluated"), Value::Integer(1)]),
            Value::list([Value::symbol("evaluated"), Value::Integer(2)]),
            Value::list([Value::symbol("evaluated"), Value::Integer(3)]),
        ])
    );
}

#[test]
fn cl_letf_rebinds_symbol_property_get_places() {
    assert_eq!(
        eval_str(
            "(progn
               (put 'sample-cl-letf-prop 'tag 'outer)
               (list
                (cl-letf (((get 'sample-cl-letf-prop 'tag) 'inner))
                  (list (get 'sample-cl-letf-prop 'tag)
                        (progn (put 'sample-cl-letf-prop 'tag 'changed)
                               (get 'sample-cl-letf-prop 'tag))))
                (get 'sample-cl-letf-prop 'tag)))"
        ),
        Value::list([
            Value::list([Value::symbol("inner"), Value::symbol("changed")]),
            Value::symbol("outer"),
        ])
    );
}

#[test]
fn setopt_warns_when_value_does_not_match_custom_type() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defcustom sample-setopt-number 0 "Doc." :type 'number)
                  (with-current-buffer (get-buffer-create "*Warnings*")
                    (let ((inhibit-read-only t))
                      (erase-buffer))
                    (setopt sample-setopt-number :bad)
                    (string-search "Value `:bad' does not match type number"
                                   (buffer-string))))"#
        ),
        Value::Integer(9)
    );
}

#[test]
fn cl_with_gensyms_produces_unique_bindings() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defmacro sample-cl-with-gensyms (value)
                    (cl-with-gensyms (tmp)
                      `(let ((,tmp ,value))
                         ,tmp)))
                  (let ((tmp 99))
                    (sample-cl-with-gensyms 42)))
                "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn cl_case_matches_atoms_lists_and_fallbacks() {
    assert_eq!(
        eval_str(
            r#"
                (list
                 (cl-case 'zip
                   ((tar ar) 'other)
                   (zip 'zip))
                 (cl-case 2
                   ((1 3) 'miss)
                   ((2 4) 'hit))
                 (cl-case nil
                   (nil 'impossible)
                   (otherwise 'fallback)))
                "#
        ),
        Value::list([
            Value::symbol("zip"),
            Value::symbol("hit"),
            Value::symbol("fallback"),
        ])
    );
}

#[test]
fn cl_case_evaluates_expression_once() {
    assert_eq!(
        eval_str(
            r#"
                (let ((count 0))
                  (list
                   (cl-case (progn (setq count (+ count 1)) 'zip)
                     (zip 'matched)
                     (otherwise 'missed))
                   count))
                "#
        ),
        Value::list([Value::symbol("matched"), Value::Integer(1)])
    );
}

#[test]
fn dired_shell_command_confirmation_positions_match_upstream() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"(progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (list (dired--need-confirm-positions "ls ? ./?" "?")
                       (dired--need-confirm-positions "ls ./? ?" "?")
                       (dired--need-confirm-positions "ls * ./*" "*")
                       (dired--need-confirm-positions "ls * *" "*")
                       (dired--need-confirm-positions "ls ? ?" "?")
                       (dired--need-confirm-positions "ls ? ./`?`" "?")))"#
        ),
        Value::list([
            Value::list([Value::Integer(7)]),
            Value::list([Value::Integer(5)]),
            Value::list([Value::Integer(7)]),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn dired_shell_command_confirms_unsafe_substitution_marks() {
    let dir = std::env::temp_dir().join(format!("emaxx-dired-shell-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let file = dir.join("foo");
    std::fs::write(&file, "contents").expect("write temp file");
    let dir_text = format!("{}/", dir.to_string_lossy()).replace('\\', "\\\\");
    let file_text = file.to_string_lossy().replace('\\', "\\\\");
    let expr = format!(
        r#"(progn
             (setq noninteractive t)
             (require 'ert-x)
             (require 'dired-aux)
             (let ((files (list "{file_text}"))
                   (temporary-file-directory "{dir_text}"))
               (cl-letf (((symbol-function 'read-char-from-minibuffer) 'error)
                         ((symbol-function 'dired-run-shell-command)
                          (lambda (_command) nil)))
                 (dired temporary-file-directory)
                 (dired-goto-file "{file_text}")
                 (list
                  (condition-case err (dired-do-shell-command "ls ? ./?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ./? ?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ? ?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls * ./*" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls * *" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ? ./`?`" nil files)
                    (error (car err)))))))"#
    );
    assert_eq!(
        eval_str_with_upstream_load_path(&expr),
        Value::list([
            Value::Symbol("error".into()),
            Value::Symbol("error".into()),
            Value::Nil,
            Value::Symbol("error".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_create_destination_dirs_controls_copy_and_rename() {
    let dir = std::env::temp_dir().join(format!("emaxx-dired-dest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let dir_text = dir.to_string_lossy().replace('\\', "\\\\");
    let expr = format!(
        r#"(progn
             (setq noninteractive t)
             (require 'dired-aux)
             (let (results)
               (dolist (mode '(always nil ask-yes ask-no))
                 (let* ((scenario-dir (expand-file-name
                                       (symbol-name mode)
                                       "{dir_text}"))
                        (from (make-temp-file "emaxx-dired-from"))
                        (dired-create-destination-dirs
                         (cond ((eq mode 'always) 'always)
                               ((eq mode 'nil) nil)
                               (t 'ask)))
                        (to-cp (expand-file-name
                                "foo-cp"
                                (file-name-as-directory
                                 (expand-file-name
                                  "bar" scenario-dir))))
                        (to-mv (expand-file-name
                                "foo-mv"
                                (file-name-as-directory
                                 (expand-file-name
                                  "qux" scenario-dir)))))
                   (cl-letf (((symbol-function 'yes-or-no-p)
                              (lambda (_prompt) (eq mode 'ask-yes))))
                     (push
                      (list mode
                            (condition-case nil
                                (progn
                                  (dired-copy-file-recursive from to-cp nil)
                                  (file-exists-p to-cp))
                              (error :error))
                            (condition-case nil
                                (progn
                                  (dired-rename-file from to-mv nil)
                                  (file-exists-p to-mv))
                              (error :error)))
                      results))))
               (nreverse results)))"#
    );
    assert_eq!(
        eval_str_with_upstream_load_path(&expr),
        Value::list([
            Value::list([Value::symbol("always"), Value::T, Value::T]),
            Value::list([Value::Nil, Value::symbol(":error"), Value::symbol(":error"),]),
            Value::list([Value::symbol("ask-yes"), Value::T, Value::T]),
            Value::list([
                Value::symbol("ask-no"),
                Value::symbol(":error"),
                Value::symbol(":error"),
            ]),
        ])
    );
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_do_create_files_recreates_destination_directory() {
    let result = eval_str_with_upstream_load_path(
        r#"(progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (let* ((target-dir (make-temp-file "emaxx-dired-target" 'dir))
                        (file1 (make-temp-file "bug30624_file1"))
                        (file2 (make-temp-file "bug30624_file2"))
                        (dired-create-destination-dirs 'always)
                        (inhibit-message t)
                        (buf (dired temporary-file-directory)))
                   (unwind-protect
                       (progn
                         (delete-directory target-dir)
                         (cl-letf (((symbol-function 'dired-mark-read-file-name)
                                    (lambda (&rest _) target-dir)))
                           (dired-revert)
                           (dired-mark-files-regexp "bug30624_file")
                           (condition-case err
                               (dired-do-create-files 'copy 'dired-copy-file "Copy" nil)
                             (error err))))
                     (ignore-errors (delete-directory target-dir 'recursive))
                     (ignore-errors (delete-file file1))
                     (ignore-errors (delete-file file2))
                     (ignore-errors (kill-buffer buf)))))"#,
    );
    assert!(result.is_truthy(), "{result:?}");
}

#[test]
fn dired_highlights_unsubstituted_shell_metacharacters() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"(progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (let* ((command "sed -r -e 's/oo?/a/' -e 's/oo?/a/' ? `?`")
                        (result (dired--highlight-no-subst-chars
                                 (dired--need-confirm-positions command "?")
                                 command
                                 t))
                        (lines (split-string result "\n"))
                        (line (car lines)))
                   (list (dired--need-confirm-positions command "?")
                         (= (length lines) 2)
                         (string-match-p
                          (regexp-quote "               ^             ^")
                          (cadr lines))
                         (get-text-property 15 'face line)
                         (get-text-property 29 'face line)
                         (text-property-not-all 1 14 'face nil line)
                         (text-property-not-all 16 28 'face nil line)
                         (text-property-not-all 30 (length line) 'face nil line))))"#
        ),
        Value::list([
            Value::list([Value::Integer(29), Value::Integer(15)]),
            Value::T,
            Value::Integer(0),
            Value::symbol("warning"),
            Value::symbol("warning"),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn dired_reuses_directory_buffer_and_preserves_file_point() {
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory test-dir
               (let ((dired-auto-revert-buffer t)
                     buffers
                     step)
                 (condition-case err
                     (progn
                       (setq step 'initial)
                       (should-not (dired-buffers-for-dir test-dir))
                       (setq step 'find-directory)
                       (with-current-buffer (find-file-noselect test-dir)
                         (make-directory "test-subdir"))
                       (setq step 'check-eob)
                       (with-current-buffer (car (dired-buffers-for-dir test-dir))
                         (unless (eobp) (error "not at eob")))
                       (setq step 'dired)
                       (push (dired test-dir) buffers)
                       (setq step 'buffer-count)
                       (unless (eq 1 (length (dired-buffers-for-dir test-dir)))
                         (error "extra dired buffer"))
                       (let ((buf (current-buffer))
                             (pt1 (point))
                             (test-file (concat (file-name-as-directory "test-subdir")
                                                "test-file")))
                         (setq step 'write-region)
                         (write-region "Test" nil test-file nil 'silent nil 'excl)
                         (setq step 'file-at-point)
                         (let ((actual (dired-file-name-at-point))
                               (expected (concat test-dir
                                                 (file-name-as-directory "test-subdir"))))
                           (unless (equal actual expected)
                             (error "not on subdir: %S expected %S" actual expected)))
                         (setq step 'find-file)
                         (push (dired-find-file) buffers)
                         (let ((pt2 (point)))
                           (setq step 'pop-back)
                           (pop-to-buffer-same-window buf)
                           (unless (eq (point) pt1)
                             (error "lost directory point"))
                           (setq step 'find-again)
                           (push (dired-find-file) buffers)
                           (list :ok (eq (point) pt2)))))
                   (error (list step err))))))"#,
    );
    assert_eq!(result, Value::list([Value::symbol(":ok"), Value::T]));
}

#[test]
fn dired_delete_empty_marked_directories_removes_entries() {
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory test-dir
                 (let* ((dired-deletion-confirmer (lambda (_) "yes"))
                      (inhibit-message t)
                      (default-directory test-dir)
                      (buf nil))
                 (dotimes (i 2) (make-directory (format "empty-dir-%d" i)))
                 (make-directory "zeta-empty-dir")
                 (unwind-protect
                     (progn
                       (setq buf (dired default-directory))
                       (dired-toggle-marks)
                       (let ((before (dired-get-marked-files)))
                         (dired-do-delete nil)
                         (list (= 3 (length before))
                               (dired-get-marked-files)
                               (file-exists-p (expand-file-name "empty-dir-0" test-dir))
                               (file-exists-p (expand-file-name "zeta-empty-dir" test-dir)))))
                   (when (buffer-live-p buf) (kill-buffer buf))))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::Nil, Value::Nil, Value::Nil])
    );
}

#[test]
fn dired_revert_preserves_line_when_header_length_changes() {
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory top-dir
               (let* ((subdir (expand-file-name "subdir" top-dir))
                      (header-len-fn (lambda ()
                                       (save-excursion
                                         (goto-char 1)
                                         (forward-line 1)
                                         (- (pos-eol) (point)))))
                      orig-len len diff pos line-nb)
                 (make-directory subdir 'parents)
                 (with-current-buffer (dired-noselect subdir)
                   (setq orig-len (funcall header-len-fn)
                         pos (point)
                         line-nb (line-number-at-pos))
                   (make-directory "subdir" t)
                   (dired-revert)
                   (save-excursion
                     (goto-char 1)
                     (forward-line 1)
                     (let ((inhibit-read-only t)
                           (new-header "  test-bug27968"))
                       (delete-region (point) (pos-eol))
                       (when (= orig-len (length new-header))
                         (setq new-header (concat new-header " :-)")))
                       (insert new-header)))
                   (setq len (funcall header-len-fn)
                         diff (- len orig-len))
                   (list (not (zerop diff))
                         (= line-nb
                            (line-number-at-pos)
                            (line-number-at-pos (+ pos diff)))
                         (dired-get-filename 'local t))))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::String("subdir".into())])
    );
}

#[test]
fn line_move_moves_by_logical_lines_in_batch() {
    let result = eval_str(
        r#"(with-temp-buffer
             (insert "alpha\nbeta\ngamma\n")
             (goto-char (point-min))
             (let ((first (line-move 1 t))
                   (second-line (line-number-at-pos))
                   (too-far (line-move 20 t)))
               (list first second-line too-far (line-number-at-pos))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::Integer(2), Value::Nil, Value::Integer(4)])
    );
}

#[test]
fn directory_empty_p_and_temporary_file_directory_match_files_helpers() {
    let result = eval_str(
        r#"(let* ((tmp (temporary-file-directory))
                  (dir (make-temp-file "emaxx-empty-dir-" t)))
             (unwind-protect
                 (let ((missing (expand-file-name "missing" dir)))
                   (list (stringp tmp)
                         (file-name-absolute-p tmp)
                         (directory-empty-p missing)
                         (directory-empty-p dir)
                         (progn
                           (make-empty-file (expand-file-name "child" dir))
                           (directory-empty-p dir))))
               (delete-directory dir t)))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::Nil, Value::T, Value::Nil])
    );
}

#[test]
fn cl_case_rejects_misplaced_otherwise() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new("(cl-case 'zip (otherwise 'fallback) (zip 'hit))")
        .read()
        .unwrap()
        .unwrap();
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "error");
    assert_eq!(error.to_string(), "Misplaced t or `otherwise' clause");
}

#[test]
fn sqlite_execute_surfaces_sql_input_errors_as_sqlite_error() {
    assert_eq!(
        eval_str(
            r#"
                (let ((db (sqlite-open)))
                  (sqlite-execute db "create table test (a)")
                  (should-error
                   (sqlite-execute db "insert into test values (fake(2))")
                   :type 'sqlite-error))
                "#
        ),
        Value::list([
            Value::symbol("sqlite-error"),
            Value::list([
                Value::String("SQL logic error".into()),
                Value::String("no such function: fake".into()),
                Value::Integer(1),
                Value::Integer(1),
            ]),
        ])
    );
}

#[test]
fn backtrace_frames_from_current_thread_returns_live_frames() {
    let mut interp = Interpreter::new();
    let current_thread = interp.current_thread_value();
    interp.push_backtrace_frame(Value::Symbol("sample-backtrace-frame".into()), Vec::new());

    assert_eq!(
        interp.thread_backtrace_frames_snapshot(interp.resolve_thread_id(&current_thread).unwrap()),
        vec![(
            Value::Symbol("sample-backtrace-frame".into()),
            Vec::new(),
            false,
        )]
    );
}

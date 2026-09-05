use super::*;

fn assert_finalizer_contract(program: &str, expected: &str) {
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let form = Reader::new(program)
        .read()
        .expect("finalizer contract parses")
        .expect("finalizer contract contains a form");
    let value = interp
        .eval(&form, &mut env)
        .expect("finalizer contract evaluates");
    let printed = call(&mut interp, "prin1-to-string", &[value], &mut env)
        .expect("print finalizer contract result");
    assert_eq!(
        string_text(&printed).expect("printed result is text"),
        expected,
        "actual post-GC callback count: {}",
        interp.number_finalizers_run,
    );
}

#[test]
fn finalizer_constructor_uses_c_functionp_not_the_lisp_function_cell() {
    assert_finalizer_contract(
        r#"
      (let ((original (symbol-function 'functionp)))
        (unwind-protect
            (progn
              (fset 'functionp #'(lambda (object) t))
              (list (condition-case data (make-finalizer 42) (error data))
                    (condition-case data (make-finalizer 'if) (error data))
                    (type-of (make-finalizer #'car))
                    (eq (make-finalizer #'car) (make-finalizer #'car))))
          (fset 'functionp original)))
    "#,
        "((wrong-type-argument functionp 42) (wrong-type-argument functionp if) finalizer nil)",
    );
}

#[test]
fn finalizer_callback_roots_precede_weak_sweeping_and_release_after_running() {
    // GNU scans live native stacks conservatively. Allocate on a joined
    // worker so stale constructor return words cannot be roots in the
    // collecting thread; this is not a collect-until-the-test-passes loop.
    assert_finalizer_contract(
        r#"
      (eval
       '(let ((events nil) (key nil)
              (table (make-hash-table :test 'eq :weakness 'key))
              (keeper nil) (inhibit-quit nil))
          (thread-join
           (make-thread
            #'(lambda ()
                (setq key (cons nil nil))
                (puthash key 42 table)
                (setq keeper
                      (make-finalizer
                       (let ((saved key))
                         #'(lambda ()
                             (setq events
                                   (cons (list 'ran inhibit-quit (gethash saved table)) events))))))
                nil)))
          (setq key nil)
          (garbage-collect)
          (let ((before (list events (hash-table-count table))))
            (setq keeper nil)
            (garbage-collect)
            (let ((after (list events (hash-table-count table))))
              (garbage-collect)
              (list before after (hash-table-count table) inhibit-quit))))
       t)
    "#,
        "((nil 1) (((ran t 42)) 1) 0 nil)",
    );
}

#[test]
fn finalizer_cycles_resurrection_and_nested_collection_run_each_callback_once() {
    assert_finalizer_contract(
        r#"
      (eval
       '(let ((events nil) (rescued nil) (inhibit-quit nil))
          (thread-join
           (make-thread
            #'(lambda ()
                (let ((box (list nil)))
                  (setcar box
                          (make-finalizer
                           #'(lambda ()
                               (setq rescued (car box) events (cons 'a events))
                               (thread-join
                                (make-thread
                                 #'(lambda ()
                                     (make-finalizer #'(lambda () (setq events (cons 'c events))))
                                     nil)))
                               (garbage-collect)))))
                (make-finalizer #'(lambda () (setq events (cons 'b events))))
                nil)))
          (garbage-collect)
          (garbage-collect)
          (list (type-of rescued) events inhibit-quit))
       t)
    "#,
        "(finalizer (c b a) nil)",
    );
}

#[test]
fn finalizer_errors_are_logged_without_message_dispatch_and_do_not_stop_the_queue() {
    // Use F_eval's explicit nil lexical environment in both editors. Bare
    // host Interpreter::eval does not itself install a GNU evaluation mode.
    assert_finalizer_contract(
        r#"
      (eval
       '(let ((original (symbol-function 'message))
            (messages-buffer-name "*finalizer-control*")
            (message-log-max t) (message-calls 0) (callback-value nil)
            (inhibit-quit nil))
        (unwind-protect
            (progn
              (fset 'message #'(lambda (&rest args) (setq message-calls (1+ message-calls))))
              (thread-join
               (make-thread
                #'(lambda ()
                    (make-finalizer #'(lambda () (signal 'error '("finalizer-control-error"))))
                    (make-finalizer #'(lambda () (signal 'quit nil)))
                    (make-finalizer #'(lambda () (setq callback-value inhibit-quit)))
                    nil)))
              (garbage-collect)
              (list message-calls callback-value inhibit-quit
                    (save-current-buffer
                      (set-buffer (get-buffer messages-buffer-name))
                      (buffer-string))))
          (fset 'message original)))
       nil)
    "#,
        "(0 t nil \"finalizer failed: (error \\\"finalizer-control-error\\\")\nfinalizer failed: (quit)\n\")",
    );
}

#[test]
fn finalizer_throw_unwinds_quit_binding_and_keeps_remaining_callbacks_queued() {
    assert_finalizer_contract(
        r#"
      (let ((event nil) (inhibit-quit nil))
        (list
         (catch 'escape
           (make-finalizer #'(lambda () (throw 'escape 'escaped)))
           (make-finalizer #'(lambda () (setq event 'after)))
           (garbage-collect)
           'not-escaped)
         (progn (garbage-collect) event)
         inhibit-quit))
    "#,
        "(escaped after nil)",
    );
}

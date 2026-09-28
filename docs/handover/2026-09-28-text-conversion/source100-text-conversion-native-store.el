(let (states events watcher)
  (setq watcher (lambda (_symbol _value operation where)
                  (push (list operation (if (bufferp where) 'buffer where)) events)))
  (set-default 'text-conversion-style '(default-293))
  (with-temp-buffer
    (add-variable-watcher 'text-conversion-style watcher)
    (unwind-protect
        (progn
          (push (list 'stored (set-text-conversion-style [native-307] 'after-key)
                      text-conversion-style (default-value 'text-conversion-style)
                      (local-variable-p 'text-conversion-style)
                      (assq 'text-conversion-style (buffer-local-variables))
                      events) states)
          (let ((text-conversion-style '(temporary-311)))
            (push (list 'bound text-conversion-style
                        (default-value 'text-conversion-style)) states))
          (push (list 'restored text-conversion-style
                      (default-value 'text-conversion-style)) states)
          (set-default 'text-conversion-style '(replaced-313))
          (push (list 'default-propagated text-conversion-style) states)
          (make-local-variable 'text-conversion-style)
          (push (list 'arbitrary (set-text-conversion-style 317)
                      text-conversion-style
                      (local-variable-p 'text-conversion-style)) states)
          (makunbound 'text-conversion-style)
          (push (list 'detached (set-text-conversion-style '(native-331) t)
                      (boundp 'text-conversion-style)
                      (assq 'text-conversion-style (buffer-local-variables))) states)
          (setq text-conversion-style '(plain-337))
          (set-text-conversion-style [native-347])
          (push (list 'independent text-conversion-style
                      (default-value 'text-conversion-style)
                      (assq 'text-conversion-style (buffer-local-variables))) states))
      (remove-variable-watcher 'text-conversion-style watcher)))
  (list (nreverse states) (nreverse events)))

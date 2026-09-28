;;; -*- lexical-binding: t; -*-
(defmacro emaxx-eager-root-probe ()
  (let ((payload (vector (make-symbol "fresh-eager-symbol")
                         (make-string 257 ?q)
                         (record 'eager-payload (list 17 29)))))
    (list 'progn '(garbage-collect)
          (list 'setq 'emaxx-eager-root-result (list 'quote payload)))))
(emaxx-eager-root-probe)

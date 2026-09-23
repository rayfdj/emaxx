;;; -*- lexical-binding: t; -*-
;; Identical Lisp input for every editor; result checks are outside timing.
;; Default GC tuning remains in force, with an explicit GC before each case.
(defun audit-measure (name thunk expected)
  (garbage-collect)
  (let ((before gcs-done) (start (float-time)) (result nil))
    (setq result (funcall thunk))
    (let ((seconds (- (float-time) start)) (collections (- gcs-done before)))
      (unless (equal result expected)
        (error "%s returned %S; expected %S" name result expected))
      (princ (format "%s %.6f %d %S\n" name seconds collections result)))))

(defvar audit-loop
  '(let ((n 0))
     (let ((i 0))
       (while (< i 2000000)
         (let* ((x (and (> i -1) i))) (if x (setq n (+ n 1))))
         (setq i (1+ i))))
     n))
(audit-measure "interpreted-lexical" (lambda () (eval audit-loop t)) 2000000)
(audit-measure "interpreted-dynamic" (lambda () (eval audit-loop nil)) 2000000)

(defun audit-add (a b) (+ a b))
(defvar audit-call
  '(let ((i 0) (n 0))
     (while (< i 1000000) (setq n (audit-add n 1)) (setq i (1+ i))) n))
(audit-measure "interpreted-calls" (lambda () (eval audit-call t)) 1000000)

(defun audit-id (x) x)
(defun audit-byte-loop (n)
  (let ((i 0) (sum 0))
    (while (< i n) (setq sum (+ sum (audit-id i)) i (1+ i)))
    sum))
(mapc #'byte-compile '(audit-id audit-byte-loop))
(audit-measure "bytecode-calls" (lambda () (audit-byte-loop 2000000)) 1999999000000)

(audit-measure "conses-6m"
               (lambda () (dotimes (i 6000000) (cons i i)) 'done) 'done)
(audit-measure "mapcar-2m"
               (lambda ()
                 (let ((result nil))
                   (dotimes (_ 20)
                     (setq result (mapcar #'1+ (number-sequence 1 100000))))
                   (list (length result) (car result) (car (last result)))))
               '(100000 2 100001))
(audit-measure "gc-idle-10"
               (lambda () (dotimes (_ 10) (garbage-collect)) 'done) 'done)
(princ (format "gc-settings %S %S\n" gc-cons-threshold gc-cons-percentage))

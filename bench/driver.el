;;; driver.el --- emaxx vs GNU benchmark driver -*- lexical-binding: t; -*-
;;
;; Usage (from the repo root; identical for both engines):
;;   emacs --batch -l bench/driver.el
;;   EMAXX_BYTECODE_VM=1 target/release/emaxx --batch -l bench/driver.el
;;
;; Byte-compile the kernels first with GNU Emacs so both engines execute
;; the same bytecode:
;;   emacs --batch -f batch-byte-compile bench/bench-kernels-1.el bench/bench-kernels-2.el
;;
;; Each kernel runs five times in-process; the minimum is the
;; noise-robust statistic (interference only ever adds time).  Results
;; append to bench/results.txt.

(defvar bench--dir (file-name-directory (or load-file-name buffer-file-name)))
(load (expand-file-name "bench-kernels-1.elc" bench--dir) nil t)
(load (expand-file-name "bench-kernels-2.elc" bench--dir) nil t)

(defvar bench--out (expand-file-name "results.txt" bench--dir))

(defun bench--time (label thunk)
  (let ((times nil))
    (dotimes (_ 5)
      (let ((start (float-time)))
        (funcall thunk)
        (push (- (float-time) start) times)))
    (setq times (sort times #'<))
    (write-region (format "%s min %.4f median %.4f\n" label (nth 0 times) (nth 2 times))
                  nil bench--out t 'quiet)))

(write-region (format "engine %s\n" (type-of (symbol-function 'bench-fib)))
              nil bench--out t 'quiet)
(bench--time "fib-30"        (lambda () (bench-fib 30)))
(bench--time "loop-sum-20M"  (lambda () (bench-loop-sum 20000000)))
(bench--time "list-build-2M" (lambda () (bench-list-build 2000000)))
(bench--time "assq-300kx400" (lambda ()
  (let ((alist (let (a) (dotimes (i 400 a) (push (cons i i) a))))
        (keys (let (k) (dotimes (i 300000 k) (push (% i 800) k)))))
    (bench-assq-scan alist keys))))
(bench--time "string-30k"    (lambda () (bench-string-ops 30000)))
(bench--time "float-8M"      (lambda () (bench2-float 8000000)))
(bench--time "vector-10M"    (lambda () (bench2-vector 10000000)))
(bench--time "hash-3M"       (lambda () (bench2-hash 3000000)))
(bench--time "mapcar-25k"    (lambda () (bench2-mapcar 25000)))
(bench--time "sort-5k"       (lambda () (bench2-sort 5000)))
(bench--time "regex-500k"    (lambda () (bench2-regex 500000)))
(bench--time "buffer-2500"   (lambda () (bench2-buffer 2500)))
(bench--time "format-1M"     (lambda () (bench2-format 1000000)))
(bench--time "plist-5M"      (lambda () (bench2-plist 5000000)))

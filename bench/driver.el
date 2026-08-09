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

(defun bench--assert-equal (label actual expected)
  (unless (equal actual expected)
    (error "Benchmark preflight %s failed: expected %S, got %S"
           label expected actual)))

;; A performance result is comparable only after the same compiled kernels
;; produce GNU's values.  Keep these inputs small so validation is negligible
;; beside the timed workloads, but cover every kernel and the mutation-heavy
;; vector/list/sort paths in particular.
(bench--assert-equal "fib" (bench-fib 10) 55)
(bench--assert-equal "loop" (bench-loop-sum 10) 45)
(bench--assert-equal "list-build" (bench-list-build 5) '(0 1 2 3 4))
(bench--assert-equal "assq"
                     (bench-assq-scan '((a . 1) (b . 2)) '(a c b)) 2)
(bench--assert-equal "string" (bench-string-ops 5) 5)
(unless (< (abs (- (bench2-float 1) 0.25000005000000003)) 1e-12)
  (error "Benchmark preflight float failed"))
(bench--assert-equal "vector" (bench2-vector 300) 18406)
(bench--assert-equal "hash" (bench2-hash 20) 0)
(bench--assert-equal "mapcar" (bench2-mapcar 2) 20200)
(let ((sorted (bench2-sort 1)))
  (bench--assert-equal
   "sort" (list (length sorted) (car sorted) (car (last sorted))
                (apply #'+ sorted))
   '(300 30 9999 1559572)))
(bench--assert-equal "regex" (bench2-regex 3) 126)
(bench--assert-equal "buffer" (bench2-buffer 1) 1890)
(bench--assert-equal "format" (bench2-format 1) 10)
(bench--assert-equal "plist" (bench2-plist 8) 36)

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

;; callgrind-diff.sh probe: the byte-code call loop (the audit's instructions an
;; iteration; the second run length is the first times three).
;; (tools/perf/callgrind-diff.sh BINARY tools/perf/cg-call-loop.el 200000 600000).
(defun micro-id (x) x)
(defun micro-call-loop () (dotimes (i 200000) (micro-id i)))
(mapc #'byte-compile '(micro-id micro-call-loop))
(let ((t0 (float-time))) (micro-call-loop) (princ (format "byte-code call loop (10M): %.3f s\n" (- (float-time) t0))))

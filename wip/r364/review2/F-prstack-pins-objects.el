;;; -*- lexical-binding: t -*-
;; every print left by a throw leaves its stack entries; what they point to
(dotimes (i 3000)
  (let ((big (list 1 (make-vector 100000 i))))
    (catch 'out (prin1 big (lambda (c) (when (= c ?1) (throw 'out nil)))))))
(garbage-collect)
(princ (format "done %S\n" (let ((rss 0)) (with-temp-buffer (insert-file-contents "/proc/self/status") (when (re-search-forward "VmRSS:[ \t]*\\([0-9]+\\)" nil t) (setq rss (string-to-number (match-string 1))))) (if (> rss 1000000) 'rss-over-1GB 'rss-under-1GB))))

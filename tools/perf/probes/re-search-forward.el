;;; -*- lexical-binding: t -*-
;; (re-search-forward "x" nil t) failing over "abc\n" from its start:
;; search_buffer_re's setup and its one failing search.
(defun probe-rsf (n) (let ((i 0) (r 0)) (while (< i n) (goto-char 1) (if (re-search-forward "x" nil t) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-rsf)
(with-temp-buffer (insert "abc\n")
  (unless (= (probe-rsf @N@) 0) (error "re-search-forward")))
(princ "DIV 1\n")

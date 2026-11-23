;;; -*- lexical-binding: t -*-
;; Six window accessors from byte code (D3).
(defun probe-win (n) (let ((i 0) (w (selected-window)) (r 0))
  (while (< i n)
    (if (window-buffer w) (setq r (1+ r)))
    (if (window-frame w) (setq r (1+ r)))
    (if (window-top-child w) (setq r (1+ r)))
    (if (window-valid-p w) (setq r (1+ r)))
    (if (window-parameter w 'probe-none) (setq r (1+ r)))
    (if (window-next-sibling w) (setq r (1+ r)))
    (setq i (1+ i))) r))
(byte-compile 'probe-win)
(princ (format "RESULT %d\n" (/ (probe-win @N@) @N@)))
(princ "DIV 6\n")

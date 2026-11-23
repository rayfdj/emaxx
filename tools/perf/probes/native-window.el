;;; -*- lexical-binding: t -*-
;; Window accessors called from native code (HANDOVER: `window-top-child'
;; 112 against 25, `window-buffer' and `window-frame' about 100 more).
(defun probe-native-window (n w)
  (let ((i 0) (r 0))
    (while (< i n)
      (if (window-buffer w) (setq r (1+ r)))
      (if (window-frame w) (setq r (1+ r)))
      (if (window-top-child w) (setq r (1+ r)))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-window)
(unless (native-comp-function-p (symbol-function 'probe-native-window)) (error "native-window: not native"))
(unless (= (probe-native-window @N@ (selected-window)) (* 2 @N@)) (error "native-window"))
(princ "DIV 3\n")

(let ((symbols-with-pos-enabled nil)
      (describe (lambda (form)
                  (mapcar (lambda (object)
                            (if (symbol-with-pos-p object)
                                (list (symbol-name (bare-symbol object))
                                      (symbol-with-pos-pos object))
                              object)) form))))
  (list (funcall describe (read-positioning-symbols "(α t nil)"))
        (with-temp-buffer
          (insert "skip (α t nil)")
          (goto-char 6)
          (let ((form (read-positioning-symbols (current-buffer))))
            (list (funcall describe form) (point))))
        (with-temp-buffer
          (insert "skip (α t nil)")
          (let* ((marker (copy-marker 6))
                 (form (read-positioning-symbols marker)))
            (list (funcall describe form) (marker-position marker))))
        (let ((chars (string-to-list "(omega t nil)")))
          (funcall describe
                   (read-positioning-symbols
                    (lambda (&optional char)
                      (if char (push char chars) (pop chars))))))))

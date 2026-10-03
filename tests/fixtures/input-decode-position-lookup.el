(save-window-excursion
  (let ((buffer (generate-new-buffer " *reader-position*")))
    (unwind-protect
        (progn
          (set-window-buffer (selected-window) buffer)
          (with-current-buffer buffer
            (insert "ab\tcd\nsecond\n")
            (goto-char 3)
            (let ((window (selected-window)))
              (list
               (mapcar
                (lambda (xy)
                  (let ((position (posn-at-x-y (car xy) (cdr xy) window t)))
                    (cons (eq window (car position)) (cdr position))))
                '((0 . 0) (1 . 0) (2 . 0) (7 . 0) (8 . 0)
                  (9 . 0) (2 . 1) (25 . 9)))
               (point)))))
      (kill-buffer buffer))))

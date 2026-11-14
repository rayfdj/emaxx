(let ((acc nil) (b (generate-new-buffer "bufname")))
  (prin1 b (lambda (c) (push c acc) (when (= c ?<) (with-current-buffer b (rename-buffer "other")))))
  (princ (concat (nreverse acc))))

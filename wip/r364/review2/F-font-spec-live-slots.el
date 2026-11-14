;; print.c:1913 reads each slot of a font spec when the walk reaches it.
(let ((spec (font-spec :family "Foo" :size 12.5 :weight 'bold)) (acc nil) (done nil))
  (prin1 spec (lambda (c) (push c acc)
                (unless done (setq done t) (font-put spec :size 99.5) (font-put spec :family "Bar") (font-put spec :weight 'light))))
  (princ (concat (nreverse acc))) (terpri))

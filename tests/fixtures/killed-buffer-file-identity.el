(let (results)
  (dolist (label '("retained-file-37" " retained file 81" "retained-file-lambda"))
    (let* ((buffer (generate-new-buffer label))
           (path (concat "/tmp/" label ".el"))
           marker)
      (unwind-protect
          (progn
            (with-current-buffer buffer
              (setq buffer-file-name path)
              (insert label)
              (setq marker (copy-marker (point-min))))
            (kill-buffer buffer)
            (garbage-collect)
            (setq results
                  (cons (list (bufferp buffer)
                              (buffer-live-p buffer)
                              (buffer-name buffer)
                              (equal (buffer-file-name buffer) path)
                              (marker-buffer marker))
                        results)))
        (when (buffer-live-p buffer) (kill-buffer buffer)))))
  (nreverse results))

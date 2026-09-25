(let* ((name (generate-new-buffer-name "buffer-create-hook-93"))
       (buffer-list-update-hook
        (list (lambda ()
                (let ((victim (get-buffer name))
                      (buffer-list-update-hook nil))
                  (when victim (kill-buffer victim)))
                (garbage-collect))))
       (buffer (get-buffer-create name)))
  (list (bufferp buffer) (buffer-live-p buffer) (eq buffer (get-buffer buffer))))

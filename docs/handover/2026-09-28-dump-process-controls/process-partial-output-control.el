;;; Same ordinary entry point and input in GNU and Emaxx.
(let* ((process-connection-type nil)
       (buffer (generate-new-buffer " *partial-output-control*"))
       (process
        (start-process
         "partial-output-control" buffer "/bin/sh" "-c"
         "IFS= read -r first; IFS= read -r second; IFS= read -r third; printf '%s\\n%s\\n' \"$first\" \"$second\"; IFS= read -r ack; printf '%s\\n' \"$third\"")))
  (unwind-protect
      (progn
        (set-process-query-on-exit-flag process nil)
        (set-process-sentinel process #'ignore)
        (process-send-string process "secret\n")
        (process-send-string process "second\n")
        (with-temp-buffer
          (insert "prefix\nregion\nsuffix")
          (process-send-region process 8 15))
        (accept-process-output process 60)
        (let ((prefix (with-current-buffer buffer (buffer-string)))
              (deadline (+ (float-time) 60)))
          ;; The child cannot produce its final line before this handshake.
          (process-send-string process "ack\n")
          (while (and (not (equal (with-current-buffer buffer (buffer-string))
                                 "secret\nsecond\nregion\n"))
                      (< (float-time) deadline))
            (accept-process-output process (- deadline (float-time))))
          (prin1 (list prefix (with-current-buffer buffer (buffer-string))))))
    (delete-process process)
    (kill-buffer buffer)))

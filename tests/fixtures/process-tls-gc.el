(progn
  (require 'gnutls)
  (defun tls-gc-create (asynchronous)
    (let* ((parameters (list 'gnutls-anon :hostname (concat "local" "host")
                             :priority (concat "NORMAL:" "+ANON-ECDH")))
           (process
            (make-network-process
             :name "gc-transport" :host "127.0.0.1" :service @PORT@
             :family 'ipv4 :coding 'binary :sentinel #'ignore :noquery t
             :nowait asynchronous
             :tls-parameters (and asynchronous parameters))))
      (unless asynchronous
        (gnutls-boot process (car parameters)
                     (append (cdr parameters) '(:complete-negotiation t))))
      process))
  (let (results)
    (dolist (asynchronous '(nil t))
      (let ((process (tls-gc-create asynchronous)) (tries 0))
        (unwind-protect
            (progn
              (garbage-collect)
              (while (and (eq (process-status process) 'connect)
                          (< (setq tries (1+ tries)) 100))
                (sit-for 0.01))
              (garbage-collect)
              (let ((status (gnutls-peer-status process)))
                (push (list (process-status process)
                            (gnutls-get-initstage process)
                            (mapcar (lambda (key) (stringp (plist-get status key)))
                                    '(:key-exchange :protocol :cipher :mac)))
                      results))
              (gnutls-deinit process))
          (delete-process process))))
    (nreverse results)))
